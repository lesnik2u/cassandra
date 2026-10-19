/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.tries.mvcc.CommitLogSegmentMappings;

import static org.apache.cassandra.Util.randomBytes;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.awaitNoMappings;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.awaitNoValueFiles;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.currentMemtable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// `value_storage: commitlog` under concurrent writers and readers, with commit log segments small enough that the
/// writes reference values in several of them: every value is referenced, readers always see the bytes written, and
/// the mappings are released once the memtable is flushed and discarded.
///
/// The test JVM sets `cassandra.memtable_value_log_verify`, so every segment handle is also compared with the value's
/// bytes when it is stored.
public class TrieMemtableCommitLogStoreThreadedTest extends CQLTester
{
    private static final int SEGMENT_SIZE_MIB = 1;
    private static final int WRITERS = 4;
    private static final int READERS = 4;
    /// Writers keep going for this long, and until they have written at least [#MIN_BYTES] (several segments).
    private static final long WRITE_MILLIS = 3000;
    private static final long MIN_BYTES = 4L << 20;
    /// Well below the memtable memory limit, which also counts the values, so that the memtable is not flushed.
    private static final long MAX_BYTES = 24L << 20;
    private static final int MAX_KEYS = (int) (MAX_BYTES / 1024);
    private static final int TIMEOUT_SECONDS = 120;

    // Overrides CQLTester.setUpClass to run before it
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        // before the commit log starts, so that every segment is this small
        DatabaseDescriptor.setCommitLogSegmentSize(SEGMENT_SIZE_MIB);
        TrieMemtableCommitLogStoreTest.setUpCommitLogValueStorage();
    }

    @Before
    public void awaitMappingsOfPreviousTestsReleased()
    {
        awaitNoMappings("mappings of previous tests were not released");
    }

    @AfterClass
    public static void awaitMappingsOfLastTestReleased()
    {
        awaitNoMappings("mappings of the last test were not released");
    }

    private static int length(int pk)
    {
        return 1024 + 512 * (pk % 8);   // 1 KiB to 4.5 KiB
    }

    @Test
    public void concurrentWritersAndReadersAcrossSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_commitlog'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);

        AtomicInteger nextKey = new AtomicInteger();
        AtomicIntegerArray written = new AtomicIntegerArray(MAX_KEYS);   // 1 once the key's insert has returned
        AtomicLong bytes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        AtomicBoolean writing = new AtomicBoolean(true);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        long start = System.nanoTime();
        long writeEnd = start + TimeUnit.MILLISECONDS.toNanos(WRITE_MILLIS);
        long deadline = start + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);

        List<Thread> writers = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++)
        {
            writers.add(new Thread(() -> {
                try
                {
                    while (errors.isEmpty() && System.nanoTime() < deadline)
                    {
                        long total = bytes.get();
                        if (total >= MAX_BYTES || (total >= MIN_BYTES && System.nanoTime() >= writeEnd))
                            return;
                        int pk = nextKey.getAndIncrement();
                        if (pk >= MAX_KEYS)
                            return;
                        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, randomBytes(length(pk), pk));
                        written.set(pk, 1);
                        bytes.addAndGet(length(pk));
                    }
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
            }, "writer-" + w));
        }
        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < READERS; r++)
        {
            readers.add(new Thread(() -> {
                try
                {
                    while (writing.get() && errors.isEmpty())
                    {
                        int bound = Math.min(nextKey.get(), MAX_KEYS);
                        if (bound == 0)
                        {
                            Thread.yield();
                            continue;
                        }
                        int pk = ThreadLocalRandom.current().nextInt(bound);
                        if (written.get(pk) == 0)
                        {
                            // in flight: the row may not be there yet, but if it is, it has the bytes being written
                            UntypedResultSet rows = execute("SELECT v FROM %s WHERE pk = ?", pk);
                            if (!rows.isEmpty())
                                assertEquals("in-flight row " + pk, randomBytes(length(pk), pk), rows.one().getBytes("v"));
                            continue;
                        }
                        ByteBuffer read = execute("SELECT v FROM %s WHERE pk = ?", pk).one().getBytes("v");
                        assertEquals("row " + pk, randomBytes(length(pk), pk), read);
                        reads.incrementAndGet();
                    }
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
            }, "reader-" + r));
        }

        readers.forEach(Thread::start);
        writers.forEach(Thread::start);
        for (Thread writer : writers)
            writer.join(TimeUnit.NANOSECONDS.toMillis(Math.max(1, deadline - System.nanoTime())));
        writing.set(false);
        for (Thread reader : readers)
            reader.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        for (Thread thread : writers)
            assertFalse(thread.getName() + " did not finish in time", thread.isAlive());
        for (Thread thread : readers)
            assertFalse(thread.getName() + " did not finish in time", thread.isAlive());
        if (!errors.isEmpty())
        {
            AssertionError failure = new AssertionError(errors.size() + " writer or reader failures");
            errors.forEach(failure::addSuppressed);
            throw failure;
        }

        int keys = Math.min(nextKey.get(), MAX_KEYS);
        for (int pk = 0; pk < keys; pk++)
        {
            if (written.get(pk) == 0)
                fail("the insert of row " + pk + " did not complete");
        }
        assertTrue("written " + bytes.get() + " bytes, fewer than " + MIN_BYTES, bytes.get() >= MIN_BYTES);
        assertTrue("no read was checked", reads.get() > 0);
        assertSame("the memtable was flushed while writing", memtable, cfs.getCurrentMemtable());
        assertEquals(bytes.get(), memtable.valueLogBytes());
        assertEquals("every value of a CQL write is in the commit log", 0, memtable.valueLogFallbackBytes());
        int mappings = CommitLogSegmentMappings.liveCount();
        logger.info("Wrote {} rows, {} bytes, in {} segments; checked {} reads", keys, bytes.get(), mappings, reads.get());
        assertTrue("the values are in " + mappings + " segments, expected several", mappings >= 2);

        for (int pk = 0; pk < keys; pk++)
            assertEquals("row " + pk, randomBytes(length(pk), pk), execute("SELECT v FROM %s WHERE pk = ?", pk).one().getBytes("v"));

        flush();
        assertNotSame(memtable, cfs.getCurrentMemtable());
        awaitNoMappings("the discarded memtable did not release its segment mappings");
        awaitNoValueFiles(cfs.metadata());
        for (int pk = 0; pk < keys; pk++)
        {
            UntypedResultSet.Row row = execute("SELECT v FROM %s WHERE pk = ?", pk).one();
            assertEquals("row " + pk, randomBytes(length(pk), pk), row.getBytes("v"));
        }
    }
}
