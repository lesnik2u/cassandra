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

package org.apache.cassandra.utils.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.BufferPoolBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.Finding;

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Keeps the invariants honest: a node busy writing, overwriting, deleting, flushing, reading through the chunk cache,
 * over the native protocol too, compacting and indexing, checked every few milliseconds by a thread of its own, as
 * the watchdog's, with no report interval, reports nothing. An invariant must fail for {@link #PERSIST_MILLIS} of
 * wall time to be reported, as with the default persistence, 2 checks at 5 s, it must fail for 5 s; a persistence of
 * 2 checks a few milliseconds apart could report a writer descheduled between two counter updates.
 */
public class ResourceAccountingNoFalsePositiveTest extends CQLTester
{
    private static final int WRITERS = 4;
    private static final int ROUNDS = 8;
    private static final long CHECK_INTERVAL_MILLIS = 5;
    // the checker sleeps the check interval between checks, so the 1st and the last are at least this far apart
    private static final long PERSIST_MILLIS = 250;
    private static final int PERSIST_CHECKS = (int) (PERSIST_MILLIS / CHECK_INTERVAL_MILLIS) + 1;
    private static final int MIN_CHECKS = 4 * PERSIST_CHECKS;

    // chunks under a page, of a page, of several pages, and no compression
    private static final String[] COMPRESSION = { "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 1}",
                                                  "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 4}",
                                                  "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}",
                                                  "{'enabled': false}" };
    // The last table, the indexed one, is on a skiplist memtable, to keep clear of a pre-existing bug (a ticket is
    // pending): with an SAI index on a trie memtable, this churn fails the assert "Row deletion without row level
    // marker" in TriePartitionUpdaterLegacyIndex.processMarkerBoundary (TriePartitionUpdaterLegacyIndex.java:125-126),
    // from f70a65fa6bf; in a single-threaded repro with no flush, on an update that replaces the whole set.
    private static final String[] MEMTABLES = { "trie", "skiplist", "trie", "skiplist" };

    @Test
    public void testBusyNodeReportsNothing() throws Throwable
    {
        // here rather than in a @BeforeClass, which would run before the setUpClass of a subclass that hides
        // CQLTester's to configure the node first
        requireNetwork();
        assertNotNull("the chunk cache is enabled in tests", ChunkCache.instance);
        MemtablePool memtablePool = AbstractAllocatorMemtable.MEMORY_POOL;

        List<String> tables = new ArrayList<>();
        for (int i = 0; i < COMPRESSION.length; i++)
        {
            String table = createTable("CREATE TABLE %s (pk int, ck int, v text, n int, s set<int>, PRIMARY KEY (pk, ck)) " +
                                       "WITH memtable = '" + MEMTABLES[i] + "' AND compression = " + COMPRESSION[i]);
            tables.add(table);
        }
        // an index accounts its memory through Memtable.markExtraOnHeapUsed, negative adjustments included
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        // the driver session, which CQLTester creates on first use, but not thread-safely, for the writers to share
        executeNet("SELECT * FROM " + KEYSPACE + '.' + tables.get(0) + " LIMIT 1");

        ResourceAccountingCheck check = ResourceAccountingCheck.forNode(PERSIST_CHECKS, PERSIST_CHECKS, 0);
        List<Finding> findings = new ArrayList<>();
        AtomicBoolean done = new AtomicBoolean();
        long[] seen = new long[3];   // the most memtable on-heap and off-heap memory, and chunk cache pool memory, in use
        long networkingRequests = networkingRequests();
        int[] checks = { 0 };
        Thread checker = new Thread(() -> {
            while (!done.get())
            {
                findings.addAll(check.check(preciseTime.now()));
                checks[0]++;
                seen[0] = Math.max(seen[0], memtablePool.onHeap.used());
                seen[1] = Math.max(seen[1], memtablePool.offHeap.used());
                seen[2] = Math.max(seen[2], BufferPoolBalance.of(BufferPools.forChunkCache()).memoryInUse);
                try
                {
                    Thread.sleep(CHECK_INTERVAL_MILLIS);
                }
                catch (InterruptedException e)
                {
                    return;
                }
            }
        }, "ResourceAccountingChecker");
        checker.start();

        ExecutorService writers = Executors.newFixedThreadPool(WRITERS);
        try
        {
            for (int round = 0; round < ROUNDS; round++)
            {
                List<Future<?>> writes = new ArrayList<>();
                for (int w = 0; w < WRITERS; w++)
                {
                    int seed = round * WRITERS + w;
                    writes.add(writers.submit(() -> {
                        try
                        {
                            write(tables, seed);
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError(t);
                        }
                    }));
                }
                for (Future<?> write : writes)
                    write.get(5, TimeUnit.MINUTES);

                for (String table : tables)
                {
                    flush(KEYSPACE, table);
                    read(table);
                }
                if (round % 3 == 2)
                {
                    for (String table : tables)
                        compact(KEYSPACE, table);
                }
            }
        }
        finally
        {
            writers.shutdownNow();
            try
            {
                // long enough for a failure that starts as the workload ends to be reported
                Thread.sleep(2 * PERSIST_MILLIS);
            }
            finally
            {
                done.set(true);
                checker.join(TimeUnit.MINUTES.toMillis(1));
            }
        }

        logger.info("{} accounting checks; at most {} bytes of memtable on-heap memory, {} bytes of memtable off-heap " +
                    "memory and {} bytes of chunk cache buffer pool memory in use", checks[0], seen[0], seen[1], seen[2]);
        assertEquals(List.of(), findings);
        assertTrue("only " + checks[0] + " checks", checks[0] >= MIN_CHECKS);
        assertTrue("the memtable pool was never in use", seen[0] > 0);
        // the off-heap sub-pool has a limit of 0 when the allocation type does not use it, as with heap_buffers
        if (memtablePool.offHeap.limit > 0)
            assertTrue("the memtable pool's off-heap memory was never in use", seen[1] > 0);
        assertTrue("the chunk cache's buffer pool was never in use", seen[2] > 0);
        assertTrue("the networking buffer pool was never asked for a buffer", networkingRequests() > networkingRequests);
    }

    // what the networking pool has been asked for: served from its chunks, or, when they are full, unpooled
    private static long networkingRequests()
    {
        BufferPool pool = BufferPools.forNetworking();
        return pool.metrics.hits() + pool.metrics.misses();
    }

    // overwrites, deletions of cells, rows and partitions, and collections, at random timestamps, so that some
    // merges shrink the memtable
    private void write(List<String> tables, int seed) throws Throwable
    {
        Random random = new Random(seed);
        for (int i = 0; i < 300; i++)
        {
            String table = KEYSPACE + '.' + tables.get(random.nextInt(tables.size()));
            int pk = random.nextInt(20);
            int ck = random.nextInt(50);
            long timestamp = 1000 + random.nextInt(100_000);
            switch (random.nextInt(8))
            {
                case 0:
                    executeFormattedQuery("DELETE FROM " + table + " USING TIMESTAMP " + timestamp + " WHERE pk = ?", pk);
                    break;
                case 1:
                    executeFormattedQuery("DELETE FROM " + table + " USING TIMESTAMP " + timestamp + " WHERE pk = ? AND ck = ?", pk, ck);
                    break;
                case 2:
                    executeFormattedQuery("UPDATE " + table + " USING TIMESTAMP " + timestamp + " SET s = s + {" +
                                          random.nextInt(10) + ", " + random.nextInt(10) + "} WHERE pk = ? AND ck = ?", pk, ck);
                    break;
                case 3:
                    executeNet("UPDATE " + table + " USING TIMESTAMP " + timestamp + " SET s = {" + random.nextInt(10) +
                               "}, v = ? WHERE pk = ? AND ck = ?", value(random), pk, ck);
                    break;
                default:
                    executeFormattedQuery("INSERT INTO " + table + " (pk, ck, v, n) VALUES (?, ?, ?, ?) USING TIMESTAMP " +
                                          timestamp, pk, ck, value(random), random.nextInt(100));
            }
        }
    }

    // values from a few bytes to a few pages, so that rows span chunks
    private static String value(Random random)
    {
        StringBuilder value = new StringBuilder();
        int length = random.nextInt(4) == 0 ? random.nextInt(20_000) : random.nextInt(100);
        while (value.length() < length)
            value.append((char) ('a' + random.nextInt(26)));
        return value.toString();
    }

    private void read(String table) throws Throwable
    {
        String name = KEYSPACE + '.' + table;
        for (int pk = 0; pk < 20; pk++)
            executeFormattedQuery("SELECT * FROM " + name + " WHERE pk = ?", pk);
        executeNet("SELECT * FROM " + name + " LIMIT 500");
        executeFormattedQuery("SELECT * FROM " + name + " WHERE n = 7 ALLOW FILTERING");
    }
}
