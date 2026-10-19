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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.commitlog.MemoryMappedSegment;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.mvcc.CommitLogSegmentMappings;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.Util.randomBytes;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.TIMEOUT_SECONDS;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.awaitNoMappings;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.awaitNoValueFiles;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.currentMemtable;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.forceMmapCommitLog;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.isAccepted;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.useSmallMemtablePool;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.valueFileCount;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeAndVerifyMixedRows;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeCounters;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeSmallValues;
import static org.junit.Assert.*;

/// End-to-end tests of `value_storage: commitlog` (the `trie_commitlog` memtable of the test yaml): values written
/// through CQL are read from the commit log segments their mutations were written to, values that cannot be found
/// there are copied into the fallback store, both are charged to the memtable's memory, and every segment mapping,
/// fallback file and charge is released when the memtable is discarded.
///
/// The test JVM sets `cassandra.memtable_value_log_verify`, so every segment handle is compared with the value's bytes
/// when it is stored.
public class TrieMemtableCommitLogStoreTest extends CQLTester
{
    // Overrides CQLTester.setUpClass to run before it
    @BeforeClass
    public static void setUpClass()
    {
        useSmallMemtablePool();
        setUpCommitLogValueStorage();
    }

    /// Prepares the server for `commitlog` value storage. Changes to the commit log configuration must be made before.
    static void setUpCommitLogValueStorage()
    {
        // commitlog value storage needs the written bytes in the segment file at once: an uncompressed, unencrypted,
        // memory-mapped commit log. Configurations that resolve to something else (commit log compression,
        // commitlog_disk_access_mode: auto resolving to direct) are overridden before the commit log starts.
        ServerTestUtils.daemonInitialization();
        forceMmapCommitLog();

        // same mechanism MemtableSizeOffheapObjectsTest uses to force the allocation type before the pool is created
        MemtableSizeTestBase.setup(Config.MemtableAllocationType.offheap_objects, Murmur3Partitioner.instance);
        // only now: the check initializes TrieMemtable, which creates the memtable pool
        assertTrue(isAccepted(TrieMemtable.ValueStorage.COMMITLOG));

        for (CommitLogSegment segment : CommitLog.instance.getSegmentManager().getActiveSegments())
            assertTrue(segment + " is not memory-mapped", segment instanceof MemoryMappedSegment);
    }

    /// Every test starts with no segment mapped, so that the mapping counts it checks are its own: the memtables of
    /// the previous test's tables are discarded asynchronously after the tables are dropped.
    @Before
    public void awaitMappingsOfPreviousTestsReleased()
    {
        awaitNoMappings("mappings of previous tests were not released");
    }

    /// The last test's memtables are discarded after it has ended, and must release their mappings too.
    @AfterClass
    public static void awaitMappingsOfLastTestReleased()
    {
        awaitNoMappings("mappings of the last test were not released");
    }

    @Test
    public void largeValuesAreReadFromTheCommitLog() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_commitlog'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);
        long written = 0;
        for (int i = 0; i < 200; i++)
        {
            int length = 1024 + 36 * i;   // 1 KiB to 8 KiB
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomBytes(length, i));
            written += length;
        }
        assertEquals(written, memtable.valueLogBytes());
        assertEquals("every value of a CQL insert is in the commit log", 0, memtable.valueLogFallbackBytes());
        assertTrue(CommitLogSegmentMappings.liveCount() > 0);

        for (int i = 0; i < 200; i++)
            assertEquals(randomBytes(1024 + 36 * i, i), execute("SELECT v FROM %s WHERE pk = ?", i).one().getBytes("v"));
        flush();
        assertNotSame(memtable, cfs.getCurrentMemtable());
        for (int i = 0; i < 200; i++)
            assertEquals(randomBytes(1024 + 36 * i, i), execute("SELECT v FROM %s WHERE pk = ?", i).one().getBytes("v"));

        // discard happens on the reclaim executor after the read barrier
        awaitNoMappings("the discarded memtable did not release its segment mappings");
        awaitNoValueFiles(cfs.metadata());
    }

    @Test
    public void mixedRowsBeforeAndAfterFlush() throws Throwable
    {
        writeAndVerifyMixedRows(this, TrieMemtable.ValueStorage.COMMITLOG);
    }

    @Test
    public void smallValuesAreEmbeddedAndCountersReferenced() throws Throwable
    {
        writeSmallValues(this, TrieMemtable.ValueStorage.COMMITLOG);
        assertEquals(0, CommitLogSegmentMappings.liveCount());

        // The counter mutation is applied with the updated context, which is written to the commit log; a local
        // increment's context supersedes the one in the memtable, so the merge keeps the written buffer
        // (Cells.reconcile) and every update is referenced in the segment rather than copied.
        writeCounters(this, TrieMemtable.ValueStorage.COMMITLOG);
        assertTrue(CommitLogSegmentMappings.liveCount() > 0);
    }

    /// A replica applies a mutation deserialized from the wire: its cells hold arrays of the received bytes, which the
    /// commit log entry either repeats (the mutation's cached serialization, recorded while deserializing, when the
    /// sender's messaging version is the storage version) or serializes again (recorded while serializing, when it
    /// is not). The values are referenced either way.
    @Test
    public void replicaWritesAreReadFromTheCommitLog() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob, w blob) WITH memtable = 'trie_commitlog'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);
        int storageVersion = StorageCompatibilityMode.current().storageMessagingVersion();
        int[] senderVersions = { MessagingService.current_version, MessagingService.VERSION_40 };
        // one sender version is the storage version, and the other is not
        assertNotEquals(senderVersions[0], senderVersions[1]);
        assertTrue(storageVersion == senderVersions[0] || storageVersion == senderVersions[1]);

        long written = 0;
        int pk = 0;
        for (int senderVersion : senderVersions)
        {
            for (int i = 0; i < 20; i++, pk++)
            {
                PartitionUpdate.SimpleBuilder update = PartitionUpdate.simpleBuilder(cfs.metadata(), pk);
                update.row().add("v", randomBytes(1024 + 100 * i, pk)).add("w", randomBytes(2048, -pk));
                byte[] wire;
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    Mutation.serializer.serialize(new Mutation(update.build()), out, senderVersion);
                    wire = out.toByteArray();
                }
                Mutation received = Mutation.serializer.deserialize(new DataInputBuffer(wire), senderVersion, DeserializationHelper.Flag.FROM_REMOTE);
                assertNotNull("locations recorded while deserializing", Mutation.serializer.offsetsFor(received, senderVersion));
                received.apply();
                assertNotNull("locations of the serialization written to the commit log", Mutation.serializer.offsetsFor(received, storageVersion));
                written += 1024 + 100 * i + 2048;
            }
        }
        assertEquals(written, memtable.valueLogBytes());
        assertEquals("every value of a replica write is in the commit log", 0, memtable.valueLogFallbackBytes());
        assertTrue(CommitLogSegmentMappings.liveCount() > 0);

        for (int i = 0; i < pk; i++)
        {
            UntypedResultSet.Row row = execute("SELECT v, w FROM %s WHERE pk = ?", i).one();
            assertEquals("row " + i, randomBytes(1024 + 100 * (i % 20), i), row.getBytes("v"));
            assertEquals("row " + i, randomBytes(2048, -i), row.getBytes("w"));
        }
    }

    @Test
    public void nonDurableKeyspaceFallsBack() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_commitlog'");
        TrieMemtable memtable = currentMemtable(getColumnFamilyStore(keyspace, table), TrieMemtable.ValueStorage.COMMITLOG);
        for (int i = 0; i < 50; i++)
            execute("INSERT INTO " + keyspace + '.' + table + " (pk, v) VALUES (?, ?)", i, randomBytes(1024, i));
        assertEquals(50L * 1024, memtable.valueLogBytes());
        assertEquals("writes that skip the commit log are copied", 50L * 1024, memtable.valueLogFallbackBytes());
        assertEquals("no segment is mapped for a write that skips the commit log", 0, CommitLogSegmentMappings.liveCount());
        for (int i = 0; i < 50; i++)
            assertEquals(randomBytes(1024, i), execute("SELECT v FROM " + keyspace + '.' + table + " WHERE pk = ?", i).one().getBytes("v"));
    }

    @Test
    public void segmentDeletedUnderAnOpenReadIsStillReadable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_commitlog'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);
        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomBytes(4096, i));
        assertEquals(20L * 4096, memtable.valueLogBytes());
        assertEquals(0, memtable.valueLogFallbackBytes());
        int mappings = CommitLogSegmentMappings.liveCount();
        assertTrue(mappings > 0);
        // the values are in segments that are active now: the memtable has not been flushed
        List<File> segmentFiles = CommitLog.instance.getSegmentManager().getActiveSegments().stream()
                                                    .map(s -> new File(s.getPath()))
                                                    .collect(Collectors.toList());
        assertFalse(segmentFiles.isEmpty());

        // the read group keeps the memtable from being discarded until the iterator has been consumed
        try (OpOrder.Group readGroup = cfs.readOrdering.start();
             UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(cfs.metadata()),
                                                                                 DataRange.allData(cfs.getPartitioner()),
                                                                                 SSTableReadsListener.NOOP_LISTENER))
        {
            // flushes every dirty table, then deletes the segments that are no longer needed: all of these
            CommitLog.instance.forceRecycleAllSegments();
            assertNotSame(memtable, cfs.getCurrentMemtable());
            Util.spinAssertEquals("commit log segments were not deleted", 0L,
                                  () -> segmentFiles.stream().filter(File::exists).count(), TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals("the memtable still maps the deleted segments", mappings, CommitLogSegmentMappings.liveCount());

            ColumnMetadata v = cfs.metadata().getColumn(ByteBufferUtil.bytes("v"));
            int count = 0;
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                    Unfiltered unfiltered = partition.next();
                    assertFalse(partition.hasNext());
                    assertEquals("row " + pk, randomBytes(4096, pk), ((Row) unfiltered).getCell(v).buffer());
                    ++count;
                }
            }
            assertEquals(20, count);
        }

        awaitNoMappings("the discarded memtable did not release its segment mappings");
        awaitNoValueFiles(cfs.metadata());
        for (int i = 0; i < 20; i++)
            assertEquals(randomBytes(4096, i), execute("SELECT v FROM %s WHERE pk = ?", i).one().getBytes("v"));
    }

    @Test
    public void replayLandsInFallback() throws Throwable
    {
        // as RecoveryManagerTest: start from an empty commit log, so that the replay below applies only this test's
        // mutations
        CommitLog.instance.resetUnsafe(true);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_commitlog'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TrieMemtable written = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);
        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomBytes(4096, i));
        assertEquals(50L * 4096, written.valueLogBytes());
        assertEquals(0, written.valueLogFallbackBytes());
        CommitLog.instance.sync(true);

        // emulate the restart: drop the data in memory and replay the commit log
        cfs.clearUnsafe();
        // clearUnsafe replaces the memtable without discarding it; release its mappings and fallback files here
        OpOrder.Barrier writeBarrier = Keyspace.writeOrder.newBarrier();
        writeBarrier.issue();
        writeBarrier.await();
        written.switchOut(writeBarrier, new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
        written.discard();
        assertEquals(0, CommitLogSegmentMappings.liveCount());

        TrieMemtable replayed = currentMemtable(cfs, TrieMemtable.ValueStorage.COMMITLOG);
        assertEquals(0, replayed.valueLogBytes());
        // the replay flushes the replayed tables; the read group keeps the replayed memtable from being discarded
        try (OpOrder.Group readGroup = cfs.readOrdering.start())
        {
            CommitLog.instance.resetUnsafe(false);
            assertEquals(50L * 4096, replayed.valueLogBytes());
            assertEquals("replayed values are copied", 50L * 4096, replayed.valueLogFallbackBytes());
            assertEquals("replay maps no segment", 0, CommitLogSegmentMappings.liveCount());

            ColumnMetadata v = cfs.metadata().getColumn(ByteBufferUtil.bytes("v"));
            int count = 0;
            try (UnfilteredPartitionIterator partitions = replayed.partitionIterator(ColumnFilter.all(cfs.metadata()),
                                                                                     DataRange.allData(cfs.getPartitioner()),
                                                                                     SSTableReadsListener.NOOP_LISTENER))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                        assertEquals("row " + pk, randomBytes(4096, pk), ((Row) partition.next()).getCell(v).buffer());
                        assertFalse(partition.hasNext());
                        ++count;
                    }
                }
            }
            assertEquals(50, count);
            assertTrue("the replayed values are in value files", valueFileCount(cfs.metadata()) > 0);
        }
        for (int i = 0; i < 50; i++)
            assertEquals(randomBytes(4096, i), execute("SELECT v FROM %s WHERE pk = ?", i).one().getBytes("v"));
        awaitNoValueFiles(cfs.metadata());
    }

    @Test
    public void valuesTriggerTheMemtableLimitFlush() throws Throwable
    {
        ValueStorageTestSupport.valuesTriggerTheMemtableLimitFlush(this, TrieMemtable.ValueStorage.COMMITLOG);
    }

    @Test
    public void valuesAreChargedUntilTheMemtableIsDiscarded() throws Throwable
    {
        ValueStorageTestSupport.valuesAreChargedUntilTheMemtableIsDiscarded(this, TrieMemtable.ValueStorage.COMMITLOG);
    }
}
