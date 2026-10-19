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

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.mvcc.CommitLogSegmentMappings;
import org.apache.cassandra.db.tries.mvcc.FileValueStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_VALUE_LOG_VERIFY;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Exercises `value_storage: file` and `commitlog` of [TrieMemtable] on paths that only a cluster reaches: values of
/// mutations deserialized from another node, hint delivery, and commit log replay after a restart. Checks the rows on
/// every node and the exact bytes each memtable put in its value stores, of which, with `commitlog` storage, only the
/// value of an uncached mutation is copied into the fallback store; and that flushed memtables release their files and
/// segment mappings. Runs with [CommitLogValueStore][org.apache.cassandra.db.tries.mvcc.CommitLogValueStore] verifying
/// every referenced value against the commit log segment.
public class MemtableValueStorageTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS_PER_PARTITION = 4;
    /// Values of up to 24 bytes, without a TTL, are embedded in the trie cell and never reach a value store.
    private static final int MAX_EMBEDDED_LENGTH = 24;
    private static final int EMBEDDED_LENGTH = 8;
    private static final int STORED_LENGTH = 1024;
    /// The value of (pk 0, ck 1): its mutation is above `CACHEABLE_MUTATION_SIZE_LIMIT` (1,000,000 bytes), so no
    /// offsets are recorded for it and `commitlog` storage copies it into the fallback store.
    private static final int UNCACHED_LENGTH = 1536 * 1024;

    private static final int INITIAL_PARTITIONS = 100;
    private static final int HINTED_PARTITIONS = 20;
    private static final int RESTARTED_PARTITIONS = 10;

    private static WithProperties properties;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        properties = new WithProperties().set(MEMTABLE_VALUE_LOG_VERIFY, true);
    }

    @AfterClass
    public static void afterClass()
    {
        properties.close();
    }

    @Test
    public void fileValueStorage() throws Throwable
    {
        testValueStorage("trie_file", false);
    }

    @Test
    public void commitLogValueStorage() throws Throwable
    {
        testValueStorage("trie_commitlog", true);
    }

    private static void testValueStorage(String memtable, boolean commitLogStorage) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             // the -latest configuration sets auto, which may resolve to direct
                                                             .set("commitlog_disk_access_mode", "mmap")
                                                             .set("hints_flush_period", "1s")
                                                             // HCD_1 and CASSANDRA_4 write the memtable to the schema as a map of
                                                             // its class and parameters, which MemtableParams reads back as the
                                                             // configuration `trie` (CNDB-17010), losing the name used here
                                                             .set("storage_compatibility_mode", "NONE")
                                                             .set("memtable", ImmutableMap.of(
                                                                 "configurations", ImmutableMap.of(
                                                                     "trie_commitlog", trieConfiguration("commitlog"),
                                                                     "trie_file", trieConfiguration("file")))))
                                           .start()))
        {
            for (int node = 1; node <= 2; ++node)
                assertTrue("value log verification off on node " + node,
                           cluster.get(node).callOnInstance(() -> MEMTABLE_VALUE_LOG_VERIFY.getBoolean()));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) " +
                                              "WITH memtable = '" + memtable + "'"));
            // with commitlog storage, only the uncached value is copied into the fallback store
            long uncachedBytes = commitLogStorage ? valueLength(0, 1) : 0;
            int hintedEnd = INITIAL_PARTITIONS + HINTED_PARTITIONS;
            int restartedEnd = hintedEnd + RESTARTED_PARTITIONS;

            // 1. Node 1 applies its writes locally, node 2 deserializes them from messages.
            write(cluster, ALL, 0, INITIAL_PARTITIONS);
            assertPartitions(cluster, 0, INITIAL_PARTITIONS);

            // 2. All values longer than a trie cell went to the value stores.
            for (int node = 1; node <= 2; ++node)
                assertValueLog(cluster.get(node), storedBytes(0, INITIAL_PARTITIONS), uncachedBytes);

            // 3. Node 2 misses the next writes and receives them as hints, delivered only once the filter is removed.
            IMessageFilters.Filter drop = cluster.filters().verbs(Verb.MUTATION_REQ.id, Verb.HINT_REQ.id).from(1).to(2).drop();
            write(cluster, ONE, INITIAL_PARTITIONS, hintedEnd);
            long hints = (long) HINTED_PARTITIONS * ROWS_PER_PARTITION;
            await().atMost(60, SECONDS).untilAsserted(() -> assertEquals("hints written by node 1", hints,
                                                                         (long) cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount())));
            for (int pk = INITIAL_PARTITIONS; pk < hintedEnd; ++pk)
                assertEquals("partition " + pk + " on node 2 before hint delivery", 0,
                             cluster.get(2).executeInternal(withKeyspace("SELECT ck FROM %s." + TABLE + " WHERE pk = ?"), pk).length);
            drop.off();
            await().atMost(60, SECONDS).untilAsserted(() -> assertPartitions(cluster.get(2), INITIAL_PARTITIONS, hintedEnd));
            for (int node = 1; node <= 2; ++node)
                assertValueLog(cluster.get(node), storedBytes(0, hintedEnd), uncachedBytes);

            // 4. Node 2 rebuilds its data from the commit log. Commit log replay flushes the tables it replayed before
            // startup returns, so the rows must be in sstables and the current memtable must be empty.
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            ClusterUtils.awaitRingStatus(cluster.get(1), cluster.get(2), "Up");
            assertPartitions(cluster.get(2), 0, hintedEnd);
            assertValueLog(cluster.get(2), 0, 0);
            assertTrue("node 2 has no sstables after replay", liveSSTableCount(cluster.get(2)) > 0);

            // 5. Writes after the restart, recorded on the restarted node as before, then a flush of both nodes, after
            // which the flushed memtables release their value files and segment mappings.
            write(cluster, ALL, hintedEnd, restartedEnd);
            assertPartitions(cluster, 0, restartedEnd);
            assertValueLog(cluster.get(1), storedBytes(0, restartedEnd), uncachedBytes);
            assertValueLog(cluster.get(2), storedBytes(hintedEnd, restartedEnd), 0);
            cluster.forEach(i -> i.flush(KEYSPACE));
            assertPartitions(cluster, 0, restartedEnd);
            for (int node = 1; node <= 2; ++node)
            {
                IInvokableInstance instance = cluster.get(node);
                assertValueLog(instance, 0, 0);
                await().atMost(60, SECONDS).untilAsserted(() -> assertReleased(instance));
            }

            // 6. No value recorded at the wrong offset.
            if (commitLogStorage)
            {
                for (int node = 1; node <= 2; ++node)
                {
                    List<String> errors = cluster.get(node).logs().grep("ERROR.*SerializedValueOffsets.*").getResult();
                    assertTrue("value mismatches on node " + node + ": " + errors, errors.isEmpty());
                }
            }
        }
    }

    private static ImmutableMap<String, Object> trieConfiguration(String valueStorage)
    {
        return ImmutableMap.of("class_name", "TrieMemtable",
                               "parameters", ImmutableMap.of("shards", "4",
                                                             "value_storage", valueStorage));
    }

    /// The length of the value of row (`pk`, `ck`): embedded in the trie cell for ck 0, uncached for (0, 1).
    private static int valueLength(int pk, int ck)
    {
        if (ck == 0)
            return EMBEDDED_LENGTH;
        return pk == 0 && ck == 1 ? UNCACHED_LENGTH : STORED_LENGTH;
    }

    /// The value of row (`pk`, `ck`): [#valueLength] seeded random bytes.
    private static ByteBuffer value(int pk, int ck)
    {
        byte[] bytes = new byte[valueLength(pk, ck)];
        new Random(((long) pk << 32) | ck).nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    /// The bytes of the values of partitions `fromPk` (inclusive) to `toPk` (exclusive) that are too long to be
    /// embedded in the trie cell.
    private static long storedBytes(int fromPk, int toPk)
    {
        long bytes = 0;
        for (int pk = fromPk; pk < toPk; ++pk)
        {
            for (int ck = 0; ck < ROWS_PER_PARTITION; ++ck)
            {
                int length = valueLength(pk, ck);
                if (length > MAX_EMBEDDED_LENGTH)
                    bytes += length;
            }
        }
        return bytes;
    }

    /// Writes partitions `fromPk` (inclusive) to `toPk` (exclusive) through node 1, one mutation per row.
    private static void write(Cluster cluster, ConsistencyLevel consistency, int fromPk, int toPk)
    {
        for (int pk = fromPk; pk < toPk; ++pk)
            for (int ck = 0; ck < ROWS_PER_PARTITION; ++ck)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (pk, ck, v) VALUES (?, ?, ?)"),
                                               consistency, pk, ck, value(pk, ck));
    }

    /// Checks every row of partitions `fromPk` (inclusive) to `toPk` (exclusive) with a local read on each node.
    private static void assertPartitions(Cluster cluster, int fromPk, int toPk)
    {
        for (int node = 1; node <= cluster.size(); ++node)
            assertPartitions(cluster.get(node), fromPk, toPk);
    }

    /// Checks every row of partitions `fromPk` (inclusive) to `toPk` (exclusive) with a local read on `node`.
    private static void assertPartitions(IInvokableInstance node, int fromPk, int toPk)
    {
        for (int pk = fromPk; pk < toPk; ++pk)
        {
            Object[][] rows = node.executeInternal(withKeyspace("SELECT ck, v FROM %s." + TABLE + " WHERE pk = ?"), pk);
            assertEquals("rows of partition " + pk + " on node " + node.config().num(), ROWS_PER_PARTITION, rows.length);
            for (int ck = 0; ck < ROWS_PER_PARTITION; ++ck)
            {
                assertEquals("clustering of row " + ck + " of partition " + pk + " on node " + node.config().num(), ck, rows[ck][0]);
                assertEquals("value of (" + pk + ", " + ck + ") on node " + node.config().num(), value(pk, ck), rows[ck][1]);
            }
        }
    }

    /// Checks the current memtable's `valueLogBytes()` and `valueLogFallbackBytes()` on `node`.
    private static void assertValueLog(IInvokableInstance node, long expectedBytes, long expectedFallbackBytes)
    {
        int num = node.config().num();
        assertEquals("value log bytes of node " + num, expectedBytes, (long) node.callOnInstance(() -> trieMemtable().valueLogBytes()));
        assertEquals("value log fallback bytes of node " + num, expectedFallbackBytes,
                     (long) node.callOnInstance(() -> trieMemtable().valueLogFallbackBytes()));
    }

    /// Checks that no value file of the table and no commit log segment mapping is left on `node`.
    private static void assertReleased(IInvokableInstance node)
    {
        int num = node.config().num();
        assertEquals("value files of node " + num, 0,
                     (int) node.callOnInstance(() -> FileValueStore.filesOf(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata()).length));
        assertEquals("commit log segment mappings of node " + num, 0, (int) node.callOnInstance(() -> CommitLogSegmentMappings.liveCount()));
    }

    private static int liveSSTableCount(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables().size());
    }

    /// The current memtable of the table; to be called on an instance.
    private static TrieMemtable trieMemtable()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        Memtable memtable = cfs.getCurrentMemtable();
        if (!(memtable instanceof TrieMemtable))
            throw new AssertionError("Expected a TrieMemtable, got " + memtable);
        return (TrieMemtable) memtable;
    }
}
