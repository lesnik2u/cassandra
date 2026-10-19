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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;

import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.offHeapOwned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Tests [TrieMemtable.CellDataBufferManager] with a stub manager standing in for a store outside memtable memory: the
/// [TrieMemtable.CellDataBufferManager#valueLocations] field a put sets and clears, the sums of
/// [TrieMemtable.CellDataBufferManager#valueLogBytes] and [TrieMemtable.CellDataBufferManager#fallbackBytes], and
/// closing on discard. Also checks the registration of the table's value log gauges, which count a memtable
/// with real stores until it is discarded.
public class TrieMemtableCellDataManagerTest
{
    private static final int VALUE_SIZE = 64;   // larger than a trie cell, so values go through the manager

    private static TableMetadata metadata;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // value storage outside memtable memory requires the off-heap pool; set before the pool is created
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        assertTrue(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
        metadata = TableMetadata.builder("ks", "cell_data_manager")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addRegularColumn("v", BytesType.instance)
                                .build();
    }

    @Test
    public void discardClosesEveryManagerAndReleasesMemoryEvenIfOneCloseFails()
    {
        // three shards: pk < 100, 100 <= pk < 200 and pk >= 200 (byte-ordered keys)
        TestOwner owner = new TestOwner(new ShardBoundaries(new Token[]{ token(99), token(199) }, 0));
        StubManager[] managers = { new StubManager(false), new StubManager(true), new StubManager(false) };
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        long usedBefore = pool.onHeap.used() + pool.offHeap.used();

        TrieMemtable memtable = memtable(owner, shard -> managers[shard]);
        put(owner, memtable, 1);
        put(owner, memtable, 150);
        put(owner, memtable, 250);
        for (StubManager manager : managers)
            assertEquals(VALUE_SIZE, manager.valueLogBytes());
        assertTrue(pool.onHeap.used() + pool.offHeap.used() > usedBefore);

        OpOrder.Barrier barrier = owner.readOrdering().newBarrier();
        barrier.issue();
        memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
        try
        {
            memtable.discard();
            fail("expected the failing close to be reported");
        }
        catch (RuntimeException e)
        {
            assertEquals("close failed", e.getMessage());
        }
        assertEquals("memtable memory released", usedBefore, pool.onHeap.used() + pool.offHeap.used());
        for (StubManager manager : managers)
            assertEquals(1, manager.closeCount);
    }

    /// The bytes a manager reports are charged to the shard's off-heap memory, exactly, by the adjustment after each
    /// update, also when the put fails after the manager counted them. Compared with a memtable whose manager reports
    /// nothing and which receives the same puts, so that the tries are the same size.
    @Test
    public void putChargesTheManagersBytesToTheShardAllocator()
    {
        TestOwner owner = new TestOwner(ShardBoundaries.NONE);
        StubManager counted = new StubManager(false);
        StubManager uncounted = new StubManager(false);
        uncounted.uncounted = true;
        TrieMemtable memtable = memtable(owner, shard -> counted);
        TrieMemtable reference = memtable(owner, shard -> uncounted);

        for (int pk = 0; pk < 3; pk++)
        {
            put(owner, memtable, pk);
            put(owner, reference, pk);
        }
        assertEquals(3L * VALUE_SIZE, counted.valueLogBytes());
        assertEquals(counted.valueLogBytes(), offHeapOwned(memtable) - offHeapOwned(reference));

        counted.failAfterStore = true;
        uncounted.failAfterStore = true;
        for (TrieMemtable m : new TrieMemtable[]{ memtable, reference })
        {
            try
            {
                put(owner, m, 3);
                fail("expected the failing store to be reported");
            }
            catch (RuntimeException e)
            {
                assertEquals("store failed after counting", e.getMessage());
            }
        }
        assertEquals(4L * VALUE_SIZE, counted.valueLogBytes());
        assertEquals("a failed put is charged too", counted.valueLogBytes(), offHeapOwned(memtable) - offHeapOwned(reference));
    }

    @Test
    public void putDeliversTheWriteContextValueLocationsToTheManager()
    {
        TestOwner owner = new TestOwner(ShardBoundaries.NONE);
        StubManager manager = new StubManager(false);
        TrieMemtable memtable = memtable(owner, shard -> manager);
        CommitLogValueLocations locations = new CommitLogValueLocations(1, new File("CommitLog-7-1.log"), 1 << 20, 100,
                                                                        new SerializedValueOffsets());

        try (OpOrder.Group group = owner.readOrdering().start())
        {
            CassandraWriteContext context = new CassandraWriteContext(group, CommitLogPosition.NONE, null, null, null, locations);
            memtable.put(update(1), UpdateTransaction.NO_OP, group, context);
            // a context without value locations, e.g. a write that skipped the commit log
            memtable.put(update(2), UpdateTransaction.NO_OP, group, new CassandraWriteContext(group, CommitLogPosition.NONE));
            memtable.put(update(3), UpdateTransaction.NO_OP, group, null);
            // the outermost write of a mutation goes through the memory limit check
            memtable.checkSpaceAndPut(update(4), UpdateTransaction.NO_OP, group, context);
        }

        assertEquals("the value locations in place while the value is stored", Arrays.asList(locations, null, null, locations), manager.storedValueLocations);
        assertNull("the value locations are cleared when the put ends", manager.valueLocations);
    }

    @Test
    public void threeArgumentPutDeliversNoValueLocations()
    {
        TestOwner owner = new TestOwner(ShardBoundaries.NONE);
        StubManager manager = new StubManager(false);
        TrieMemtable memtable = memtable(owner, shard -> manager);

        put(owner, memtable, 1);

        assertEquals(Collections.singletonList(null), manager.storedValueLocations);
        assertNull(manager.valueLocations);
    }

    @Test
    public void putClearsTheValueLocationsWhenTheStoreFails()
    {
        TestOwner owner = new TestOwner(ShardBoundaries.NONE);
        StubManager manager = new StubManager(false);
        manager.failOnStore = true;
        TrieMemtable memtable = memtable(owner, shard -> manager);
        CommitLogValueLocations locations = new CommitLogValueLocations(1, new File("CommitLog-7-1.log"), 1 << 20, 100,
                                                                        new SerializedValueOffsets());

        try (OpOrder.Group group = owner.readOrdering().start())
        {
            CassandraWriteContext context = new CassandraWriteContext(group, CommitLogPosition.NONE, null, null, null, locations);
            memtable.put(update(1), UpdateTransaction.NO_OP, group, context);
            fail("expected the failing store to be reported");
        }
        catch (RuntimeException e)
        {
            assertEquals("store failed", e.getMessage());
        }

        assertSame(locations, manager.storedValueLocations.get(0));
        assertNull(manager.valueLocations);
    }

    /// The table's gauges count a memtable that keeps its values outside memtable memory, here with the stores it
    /// creates itself, from its creation until its discard.
    @Test
    public void gaugesCountTheMemtableUntilItIsDiscarded()
    {
        // a table of its own: the memtables of the other tests are never discarded
        TableMetadata gaugeMetadata = TableMetadata.builder("ks", "cell_data_manager_gauges")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("v", BytesType.instance)
                                                   .build();
        TrieMemtableMetricsView metrics = TrieMemtableMetricsView.getOrCreate(gaugeMetadata.keyspace, gaugeMetadata.name);
        for (TrieMemtable.ValueStorage valueStorage : new TrieMemtable.ValueStorage[]{ TrieMemtable.ValueStorage.FILE, TrieMemtable.ValueStorage.COMMITLOG })
        {
            TestOwner owner = new TestOwner(ShardBoundaries.NONE);
            TrieMemtable memtable = new TrieMemtable(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(gaugeMetadata),
                                                     owner, null, valueStorage);
            assertEquals(valueStorage.toString(), 0L, (long) metrics.valueLogBytes.getValue());
            assertEquals(valueStorage.toString(), 0L, (long) metrics.valueLogFallbackBytes.getValue());

            for (int pk = 0; pk < 3; pk++)
            {
                try (OpOrder.Group group = owner.readOrdering().start())
                {
                    PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(gaugeMetadata, pk);
                    builder.row().add("v", ByteBuffer.allocate(VALUE_SIZE));
                    memtable.put(builder.build(), UpdateTransaction.NO_OP, group);
                }
            }
            // without commit log value locations, commitlog value storage copies every value into its fallback
            long fallbackBytes = valueStorage == TrieMemtable.ValueStorage.COMMITLOG ? 3L * VALUE_SIZE : 0;
            assertEquals(valueStorage.toString(), 3L * VALUE_SIZE, (long) metrics.valueLogBytes.getValue());
            assertEquals(valueStorage.toString(), fallbackBytes, (long) metrics.valueLogFallbackBytes.getValue());

            OpOrder.Barrier barrier = owner.readOrdering().newBarrier();
            barrier.issue();
            memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
            assertEquals("a switched out memtable is still counted", 3L * VALUE_SIZE, (long) metrics.valueLogBytes.getValue());
            memtable.discard();
            assertEquals(valueStorage.toString(), 0L, (long) metrics.valueLogBytes.getValue());
            assertEquals(valueStorage.toString(), 0L, (long) metrics.valueLogFallbackBytes.getValue());
        }
    }

    @Test
    public void valueLogFallbackBytesSumsShards()
    {
        // three shards: pk < 100, 100 <= pk < 200 and pk >= 200 (byte-ordered keys)
        TestOwner owner = new TestOwner(new ShardBoundaries(new Token[]{ token(99), token(199) }, 0));
        StubManager[] managers = { new StubManager(false), new StubManager(false), new StubManager(false) };
        TrieMemtable memtable = memtable(owner, shard -> managers[shard]);
        managers[0].fallbackBytes = 3;
        managers[2].fallbackBytes = 40;

        assertEquals(43, memtable.valueLogFallbackBytes());
    }

    private static TrieMemtable memtable(TestOwner owner, IntFunction<TrieMemtable.CellDataBufferManager> managers)
    {
        return new TrieMemtable(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(metadata), owner,
                                null, TrieMemtable.ValueStorage.FILE, managers);
    }

    private static Token token(int pk)
    {
        return metadata.partitioner.getToken(Int32Type.instance.decompose(pk));
    }

    private static void put(TestOwner owner, TrieMemtable memtable, int pk)
    {
        try (OpOrder.Group group = owner.readOrdering().start())
        {
            memtable.put(update(pk), UpdateTransaction.NO_OP, group);
        }
    }

    private static PartitionUpdate update(int pk)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk);
        builder.row().add("v", ByteBuffer.allocate(VALUE_SIZE));
        return builder.build();
    }

    /// Keeps values on heap and counts their bytes as the value log size. Records the value locations it is given.
    private static class StubManager extends TrieMemtable.CellDataBufferManager
    {
        private final Map<Long, ByteBuffer> values = new ConcurrentHashMap<>();
        private final boolean failOnClose;
        private volatile long valueLogBytes;
        volatile int closeCount;
        boolean failOnStore;
        /// Whether [#store] throws after the value is stored and counted.
        boolean failAfterStore;
        /// Whether [#valueLogBytes] reports 0, as a manager that keeps its values in memtable memory does.
        boolean uncounted;
        long fallbackBytes;
        final List<CommitLogValueLocations> storedValueLocations = new ArrayList<>();

        StubManager(boolean failOnClose)
        {
            this.failOnClose = failOnClose;
        }

        @Override
        public long store(ByteBuffer buffer, int length)
        {
            storedValueLocations.add(valueLocations);
            if (failOnStore)
                throw new RuntimeException("store failed");
            long handle = valueLogBytes;
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(buffer.duplicate()).flip();
            values.put(handle, copy);
            valueLogBytes = handle + length;
            if (failAfterStore)
                throw new RuntimeException("store failed after counting");
            return handle;
        }

        @Override
        public ByteBuffer load(long handle, int length)
        {
            return values.get(handle).duplicate();
        }

        @Override
        public void release(long handle, int length)
        {
        }

        @Override
        long onHeapSize()
        {
            return 0;
        }

        @Override
        boolean releaseNeeded()
        {
            return false;
        }

        @Override
        void completeMutation()
        {
        }

        @Override
        void abortMutation()
        {
        }

        @Override
        long unusedReservedOnHeapMemory()
        {
            return 0;
        }

        @Override
        void releaseReferencesUnsafe()
        {
        }

        @Override
        long valueLogBytes()
        {
            return uncounted ? 0 : valueLogBytes;
        }

        @Override
        long fallbackBytes()
        {
            return fallbackBytes;
        }

        @Override
        void close()
        {
            ++closeCount;
            if (failOnClose)
                throw new RuntimeException("close failed");
        }
    }

    private static class TestOwner implements Memtable.Owner
    {
        private final ShardBoundaries boundaries;
        private final OpOrder readOrdering = new OpOrder();

        TestOwner(ShardBoundaries boundaries)
        {
            this.boundaries = boundaries;
        }

        public Future<CommitLogPosition> signalFlushRequired(Memtable memtable, ColumnFamilyStore.FlushReason reason)
        {
            return null;
        }

        public Memtable getCurrentMemtable() { return null; }
        public Iterable<Memtable> getIndexMemtables() { return Collections.emptyList(); }
        public ShardBoundaries localRangeSplits(int shardCount) { return boundaries; }
        public OpOrder readOrdering() { return readOrdering; }
        public int getMemtableFlushPeriodInMs() { return Integer.MAX_VALUE; }
    }
}
