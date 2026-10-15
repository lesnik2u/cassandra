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

package org.apache.cassandra.test.microbench.tries;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.memtable.TrieMemtableFactory;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

/// Write path of [TrieMemtable] end to end: `TrieMemtable.put` of a [PartitionUpdate] built the way a live
/// mutation builds it, i.e. through the memtable factory's `partitionUpdateFactory()` (`TriePartitionUpdateStage2`),
/// so that every put converts the update with `TriePartitionUpdate.asMergableTrie` before merging it in.
///
/// Inside the measurement: `TrieMemtable.put`, which is the `asMergableTrie` conversion, the shard lock, the
/// `InMemoryDeletionAwareTrie` mutation (including `applyDeletionBranch` when the partition has or receives range
/// tombstones), copying the cells into memtable memory, and the shard statistics. Also inside, amortised over
/// `partitions * updatesPerPartition` puts: replacing the memtable with a fresh one (discarding the old) when the
/// pool of updates wraps, plus, for [#putWithExistingRangeDeletions], seeding the fresh memtable with one range
/// tombstone per partition.
///
/// Outside the measurement: building the updates (row builders, the update's BTree), the commit log, `Mutation`
/// and `Keyspace.apply`, index transactions and the memtable space check (`checkSpaceAndPut`). The write `OpOrder`
/// group is held for a whole memtable lifetime rather than started per mutation.
///
/// The updates are pre-generated once per trial with strictly increasing timestamps. When the pool wraps, the
/// memtable is replaced, so every update is applied to a memtable that has only seen older timestamps and cells are
/// really overwritten instead of reconciled against identical copies. This also bounds the memtable size to one
/// pass over the pool, so no flush is ever required.
///
/// The memtable uses `offheap_objects`, the default in `conf/cassandra.yaml`, regardless of the test configuration.
/// This is fixed at class initialisation of the memtable, so it cannot be a parameter of an in-process (`-f 0`) run.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // memtable shards take writes under a lock, no point in concurrent writes
@State(Scope.Benchmark)
public class TrieMemtablePutBench
{
    @Param({"1", "10"})
    int rowsPerUpdate = 10;

    @Param({"4"})
    int cellsPerRow = 4;

    /// When true, each update also carries one range tombstone covering `RANGE_TOMBSTONE_WIDTH` clusterings just
    /// after its rows, so the memtable accumulates deletion branches and later puts to the same partition run
    /// `applyDeletionBranch` with an existing alternate branch.
    @Param({"false", "true"})
    boolean rangeDeletion = true;

    /// Number of distinct partitions; updates cycle through them, so all but the first put to a partition merge into
    /// existing data.
    @Param({"1000"})
    int partitions = 1000;

    /// Puts per partition in one memtable lifetime; the update pool has `partitions * updatesPerPartition` entries.
    @Param({"16"})
    int updatesPerPartition = 16;

    /// Clusterings a partition's rows are chosen from, so that rows are both inserted and overwritten.
    static final int CLUSTERINGS_PER_PARTITION = 64;
    static final int RANGE_TOMBSTONE_WIDTH = 4;
    static final long BASE_TIMESTAMP = 1_700_000_000_000_000L;

    TableMetadata metadata;
    Memtable.Factory memtableFactory;
    PartitionUpdate[] updates;
    /// One partition-wide range tombstone per partition, older than all updates. Only used by
    /// [#putWithExistingRangeDeletions].
    PartitionUpdate[] seedUpdates;

    final OpOrder writeOrder = new OpOrder();
    final OpOrder readOrder = new OpOrder();
    Memtable memtable;
    OpOrder.Group writeGroup;
    int next;

    @Setup(Level.Trial)
    public void setup(BenchmarkParams params) throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        setOffHeapObjectsAllocation();

        TableMetadata.Builder builder = TableMetadata.builder("ks", "t")
                                                     .partitioner(Murmur3Partitioner.instance)
                                                     .memtable(MemtableParams.get("trie"))
                                                     .addPartitionKeyColumn("pk", LongType.instance)
                                                     .addClusteringColumn("ck", LongType.instance);
        for (int i = 0; i < cellsPerRow; ++i)
            builder.addRegularColumn("v" + i, LongType.instance);
        metadata = builder.build();
        memtableFactory = metadata.params.memtable.factory();
        if (!(memtableFactory instanceof TrieMemtableFactory))
            throw new AssertionError("Expected a trie memtable factory, got " + memtableFactory);
        if (TrieMemtable.BUFFER_TYPE != BufferType.OFF_HEAP)
            throw new AssertionError("TrieMemtable initialised with " + TrieMemtable.BUFFER_TYPE + " before the allocation type could be set");

        PartitionUpdate.Factory updateFactory = memtableFactory.partitionUpdateFactory();
        Random rand = new Random(1);
        long nowInSec = FBUtilities.nowInSeconds();
        DecoratedKey[] keys = new DecoratedKey[partitions];
        for (int i = 0; i < partitions; ++i)
            keys[i] = metadata.partitioner.decorateKey(LongType.instance.decompose(rand.nextLong()));

        updates = new PartitionUpdate[partitions * updatesPerPartition];
        for (int i = 0; i < updates.length; ++i)
        {
            long timestamp = BASE_TIMESTAMP + i;
            PartitionUpdate.Builder update = updateFactory.builder(metadata, keys[i % partitions], metadata.regularAndStaticColumns(), rowsPerUpdate);
            int firstClustering = rand.nextInt(CLUSTERINGS_PER_PARTITION - rowsPerUpdate - RANGE_TOMBSTONE_WIDTH + 1);
            for (int r = 0; r < rowsPerUpdate; ++r)
            {
                // Built the way UpdateParameters builds an UPDATE: unsorted builder, no primary key liveness.
                Row.Builder row = updateFactory.rowBuilder(metadata.regularColumns(), false);
                row.newRow(clustering(firstClustering + r));
                for (ColumnMetadata column : metadata.regularColumns())
                    row.addCell(BufferCell.live(column, timestamp, LongType.instance.decompose(rand.nextLong())));
                update.add(row.build());
            }
            if (rangeDeletion)
            {
                int start = firstClustering + rowsPerUpdate;
                update.add(new RangeTombstone(Slice.make(clustering(start), clustering(start + RANGE_TOMBSTONE_WIDTH - 1)),
                                              DeletionTime.build(timestamp, nowInSec)));
            }
            updates[i] = update.build();
        }

        if (params.getBenchmark().endsWith(".putWithExistingRangeDeletions"))
        {
            seedUpdates = new PartitionUpdate[partitions];
            for (int i = 0; i < partitions; ++i)
            {
                PartitionUpdate.Builder update = updateFactory.builder(metadata, keys[i], metadata.regularAndStaticColumns(), 0);
                update.add(new RangeTombstone(Slice.make(clustering(0), clustering(CLUSTERINGS_PER_PARTITION - 1)),
                                              DeletionTime.build(BASE_TIMESTAMP - 1, nowInSec)));
                seedUpdates[i] = update.build();
            }
        }

        createMemtable();
    }

    static void setOffHeapObjectsAllocation() throws ReflectiveOperationException
    {
        Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
        confField.setAccessible(true);
        Config conf = (Config) confField.get(null);
        conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
    }

    static Clustering<?> clustering(long ck)
    {
        return Clustering.make(LongType.instance.decompose(ck));
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        discardMemtable();
    }

    @Benchmark
    public long put()
    {
        return putNext();
    }

    /// Same as [#put], but every partition is seeded with an older range tombstone covering all its clusterings
    /// when the memtable is created, so every put hits an existing deletion branch, whether or not the update itself
    /// carries a range tombstone (`rangeDeletion`).
    @Benchmark
    public long putWithExistingRangeDeletions()
    {
        return putNext();
    }

    private long putNext()
    {
        if (next == updates.length)
        {
            if (memtable.partitionCount() != partitions)
                throw new AssertionError("Memtable has " + memtable.partitionCount() + " partitions instead of " + partitions);
            discardMemtable();
            createMemtable();
        }
        return memtable.put(updates[next++], UpdateTransaction.NO_OP, writeGroup);
    }

    private void createMemtable()
    {
        memtable = memtableFactory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(metadata), owner);
        writeGroup = writeOrder.start();
        if (seedUpdates != null)
            for (PartitionUpdate seed : seedUpdates)
                memtable.put(seed, UpdateTransaction.NO_OP, writeGroup);
        next = 0;
    }

    private void discardMemtable()
    {
        writeGroup.close();
        OpOrder.Barrier barrier = writeOrder.newBarrier();
        barrier.issue();
        memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
        barrier.await();
        memtable.discard();
        memtable = null;
    }

    private final Memtable.Owner owner = new Memtable.Owner()
    {
        public Future<CommitLogPosition> signalFlushRequired(Memtable memtable, ColumnFamilyStore.FlushReason reason)
        {
            throw new AssertionError("Memtable requested a flush (" + reason + "); the benchmark must not need one");
        }

        public Memtable getCurrentMemtable()
        {
            return memtable;
        }

        public Iterable<Memtable> getIndexMemtables()
        {
            return Collections.emptyList();
        }

        public ShardBoundaries localRangeSplits(int shardCount)
        {
            return ShardBoundaries.NONE;
        }

        public OpOrder readOrdering()
        {
            return readOrder;
        }

        public int getMemtableFlushPeriodInMs()
        {
            return 0;
        }
    };
}
