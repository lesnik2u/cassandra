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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.WriteOrigin;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.memtable.TrieMemtableFactory;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.mvcc.CommitLogSegmentMappings;
import org.apache.cassandra.db.tries.mvcc.FileValueStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativePool;
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

/// The local write path of a mutation into a [TrieMemtable], commit log included: per operation, a [Mutation] of one
/// [PartitionUpdate] is written to `CommitLog.instance` and the update is put into the memtable with a
/// [CassandraWriteContext] carrying the commit log position and, when value locations are recorded, where the values
/// were written in the segment, as `CassandraKeyspaceWriteHandler.beginWrite` and `ColumnFamilyStore.apply` do. This
/// compares the `value_storage` options end to end: `native` copies each value into memtable memory, `file` into a
/// per-shard value log, and `commitlog` refers to the copy the commit log just wrote.
///
/// Inside the measurement: building the `Mutation` (a wrapper around the pre-built update), the commit log append
/// (serializing the mutation, recording value locations when `SerializedValueOffsets.ENABLED`, allocating in the
/// segment and copying the entry into it, which may wait for the segment manager to provide a new segment or for a
/// lagging periodic sync) and `TrieMemtable.put`. Outside: building the updates, `Keyspace.apply`, index transactions
/// and the memtable space check. The commit log is started in process with `commitlog_sync: periodic` (10 s),
/// `commitlog_disk_access_mode: mmap`, no compression, no encryption and 32 MiB segments, which is what
/// `conf/cassandra.yaml` resolves to, rather than the test configuration's 5 MiB segments.
/// `SerializedValueOffsets.ENABLED` is set by building a `commitlog` memtable factory, as on a node, so `native` and
/// `file` take the plain `CommitLog.add` path when run in their own JVM, which is how every run should be made.
///
/// The memtable is replaced when the pool of `partitions * updatesPerPartition` updates wraps, as in
/// [TrieMemtablePutBench]; the replaced memtable is then handled on a background thread the way a flush handles it:
/// once its writes are done, the commit log segments it covered are marked clean (and deleted when no longer in use),
/// then the memtable is discarded, releasing its value log files and commit log segment mappings. A replacement waits
/// for the previous discard to finish, so at most one replaced memtable is alive next to the current one. Without a
/// [#targetRatePerSec] the replacement on the put thread is inside the measurement of the put that wraps the pool;
/// with one, it runs before the wait for the next put's start.
///
/// The trial ends by discarding the last memtable, checking that no commit log segment mapping and no value log file
/// is left, and stopping the commit log and deleting its segments (`CommitLog.instance.stopUnsafe`). It prints the
/// number of writes, the bytes the memtables stored in value logs or commit log references, how many of those
/// `commitlog` storage had to copy, and the memory usage of the last memtable replaced when full.
///
/// As for [TrieMemtablePutBench], the memtable uses `offheap_objects` regardless of the test configuration, and under
/// `-f 0` the JVM arguments must be given on the command line (`-ea`, `-XX:CompileCommandFile=conf/hotspot_compiler`,
/// heap size). `-Dcassandra.skip_sync=true` would turn the commit log syncs into no-ops.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-XX:CompileCommandFile=conf/hotspot_compiler", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // memtable shards take writes under a lock, no point in concurrent writes
@State(Scope.Benchmark)
public class CommitLogPutBench
{
    @Param({"10"})
    int rowsPerUpdate = 10;

    @Param({"4"})
    int cellsPerRow = 4;

    /// Number of distinct partitions; updates cycle through them, so all but the first put to a partition merge into
    /// existing data.
    @Param({"100"})
    int partitions = 100;

    /// Puts per partition in one memtable lifetime; the update pool has `partitions * updatesPerPartition` entries.
    @Param({"16"})
    int updatesPerPartition = 16;

    /// Clusterings per partition the updates are drawn from.
    @Param({"64"})
    int clusteringsPerPartition = 64;

    /// Bytes per cell value.
    @Param({"1024", "16384"})
    int valueSize = 1024;

    /// The `value_storage` option of the memtable.
    @Param({"native", "file", "commitlog"})
    String valueStorage = "native";

    /// Writes per second, or 0 for no limit, scheduled as in [TrieMemtablePutBench#targetRatePerSec]. Use with
    /// `-bm sample`; every iteration prints the rate achieved and the time the memtable replacements took on the put
    /// thread.
    @Param({"0", "5000"})
    int targetRatePerSec = 0;

    static final int COMMITLOG_SEGMENT_SIZE_MIB = 32;
    static final String COMMITLOG_SYNC_PERIOD = "10s";
    static final long BASE_TIMESTAMP = 1_700_000_000_000_000L;

    TableMetadata metadata;
    Memtable.Factory memtableFactory;
    PartitionUpdate[] updates;

    final OpOrder writeOrder = new OpOrder();
    final OpOrder readOrder = new OpOrder();
    final TrieMemtablePutBench.BackgroundDiscard backgroundDiscard = new TrieMemtablePutBench.BackgroundDiscard();
    Memtable memtable;
    CommitLogPosition memtableLowerBound;
    OpOrder.Group writeGroup;
    int next;

    long writes;
    long valueLogBytes;
    long valueLogFallbackBytes;
    Memtable.MemoryUsage replacedMemtableUsage;

    long iterationStartNanos;
    long iterationPuts;
    long iterationReplacements;
    long iterationReplaceNanos;

    @Setup(Level.Trial)
    public void setup()
    {
        if (DatabaseDescriptor.isDaemonInitialized() &&
            DatabaseDescriptor.getMemtableAllocationType() != Config.MemtableAllocationType.offheap_objects)
            throw new IllegalStateException("DatabaseDescriptor already initialised with memtable_allocation_type=" +
                                            DatabaseDescriptor.getMemtableAllocationType() +
                                            " in this JVM; run this benchmark alone or forked");
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
            config.memtable_offheap_space = new DataStorageSpec.IntMebibytesBound(TrieMemtablePutBench.MEMTABLE_OFFHEAP_SPACE_MIB);
            config.commitlog_sync = Config.CommitLogSync.periodic;
            config.commitlog_sync_period = new DurationSpec.IntMillisecondsBound(COMMITLOG_SYNC_PERIOD);
            config.commitlog_segment_size = new DataStorageSpec.IntMebibytesBound(COMMITLOG_SEGMENT_SIZE_MIB);
            config.commitlog_disk_access_mode = Config.DiskAccessMode.mmap;
            config.commitlog_compression = null;
            return config;
        });
        if (DatabaseDescriptor.getCommitLogWriteDiskAccessMode() != Config.DiskAccessMode.mmap ||
            DatabaseDescriptor.getCommitLogSegmentSize() != COMMITLOG_SEGMENT_SIZE_MIB << 20)
            throw new IllegalStateException("DatabaseDescriptor already initialised with another commit log configuration " +
                                            "in this JVM; run this benchmark alone or forked");
        CommitLog.instance.start();

        TableMetadata.Builder builder = TableMetadata.builder("ks", "t")
                                                     .partitioner(Murmur3Partitioner.instance)
                                                     .memtable(MemtableParams.get("trie"))
                                                     .addPartitionKeyColumn("pk", LongType.instance)
                                                     .addClusteringColumn("ck", LongType.instance);
        for (int i = 0; i < cellsPerRow; ++i)
            builder.addRegularColumn("v" + i, BytesType.instance);
        metadata = builder.build();
        memtableFactory = TrieMemtablePutBench.memtableFactory(valueStorage);
        if (!(memtableFactory instanceof TrieMemtableFactory))
            throw new AssertionError("Expected a trie memtable factory, got " + memtableFactory);
        if (TrieMemtable.BUFFER_TYPE != BufferType.OFF_HEAP)
            throw new AssertionError("TrieMemtable initialised with " + TrieMemtable.BUFFER_TYPE + " before the allocation type could be set");
        if (!(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool))
            throw new AssertionError("Memtable pool initialised as " + AbstractAllocatorMemtable.MEMORY_POOL.getClass().getSimpleName() + " before the allocation type could be set");
        if (SerializedValueOffsets.ENABLED != valueStorage.equals("commitlog"))
            throw new AssertionError("SerializedValueOffsets.ENABLED is " + SerializedValueOffsets.ENABLED + " with value_storage " + valueStorage +
                                     "; run each value storage in its own JVM");

        PartitionUpdate.Factory updateFactory = memtableFactory.partitionUpdateFactory();
        ByteBuffer[] values = new ByteBuffer[rowsPerUpdate * cellsPerRow];
        Random valueRand = new Random(2);
        for (int i = 0; i < values.length; ++i)
        {
            byte[] value = new byte[valueSize];
            valueRand.nextBytes(value);
            values[i] = ByteBuffer.wrap(value);
        }

        Random rand = new Random(1);
        DecoratedKey[] keys = new DecoratedKey[partitions];
        for (int i = 0; i < partitions; ++i)
            keys[i] = metadata.partitioner.decorateKey(LongType.instance.decompose(rand.nextLong()));

        updates = new PartitionUpdate[partitions * updatesPerPartition];
        for (int i = 0; i < updates.length; ++i)
        {
            long timestamp = BASE_TIMESTAMP + i;
            PartitionUpdate.Builder update = updateFactory.builder(metadata, keys[i % partitions], metadata.regularAndStaticColumns(), rowsPerUpdate);
            int firstClustering = rand.nextInt(clusteringsPerPartition - rowsPerUpdate + 1);
            for (int r = 0; r < rowsPerUpdate; ++r)
            {
                // Built the way UpdateParameters builds an UPDATE: unsorted builder, no primary key liveness.
                Row.Builder row = updateFactory.rowBuilder(metadata.regularColumns(), false);
                row.newRow(TrieMemtablePutBench.clustering(firstClustering + r));
                int c = 0;
                for (ColumnMetadata column : metadata.regularColumns())
                    row.addCell(BufferCell.live(column, timestamp, values[r * cellsPerRow + c++]));
                update.add(row.build());
            }
            updates[i] = update.build();
        }

        createMemtable();
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        try
        {
            backgroundDiscard.await();
            switchOutMemtable().run();

            int liveMappings = CommitLogSegmentMappings.liveCount();
            File[] valueLogFiles = FileValueStore.directory().tryList();
            System.out.printf(Locale.ROOT, "%nValue storage %s: %d writes, %d value bytes stored outside memtable memory, %d of them copied by the commitlog fallback; " +
                                           "after the last discard %d commit log segment mappings and %d value log files are left; last replaced memtable: %s%n",
                              valueStorage, writes, valueLogBytes, valueLogFallbackBytes, liveMappings, valueLogFiles == null ? 0 : valueLogFiles.length,
                              replacedMemtableUsage);
            if (liveMappings != 0)
                throw new AssertionError(liveMappings + " commit log segment mappings left after discarding every memtable");
            if (valueLogFiles != null && valueLogFiles.length != 0)
                throw new AssertionError("Value log files left after discarding every memtable: " + Arrays.toString(valueLogFiles));
        }
        finally
        {
            // not resetUnsafe, which replays the last segments and restarts the commit log, whose threads would keep an
            // in-process (-f 0) run from exiting
            CommitLog.instance.stopUnsafe(true);
        }
    }

    @Setup(Level.Iteration)
    public void startIteration()
    {
        iterationPuts = 0;
        iterationReplacements = 0;
        iterationReplaceNanos = 0;
        iterationStartNanos = System.nanoTime();
    }

    @TearDown(Level.Iteration)
    public void endIteration()
    {
        if (targetRatePerSec == 0)
            return;
        double seconds = (System.nanoTime() - iterationStartNanos) / 1e9;
        System.out.printf(Locale.ROOT, "%nAchieved rate: %.0f writes/s (target %d), %d writes in %.3f s, %d memtable replacements took %.3f s%n",
                          iterationPuts / seconds, targetRatePerSec, iterationPuts, seconds, iterationReplacements, iterationReplaceNanos / 1e9);
    }

    /// With a [#targetRatePerSec], replaces the memtable if the pool has wrapped and waits for the next write's
    /// scheduled start. Runs before every write and outside its measurement.
    @Setup(Level.Invocation)
    public void awaitSchedule()
    {
        if (targetRatePerSec == 0)
            return;
        if (next == updates.length)
        {
            long replaceStart = System.nanoTime();
            replaceMemtableIfWrapped();
            iterationReplaceNanos += System.nanoTime() - replaceStart;
            ++iterationReplacements;
        }
        TrieMemtablePutBench.awaitNanoTime(iterationStartNanos + iterationPuts++ * TimeUnit.SECONDS.toNanos(1) / targetRatePerSec);
    }

    @Benchmark
    public long write()
    {
        replaceMemtableIfWrapped();
        PartitionUpdate update = updates[next++];
        Mutation mutation = new Mutation(update);
        // as CassandraKeyspaceWriteHandler.beginWrite
        CassandraWriteContext context = CassandraWriteContext.addToCommitLog(writeGroup, mutation, WriteOptions.DEFAULT, WriteOrigin.LOCAL, null);
        return memtable.put(update, UpdateTransaction.NO_OP, writeGroup, context);
    }

    private void replaceMemtableIfWrapped()
    {
        if (next == updates.length)
        {
            if (memtable.partitionCount() != partitions)
                throw new AssertionError("Memtable has " + memtable.partitionCount() + " partitions instead of " + partitions);
            backgroundDiscard.start(switchOutMemtable());
            createMemtable();
        }
    }

    private void createMemtable()
    {
        memtableLowerBound = CommitLog.instance.getCurrentPosition();
        memtable = memtableFactory.create(new AtomicReference<>(memtableLowerBound), TableMetadataRef.forOfflineTools(metadata), owner);
        writeGroup = writeOrder.start();
        next = 0;
    }

    /// Switches the memtable out, and returns the task that, as a flush followed by its reclaim, waits for the
    /// memtable's writes, marks the commit log segments it covered clean and discards it. The write barrier is issued
    /// here, before the next memtable's write group is started, so that it does not wait for that group.
    private Runnable switchOutMemtable()
    {
        writeGroup.close();
        OpOrder.Barrier barrier = writeOrder.newBarrier();
        barrier.issue();
        TrieMemtable switched = (TrieMemtable) memtable;
        CommitLogPosition lowerBound = memtableLowerBound;
        CommitLogPosition upperBound = CommitLog.instance.getCurrentPosition();
        if (next == updates.length)
            replacedMemtableUsage = Memtable.getMemoryUsage(switched);
        switched.switchOut(barrier, new AtomicReference<>(upperBound));
        writes += next;
        valueLogBytes += switched.valueLogBytes();
        valueLogFallbackBytes += switched.valueLogFallbackBytes();
        memtable = null;
        return () -> {
            barrier.await();
            CommitLog.instance.discardCompletedSegments(metadata.id, lowerBound, upperBound);
            switched.discard();
        };
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
