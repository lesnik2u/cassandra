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
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/// Read paths of a loaded [TrieMemtable] with `valueStorage` `native` (values in memtable memory) or `file` (values
/// larger than a trie cell in the per-shard value log): point reads of recently written and of random partitions,
/// and a full scan through the flush view.
///
/// The memtable is loaded once per trial with `partitions` partitions of `rowsPerPartition` rows, each row holding one
/// `blob` cell of `valueSize` bytes, i.e. `partitions * rowsPerPartition * valueSize` value bytes (with the defaults,
/// 10 MB for 64-byte values, 164 MB for 1 KiB and 2.6 GB for 16 KiB). The partitions are written in the order of
/// `keys`, so the last entries of `keys` are the most recently written. After loading, setup prints the memtable
/// memory usage and the value log size.
///
/// Every read copies each value it returns into a scratch array, the way a reader serialising the value would, so
/// that every byte of the value is read from wherever the memtable keeps it. Point reads go through
/// [TrieMemtable#rowIterator], which already copies the rows onto the heap; [#scanAll] goes through
/// [TrieMemtable#getFlushSet], which does not copy, and the scratch copy is the only read of the value.
///
/// Whether [#pointReadCold] reads from disk depends on the value log fitting in the page cache, which it does
/// whenever the data set is smaller than the free memory of the machine; the benchmark does not evict it.
///
/// The bootstrap is the one of [TrieMemtablePutBench], including `offheap_objects` and its restrictions on in-process
/// (`-f 0`) runs; under `-f 0` the `@Fork` JVM arguments must be given on the command line.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-ea", "-XX:CompileCommandFile=conf/hotspot_compiler" })
@State(Scope.Benchmark)
public class TrieMemtableReadBench
{
    /// Bytes per cell value. Values up to 16 bytes are embedded in the trie cell with either value storage.
    @Param({"64", "1024", "16384"})
    int valueSize = 1024;

    /// The `value_storage` option of the memtable: `native` keeps values in memtable memory, `file` in the value log.
    @Param({"native", "file"})
    String valueStorage = "native";

    @Param({"20000"})
    int partitions = 20000;

    @Param({"8"})
    int rowsPerPartition = 8;

    /// Distinct value buffers the cells are filled from; the content does not matter to the memtable.
    static final int VALUE_POOL_SIZE = 256;
    static final long BASE_TIMESTAMP = 1_700_000_000_000_000L;

    TableMetadata metadata;
    ColumnMetadata column;
    TrieMemtable memtable;
    DecoratedKey[] keys;
    byte[] scratch;
    int hotIndex;
    Random random;

    final OpOrder writeOrder = new OpOrder();
    final OpOrder readOrder = new OpOrder();

    @Setup(Level.Trial)
    public void setup() throws Throwable
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
            return config;
        });

        metadata = TableMetadata.builder("ks", "t")
                                .partitioner(Murmur3Partitioner.instance)
                                .memtable(MemtableParams.get("trie"))
                                .addPartitionKeyColumn("pk", LongType.instance)
                                .addClusteringColumn("ck", LongType.instance)
                                .addRegularColumn("v", BytesType.instance)
                                .build();
        column = metadata.getColumn(ByteBufferUtil.bytes("v"));
        Memtable.Factory memtableFactory = TrieMemtablePutBench.memtableFactory(valueStorage);
        if (TrieMemtable.BUFFER_TYPE != BufferType.OFF_HEAP)
            throw new AssertionError("TrieMemtable initialised with " + TrieMemtable.BUFFER_TYPE + " before the allocation type could be set");
        if (!(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool))
            throw new AssertionError("Memtable pool initialised as " + AbstractAllocatorMemtable.MEMORY_POOL.getClass().getSimpleName() + " before the allocation type could be set");

        Random rand = new Random(1);
        ByteBuffer[] values = new ByteBuffer[VALUE_POOL_SIZE];
        for (int i = 0; i < values.length; ++i)
        {
            byte[] value = new byte[valueSize];
            rand.nextBytes(value);
            values[i] = ByteBuffer.wrap(value);
        }
        scratch = new byte[valueSize];

        keys = new DecoratedKey[partitions];
        for (int i = 0; i < partitions; ++i)
            keys[i] = metadata.partitioner.decorateKey(LongType.instance.decompose(rand.nextLong()));

        memtable = (TrieMemtable) memtableFactory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(metadata), owner);
        PartitionUpdate.Factory updateFactory = memtableFactory.partitionUpdateFactory();
        try (OpOrder.Group writeGroup = writeOrder.start())
        {
            for (int i = 0; i < partitions; ++i)
            {
                PartitionUpdate.Builder update = updateFactory.builder(metadata, keys[i], metadata.regularAndStaticColumns(), rowsPerPartition);
                for (int r = 0; r < rowsPerPartition; ++r)
                {
                    Row.Builder row = updateFactory.rowBuilder(metadata.regularColumns(), false);
                    row.newRow(TrieMemtablePutBench.clustering(r));
                    row.addCell(BufferCell.live(column, BASE_TIMESTAMP + i, values[(i * rowsPerPartition + r) % values.length]));
                    update.add(row.build());
                }
                memtable.put(update.build(), UpdateTransaction.NO_OP, writeGroup);
            }
        }
        if (memtable.partitionCount() != partitions)
            throw new AssertionError("Memtable has " + memtable.partitionCount() + " partitions instead of " + partitions);

        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        long valueLogBytes = memtable.valueLogBytes();
        System.out.printf("%nTrieMemtableReadBench loaded: valueStorage=%s valueSize=%d partitions=%d rowsPerPartition=%d " +
                          "valueBytes=%d ownsOnHeap=%d ownsOffHeap=%d valueLogBytes=%d (%s, value log %s)%n",
                          valueStorage, valueSize, partitions, rowsPerPartition,
                          (long) partitions * rowsPerPartition * valueSize,
                          usage.ownsOnHeap, usage.ownsOffHeap, valueLogBytes,
                          usage, FBUtilities.prettyPrintMemory(valueLogBytes));

        hotIndex = 0;
        random = new Random(2);
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        OpOrder.Barrier barrier = writeOrder.newBarrier();
        barrier.issue();
        memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
        barrier.await();
        memtable.discard();
        memtable = null;
    }

    /// Reads a partition from the last 1% written.
    @Benchmark
    public int pointReadHot(Blackhole bh)
    {
        return readPartition(keys[keys.length - 1 - (hotIndex++ % (keys.length / 100))], bh);
    }

    /// Reads a uniformly random partition.
    @Benchmark
    public int pointReadCold(Blackhole bh)
    {
        return readPartition(keys[random.nextInt(keys.length)], bh);
    }

    /// Reads every partition through the view flush writes from, which does not copy the values onto the heap.
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long scanAll(Blackhole bh)
    {
        long rows = 0;
        for (TrieBackedPartition partition : memtable.getFlushSet(null, null))
        {
            try (UnfilteredRowIterator it = partition.unfilteredIterator())
            {
                while (it.hasNext())
                {
                    consumeValue((Row) it.next(), bh);
                    ++rows;
                }
            }
        }
        return rows;
    }

    private int readPartition(DecoratedKey key, Blackhole bh)
    {
        int rows = 0;
        try (UnfilteredRowIterator it = memtable.rowIterator(key, Slices.ALL, ColumnFilter.all(metadata), false, SSTableReadsListener.NOOP_LISTENER))
        {
            while (it.hasNext())
            {
                consumeValue((Row) it.next(), bh);
                ++rows;
            }
        }
        return rows;
    }

    private void consumeValue(Row row, Blackhole bh)
    {
        ByteBuffer value = row.getCell(column).buffer();
        ByteBufferUtil.copyBytes(value, value.position(), scratch, 0, value.remaining());
        bh.consume(scratch);
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
