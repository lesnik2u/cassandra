/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.partitions.TriePartitionUpdaterLegacyIndex;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.ContentManagerPojo;
import org.apache.cassandra.db.tries.ContentSerializer;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieDumperWithPath;
import org.apache.cassandra.db.tries.TrieEntriesWalker;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.db.tries.mvcc.CommitLogValueStore;
import org.apache.cassandra.db.tries.mvcc.FileValueStore;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableBufferAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;
import org.github.jamm.Unmetered;

/// Trie memtable implementation. Improves memory usage, garbage collection efficiency and lookup performance.
/// The implementation is described in detail in [TrieMemtable.md](./TrieMemtable.md).
///
/// The configuration takes the following parameters:
/// - shards: the number of shards to split into, defaulting to four times the number of CPU cores.
/// - value_storage: where values that do not fit a trie cell are kept, `native` (default), `file` or `commitlog`, see
///   [ValueStorage].
/// Also see [Memtable_API.md](./Memtable_API.md).
///
/// `value_storage: file` and `commitlog` are experimental. They keep the values that do not fit a trie cell outside
/// memtable memory: in a memory-mapped file per shard, or where the write put them in the commit log. The memtable
/// still charges those value bytes to its off-heap memory, so that the memtable memory limit covers them as if they
/// were in it. See [Values outside memtable memory](./TrieMemtable.md#values-outside-memtable-memory).
public class TrieMemtable extends AbstractShardedMemtable implements TrieMemtableMetricsView.ValueLogSource
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);

    /// Buffer type to use for memtable tries (on- vs off-heap)
    public static final BufferType BUFFER_TYPE = DatabaseDescriptor.getMemtableAllocationType().toBufferType();

    /// Force copy checker (see [org.apache.cassandra.db.tries.InMemoryTrie#apply]) ensuring all modifications apply
    /// atomically and consistently to the whole partition.
    public static final Predicate<InMemoryBaseTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION_BOUNDARY =
        features -> TrieBackedPartition.isPartitionBoundary(features.content());

    public static volatile boolean SHARD_LOCK_FAIRNESS = CassandraRelevantProperties.TRIE_MEMTABLE_SHARD_LOCK_FAIRNESS.getBoolean();

    public static final String TRIE_MEMTABLE_CONFIG_OBJECT_NAME = "org.apache.cassandra.db:type=TrieMemtableConfig";

    public static final String VALUE_STORAGE_OPTION = "value_storage";

    /// Distinguishes the value stores of successive memtables of the same table.
    private static final AtomicLong VALUE_LOG_SEQ = new AtomicLong();

    /// Whether this process writes through a commit log, so that `commitlog` value storage must check its
    /// configuration and record value locations. Offline tools do not: they never write through a memtable, and do not
    /// resolve the commit log disk access mode.
    private static boolean writesThroughCommitLog()
    {
        return DatabaseDescriptor.isDaemonInitialized() && DatabaseDescriptor.getCommitLogWriteDiskAccessMode() != null;
    }

    static
    {
        MBeanWrapper.instance.registerMBean(new TrieMemtableConfig(), TRIE_MEMTABLE_CONFIG_OBJECT_NAME, MBeanWrapper.OnException.LOG);
    }

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private final AtomicBoolean switchRequested = new AtomicBoolean(false);

    /// Sharded memtable sections. Each is responsible for a contiguous range of the token space (between `boundaries[i]`
    /// and `boundaries[i+1]`) and is written to by one thread at a time, while reads are carried out concurrently
    /// (including with any write).
    private final MemtableShard[] shards;

    /// A merged view of the memtable map. Used for partition range queries and flush.
    /// For efficiency, we serve single partition requests off the shard which offers more direct
    /// [org.apache.cassandra.db.tries.InMemoryTrie] methods.
    @VisibleForTesting
    final DeletionAwareTrie<Object, TrieTombstoneMarker> mergedTrie;

    @Unmetered
    private final TrieMemtableMetricsView metrics;

    @Unmetered
    private final TableMetadata actualMetadata;

    private final ValueStorage valueStorage;

    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner,
                 Integer shardCountOption, ValueStorage valueStorage)
    {
        this(commitLogLowerBound, metadataRef, owner, shardCountOption, valueStorage, null);
    }

    /// `cellDataManagers`, if not null, creates the cell data manager of each shard instead of
    /// [ValueStorage#newCellDataManager].
    @VisibleForTesting
    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner,
                 Integer shardCountOption, ValueStorage valueStorage, IntFunction<CellDataBufferManager> cellDataManagers)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        // factory() already rejects this; checked again in case the pool and the configuration disagree
        if (valueStorage != ValueStorage.NATIVE && !(MEMORY_POOL instanceof NativePool))
            throw new ConfigurationException(valueStorage.requiresOffheapObjectsMessage());
        this.valueStorage = valueStorage;
        this.metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
        this.actualMetadata = metadataRef.get();
        TableMetadata tableMetadata = actualMetadata;
        long memtableSeq = VALUE_LOG_SEQ.incrementAndGet();
        this.shards = generatePartitionShards(boundaries.shardCount(), actualMetadata, metrics, owner.readOrdering(),
                                              cellDataManagers != null
                                              ? cellDataManagers
                                              : shard -> valueStorage.newCellDataManager(tableMetadata, memtableSeq, shard));
        this.mergedTrie = makeMergedTrie(shards);
        if (valueStorage != ValueStorage.NATIVE)
            metrics.addValueLogSource(this);
        logger.trace("Created memtable with {} shards", this.shards.length);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           TableMetadata metadata,
                                                           TrieMemtableMetricsView metrics,
                                                           OpOrder opOrder,
                                                           IntFunction<CellDataBufferManager> cellDataManagers)
    {
        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        CellDataBufferManager[] managers = new CellDataBufferManager[splits];
        try
        {
            for (int i = 0; i < splits; i++)
            {
                managers[i] = cellDataManagers.apply(i);
                partitionMapContainer[i] = new MemtableShard(metadata, metrics, opOrder, managers[i]);
            }
        }
        catch (Throwable t)
        {
            // The memtable will not be published, so discard() will not run: release the stores created so far.
            for (CellDataBufferManager manager : managers)
                if (manager != null)
                    t = Throwables.perform(t, manager::close);
            throw Throwables.throwAsUncheckedException(t);
        }

        return partitionMapContainer;
    }

    private static DeletionAwareTrie<Object, TrieTombstoneMarker> makeMergedTrie(MemtableShard[] shards)
    {
        List<DeletionAwareTrie<Object, TrieTombstoneMarker>> tries = new ArrayList<>(shards.length);
        for (MemtableShard shard : shards)
            tries.add(shard.data);
        return DeletionAwareTrie.mergeDistinct(tries);
    }

    @Override
    public TableMetadata metadata()
    {
        return actualMetadata;
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        if (super.shouldSwitch(reason))
            return true;
        if (reason != ColumnFamilyStore.FlushReason.SCHEMA_CHANGE)
            return false;
        // If the columns definition changes, we need to flush as we would need to remap column indexes.
        return !actualMetadata.regularAndStaticColumns().equals(metadata.get().regularAndStaticColumns());
    }

    @Override
    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isClean())
                return false;
        return true;
    }

    @VisibleForTesting
    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);

        for (MemtableShard shard : shards)
            shard.allocator.setDiscarding();
    }

    @Override
    public void discard()
    {
        super.discard();
        if (valueStorage != ValueStorage.NATIVE)
            metrics.removeValueLogSource(this);
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (MemtableShard shard : shards)
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        // the buffer release is a longer-running process, do it in a separate loop to not make the metrics update wait
        Throwable fail = null;
        try
        {
            for (MemtableShard shard : shards)
            {
                shard.allocator.setDiscarded();
                shard.data.discardBuffers();
            }
        }
        catch (Throwable t)
        {
            fail = t;
        }
        // close the stores outside memtable memory last, so that a failure cannot prevent releasing memtable memory,
        // and also if releasing memtable memory failed
        for (MemtableShard shard : shards)
            fail = Throwables.perform(fail, shard.cellDataBufferManager::close);
        Throwables.maybeFail(fail);
    }

    @Override
    @VisibleForTesting
    public void overwriteAllData()
    {
        super.overwriteAllData();
        for (MemtableShard shard : shards)
        {
            shard.allocator.overwriteAllData();
            shard.data.overwriteAllBuffers();
        }
    }

    /// As [#put(PartitionUpdate, UpdateTransaction, OpOrder.Group, WriteContext)] without a write context.
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        return put(update, indexer, opGroup, null);
    }

    /// Should only be called by [ColumnFamilyStore#apply] via `Keyspace#apply`, which supplies the appropriate
    /// [OpOrder.Group].
    ///
    /// `commitLogSegmentPosition` should only be null if this is a secondary index, in which case it is *expected* to
    /// be null.
    ///
    /// The context's [CassandraWriteContext#valueLocations], if any, let `commitlog` value storage refer to the
    /// update's values in the commit log segment they were written to.
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, WriteContext context)
    {
        DecoratedKey key = update.partitionKey();
        MemtableShard shard = shards[boundaries.getShardForKey(key)];
        CommitLogValueLocations valueLocations = context instanceof CassandraWriteContext ? ((CassandraWriteContext) context).valueLocations() : null;
        long colUpdateTimeDelta = shard.put(update, indexer, opGroup, valueLocations);

        if (shard.data.reachedAllocatedSizeThreshold())
            signalFlushRequired(ColumnFamilyStore.FlushReason.TRIE_LIMIT, true);

        return colUpdateTimeDelta;
    }

    @Override
    public void signalFlushRequired(ColumnFamilyStore.FlushReason flushReason, boolean skipIfSignaled)
    {
        if (!switchRequested.getAndSet(true) || !skipIfSignaled)
        {
            logger.info("Scheduling flush for table {} due to {}", this.metadata.get(), flushReason);
            owner.signalFlushRequired(this, flushReason);
        }
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
        for (MemtableShard shard : shards)
        {
            stats.ownsOnHeap += shard.allocator.onHeap().owns();
            stats.ownsOffHeap += shard.allocator.offHeap().owns();
            stats.ownershipRatioOnHeap += shard.allocator.onHeap().ownershipRatio();
            stats.ownershipRatioOffHeap += shard.allocator.offHeap().ownershipRatio();
        }
    }

    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.liveDataSize();
        return total;
    }

    @VisibleForTesting
    ValueStorage valueStorage()
    {
        return valueStorage;
    }

    /// Value bytes written to the per-shard value stores (0 for `native`). Written, not live, bytes.
    @Override
    public long valueLogBytes()
    {
        long total = 0;
        for (MemtableShard shard : shards)
            total += shard.cellDataBufferManager.valueLogBytes();
        return total;
    }

    /// The part of [#valueLogBytes] that `commitlog` value storage copied because it could not refer to the values in
    /// the commit log (0 for `native` and `file`).
    @Override
    public long valueLogFallbackBytes()
    {
        long total = 0;
        for (MemtableShard shard : shards)
            total += shard.cellDataBufferManager.fallbackBytes();
        return total;
    }

    @Override
    public long operationCount()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (MemtableShard shard : shards)
            total += shard.partitionCount();
        return total;
    }

    public int getShardCount()
    {
        return shards.length;
    }

    /// Returns the minimum timestamp if one available, otherwise `NO_MIN_TIMESTAMP`.
    /// [EncodingStats] uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return
    /// `NO_MIN_TIMESTAMP` instead.
    ///
    /// @return The minTS or `NO_MIN_TIMESTAMP` if none available
    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  EncodingStats.mergeMinTimestamp(min, shard.stats);
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  EncodingStats.mergeMinLocalDeletionTime(min, shard.stats);
        return min;
    }

    @Override
    public DecoratedKey minPartitionKey()
    {
        for (int i = 0; i < shards.length; i++)
        {
            MemtableShard shard = shards[i];
            if (!shard.isClean())
                return shard.minPartitionKey();
        }
        return null;
    }

    @Override
    public DecoratedKey maxPartitionKey()
    {
        for (int i = shards.length - 1; i >= 0; i--)
        {
            MemtableShard shard = shards[i];
            if (!shard.isClean())
                return shard.maxPartitionKey();
        }
        return null;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (MemtableShard shard : shards)
            columnsCollector.update(shard.columns);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (MemtableShard shard : shards)
            statsCollector.update(shard.stats);
        return statsCollector.get();
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                         final DataRange dataRange,
                                                         SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        DeletionAwareTrie<Object, TrieTombstoneMarker> subMap =
            mergedTrie.subtrie(toComparableBound(keyRange.left, includeStart),
                               toComparableBound(keyRange.right, !includeStop));

        return new MemtableUnfilteredPartitionIterator(actualMetadata,
                                                       metadata.get(),
                                                       true, // even if our data is on-heap, we can recycle
                                                             // cells as soon as the opOrder is closed
                                                       subMap,
                                                       columnFilter,
                                                       dataRange,
                                                       getMinLocalDeletionTime());
        // Note: the minLocalDeletionTime reported by the iterator is the memtable's minLocalDeletionTime. This is okay
        // because we only need to report a lower bound that will eventually advance, and calculating a more precise
        // bound would be an unnecessary expense.
    }

    private static ByteComparable toComparableBound(PartitionPosition position, boolean before)
    {
        return position == null || position.isMinimum() ? null : position.asComparableBound(before);
    }

    public Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        DeletionAwareTrie<Object, TrieTombstoneMarker> trie = shards[shardIndex].data.tailTrie(key);
        // Always copy to heap. Even if our data is on-heap, we can recycle cells as soon as the opOrder is closed.
        return createPartition(metadata(), metadata.get(), true, key, trie);
    }

    private static TrieBackedPartition createPartition(TableMetadata metadata, TableMetadata droppedColumnsSource, boolean copyToHeap, DecoratedKey key, DeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        if (trie == null)
            return null;
        PartitionData holder = (PartitionData) trie.get(ByteComparable.EMPTY);
        // If we found a matching path in the trie, it must be the root of this partition (because partition keys are
        // prefix-free, it can't be a prefix for a different path, or have another partition key as prefix) and contain
        // PartitionData (because the attachment of a new or modified partition to the trie is atomic).
        assert holder != null : "Entry for " + key + " without associated PartitionData";

        return TrieBackedPartition.create(key,
                                          holder.columns(),
                                          holder.stats(),
                                          holder.rowCountIncludingStatic(),
                                          holder.tombstoneCount(),
                                          trie,
                                          metadata,
                                          droppedColumnsSource,
                                          copyToHeap);
    }


    @Override
    public Cell<?> getCellForKey(DecoratedKey partitionKey, Clustering<?> clustering, ColumnMetadata column)
    {
        TableMetadata metadata = metadata();
        ByteComparable key = v -> ByteSource.concat(
            partitionKey.asComparableBytes(v),
            metadata.comparator.asByteComparable(clustering).asComparableBytes(v),
            TrieBackedRow.columnKey(clustering == Clustering.STATIC_CLUSTERING ? metadata.staticColumns()
                                                                               : metadata.regularColumns(),
                                    column));

        Object cell = mergedTrie.get(key);
        if (!(cell instanceof CellData))
            return null;
        return ((CellData<?, ?>) cell).toCell(column, null)
                                      .clone(HeapCloner.instance); // copy to heap to guard against modifications to the
                                                                   // buffer after the opOrder is closed
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    private static DecoratedKey getPartitionKeyFromPath(TableMetadata metadata, ByteComparable path)
    {
        return BufferDecoratedKey.fromByteComparable(path,
                                                     TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                     metadata.partitioner);
    }

    /// Make a textual representation of the trie, linking content with its type and key. See
    /// [TrieMemtable.md](TrieMemtable.md) for an example of the output.
    public String dump()
    {
        return mergedTrie.process(Direction.FORWARD, new Dumper(metadata()));
    }

    static final int PARTITIONDATA_OFFSET_ROW_COUNT = 0;
    static final int PARTITIONDATA_OFFSET_TOMBSTONE_COUNT = 4;

    /// Metadata object signifying the root node of a partition. Stores row and tombstone counts in a trie cell,
    /// as well as a link to the owning subrange, which is used for compiling encoding statistics and column sets.
    ///
    /// Descends from [TrieBackedPartition.PartitionMarker] to permit tail tries to be passed directly to
    /// [TrieBackedPartition].
    public static class PartitionData implements TrieBackedPartition.PartitionMarker
    {
        public final MemtableShard owner;

        UnsafeBuffer buffer;
        int inBufferPos;

        public PartitionData(MemtableShard owner)
        {
            this(owner, null, 0);
        }

        public PartitionData(MemtableShard owner, UnsafeBuffer buffer, int inBufferPos)
        {
            this.owner = owner;
            this.buffer = buffer;
            this.inBufferPos = inBufferPos;
        }

        public RegularAndStaticColumns columns()
        {
            return owner.columns;
        }

        public EncodingStats stats()
        {
            return owner.stats;
        }

        public int rowCountIncludingStatic()
        {
            return buffer.getInt(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT);
        }

        public int tombstoneCount()
        {
            return buffer.getInt(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT);
        }

        public void markInsertedRows(int howMany)
        {
            buffer.addIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT, howMany);
        }

        public void markAddedTombstones(int howMany)
        {
            buffer.addIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT, howMany);
        }

        @Override
        public String toString()
        {
            return String.format("partition with %d rows and %d tombstones", rowCountIncludingStatic(), tombstoneCount());
        }

        public long unsharedHeapSize()
        {
            return 0;
        }
    }

    class KeySizeAndCountCollector extends TrieEntriesWalker<Object, Void>
    {
        long keySize = 0;
        int keyCount = 0;

        @Override
        public Void complete()
        {
            return null;
        }

        @Override
        protected void content(Object content, byte[] bytes, int byteLength)
        {
            // This is used with processSkippingBranches which should ensure that we only see the partition roots.
            assert content instanceof PartitionData;
            ++keyCount;
            byte[] keyBytes = DecoratedKey.keyFromByteSource(ByteSource.preencoded(bytes, 0, byteLength),
                                                             TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                             metadata().partitioner);
            keySize += keyBytes.length;
        }
    }

    @Override
    public FlushablePartitionSet<TrieBackedPartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> toFlush = mergedTrie.subtrie(toComparableBound(from, true), toComparableBound(to, true));

        var counter = new KeySizeAndCountCollector(); // need to jump over tails keys
        toFlush.processSkippingBranches(Direction.FORWARD, counter);
        int partitionCount = counter.keyCount;
        long partitionKeySize = counter.keySize;

        return new AbstractFlushablePartitionSet<>()
        {
            public Memtable memtable()
            {
                return TrieMemtable.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<TrieBackedPartition> iterator()
            {
                return new PartitionIterator(toFlush, actualMetadata, metadata.get(), false);
            }

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }
        };
    }

    public static class MemtableShard
    {
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        private volatile long minTimestamp = Long.MAX_VALUE;

        private volatile long liveDataSize = 0;

        private volatile long currentOperations = 0;

        private volatile int partitionCount = 0;

        @Unmetered
        private final ReentrantLock writeLock = new ReentrantLock(SHARD_LOCK_FAIRNESS);

        /// Content map for the given shard. This is implemented as an in-memory trie which uses the prefix-free
        /// byte-comparable [ByteSource] representations of keys to address partitions and individual rows within
        /// partitions.
        ///
        /// This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
        /// several threads may read from it and iterate over it. Iterators (especially partition range iterators)
        /// may operate for a long period of time and thus iterators should not throw `ConcurrentModificationException`s
        /// if the underlying map is modified during iteration, they should provide a weakly consistent view of the map
        /// instead.
        ///
        /// Also, this data is backed by memtable memory. When accessing it callers must specify if it can be accessed
        /// unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        /// should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;

        volatile RegularAndStaticColumns columns;

        volatile EncodingStats stats;

        @Unmetered  // total pool size should not be included in memtable's deep size
        private final MemtableAllocator allocator;

        private final CellDataBufferManager cellDataBufferManager;

        @Unmetered
        private final TrieMemtableMetricsView metrics;

        private final TableMetadata metadata;

        private TriePartitionUpdater noIndexUpdater;
        private TriePartitionUpdaterLegacyIndex legacyIndexUpdater;

        MemtableShard(TableMetadata metadata, TrieMemtableMetricsView metrics, OpOrder opOrder, CellDataBufferManager cellDataManager)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(metadata.toString()), metrics, opOrder, cellDataManager);
        }

        @VisibleForTesting
        MemtableShard(TableMetadata metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, OpOrder opOrder)
        {
            this(metadata, allocator, metrics, opOrder, null);
        }

        /// `cellDataManager` null selects the manager matching the allocator (values in memtable memory).
        MemtableShard(TableMetadata metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, OpOrder opOrder,
                      CellDataBufferManager cellDataManager)
        {
            this.metadata = metadata;
            this.allocator = allocator;
            if (cellDataManager != null)
                this.cellDataBufferManager = cellDataManager;
            else if (this.allocator instanceof NativeAllocator)
                this.cellDataBufferManager = new NativeBufferManager((NativeAllocator) allocator);
            else
                this.cellDataBufferManager = new SlabBufferManager((MemtableBufferAllocator) allocator,
                                                                   opOrder,
                                                                   BUFFER_TYPE.onHeapSizeWithoutData());

            this.data = InMemoryDeletionAwareTrie.longLived(TrieBackedPartition.BYTE_COMPARABLE_VERSION, BUFFER_TYPE, opOrder,
                                                            new TrieSerializer(cellDataBufferManager, this));
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.metrics = metrics;
        }

        private TriePartitionUpdater getAndConfigureUpdater(UpdateTransaction indexer)
        {
            if (indexer == UpdateTransaction.NO_OP)
            {
                if (noIndexUpdater == null)
                    noIndexUpdater = new TriePartitionUpdater(this, data);
                return noIndexUpdater;
            }
            else
            {
                if (legacyIndexUpdater == null)
                    legacyIndexUpdater = new TriePartitionUpdaterLegacyIndex(this, data, metadata);
                legacyIndexUpdater.setIndexContext(indexer);
                return legacyIndexUpdater;
            }
        }

        /// `valueLocations` is where the update's values were written in the commit log, or null.
        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, CommitLogValueLocations valueLocations)
        {
            // If the update is not TriePartitionUpdate, convert it to one before taking the lock.
            DeletionAwareTrie<Object, TrieTombstoneMarker> mergableTrie = TriePartitionUpdate.asMergableTrie(update);

            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = Clock.Global.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(Clock.Global.nanoTime() - lockStartTime);
            }

            TriePartitionUpdater updater = getAndConfigureUpdater(indexer);
            try
            {
                try
                {
                    this.cellDataBufferManager.opOrderGroup = opGroup;
                    this.cellDataBufferManager.valueLocations = valueLocations;
                    int partitionsAdded = mergeUpdate(data,
                                                      allocator,
                                                      mergableTrie,
                                                      indexer,
                                                      opGroup,
                                                      updater);
                    partitionCount += partitionsAdded;
                }
                finally
                {
                    // do not keep the update's cached serialization reachable from the shard
                    this.cellDataBufferManager.valueLocations = null;
                    indexer.commit();
                    updateMinTimestamp(update.stats().minTimestamp);
                    updateLiveDataSize(updater.dataSize);
                    updateCurrentOperations(update.operationCount());

                    columns = columns.mergeTo(update.columns());
                    stats = stats.mergeWith(update.stats());
                }
            }
            finally
            {
                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        public boolean isClean()
        {
            return data.isEmpty();
        }

        private void updateMinTimestamp(long timestamp)
        {
            if (timestamp < minTimestamp)
                minTimestamp = timestamp;
        }

        void updateLiveDataSize(long size)
        {
            liveDataSize += size;
        }

        private void updateCurrentOperations(long op)
        {
            currentOperations += op;
        }

        public int partitionCount()
        {
            return partitionCount;
        }

        long liveDataSize()
        {
            return liveDataSize;
        }

        long currentOperations()
        {
            return currentOperations;
        }

        private DecoratedKey firstPartitionKey(Direction direction)
        {
            // Note: there is no need to skip tails here as this will only be run until we find the first partition.
            Iterator<Map.Entry<ByteComparable.Preencoded, PartitionData>> iter = data.filteredEntryIterator(direction, PartitionData.class);
            if (!iter.hasNext())
                return null;

            Map.Entry<ByteComparable.Preencoded, PartitionData> entry = iter.next();
            return getPartitionKeyFromPath(metadata, entry.getKey());
        }

        public DecoratedKey minPartitionKey()
        {
            return firstPartitionKey(Direction.FORWARD);
        }

        public DecoratedKey maxPartitionKey()
        {
            return firstPartitionKey(Direction.REVERSE);
        }
    }

    /// Merge an update into the given data trie using the given helpers. Extracted to separate method for testing.
    static int mergeUpdate(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> dataTrie,
                           MemtableAllocator allocator,
                           DeletionAwareTrie<Object, TrieTombstoneMarker> updateTrie,
                           UpdateTransaction indexer,
                           OpOrder.Group opGroup,
                           TriePartitionUpdater updater)
    {
        indexer.start();
        // Add the initial trie size on the first operation. This technically isn't correct (other shards
        // do take their memory share even if they are empty) but doing it during construction may cause
        // the allocator to block while we are trying to flush a memtable and become a deadlock.
        // A first put that aborts after its values were appended to a store outside memtable memory leaves the trie
        // empty, so the next put charges those bytes again: this only over-counts, and the discard releases it.
        long onHeap = dataTrie.isEmpty() ? 0 : dataTrie.usedSizeOnHeap();
        long offHeap = dataTrie.isEmpty() ? 0 : dataTrie.usedSizeOffHeap();
        try
        {
            updater.mergeUpdate(updateTrie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            // This should never really happen as a flush would be triggered long before this limit is reached.
            throw new AssertionError(e);
        }
        finally
        {
            allocator.offHeap().adjust(dataTrie.usedSizeOffHeap() - offHeap, opGroup);
            allocator.onHeap().adjust((dataTrie.usedSizeOnHeap() - onHeap), opGroup);
        }
        return updater.partitionsAdded;
    }

    /// Iterator over partitions of the given trie. Looks for partition markers and presents the branch of each
    /// partition marker as a [TrieBackedPartition].
    static class PartitionIterator extends TrieTailsIterator.DeletionAwareWithoutCoveringDeletions<Object, TrieTombstoneMarker, TrieBackedPartition>
    {
        final TableMetadata metadata;
        final TableMetadata droppedColumnsSource;
        final boolean copyToHeap;

        PartitionIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> source, TableMetadata metadata, TableMetadata droppedColumnsSource, boolean copyToHeap)
        {
            super(source, Direction.FORWARD, TrieBackedPartition.IS_PARTITION_BOUNDARY);
            this.metadata = metadata;
            this.droppedColumnsSource = droppedColumnsSource;
            this.copyToHeap = copyToHeap;
        }

        @Override
        protected TrieBackedPartition mapContent(Object content, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            PartitionData pd = (PartitionData) content;
            DecoratedKey key = getPartitionKeyFromPath(metadata,
                                                       ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                 bytes, 0, byteLength));
            return TrieBackedPartition.create(key,
                                              pd.columns(),
                                              pd.stats(),
                                              pd.rowCountIncludingStatic(),
                                              pd.tombstoneCount(),
                                              tailTrie,
                                              metadata,
                                              droppedColumnsSource,
                                              copyToHeap);
        }
    }

    /// The implementation of [UnfilteredPartitionIterator] used to walk partition ranges.
    static class MemtableUnfilteredPartitionIterator
    extends AbstractUnfilteredPartitionIterator
    implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<TrieBackedPartition> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;
        private final long minLocalDeletionTime;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   TableMetadata droppedColumnsSource,
                                                   boolean copyToHeap,
                                                   DeletionAwareTrie<Object, TrieTombstoneMarker> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange,
                                                   long minLocalDeletionTime)
        {
            this.iter = new PartitionIterator(source, metadata, droppedColumnsSource, copyToHeap);
            this.metadata = metadata;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
            this.minLocalDeletionTime = minLocalDeletionTime;
        }

        public long getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Partition partition = iter.next();
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    public static Factory factory(Map<String, String> optionsCopy)
    {
        String shardsString = optionsCopy.remove(SHARDS_OPTION);
        Integer shardCount = shardsString != null ? Integer.parseInt(shardsString) : null;

        ValueStorage valueStorage = ValueStorage.fromOption(optionsCopy.remove(VALUE_STORAGE_OPTION));
        valueStorage.validate();
        valueStorage.onAccepted();
        return new TrieMemtableFactory(shardCount, valueStorage);
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        long size = 0;
        for (MemtableShard shard : shards)
        {
            size += shard.data.unusedReservedOnHeapMemory();
            size += shard.allocator.unusedReservedOnHeapMemory();
        }
        size += this.allocator.unusedReservedOnHeapMemory();
        return size;
    }

    /// Release all recycled content references, including the ones waiting in still incomplete recycling lists.
    /// This is a test method and can cause null pointer exceptions if used on a live trie.
    @VisibleForTesting
    void releaseReferencesUnsafe()
    {
        for (MemtableShard shard : shards)
            shard.data.releaseReferencesUnsafe();
    }

    public static class TrieMemtableConfig implements TrieMemtableConfigMXBean
    {
        @Override
        public void setDefaultShardCount(String shardCount)
        {
            AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG.setDefaultShardCount(shardCount);
        }

        @Override
        public String getDefaultShardCount()
        {
            return AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG.getDefaultShardCount();
        }

        @Override
        public void setShardCount(String shardCount)
        {
            setDefaultShardCount(shardCount);
        }

        @Override
        public String getShardCount()
        {
            return getDefaultShardCount();
        }

        @Override
        public void setLockFairness(String fairness)
        {
            SHARD_LOCK_FAIRNESS = Boolean.parseBoolean(fairness);
            CassandraRelevantProperties.TRIE_MEMTABLE_SHARD_LOCK_FAIRNESS.setBoolean(SHARD_LOCK_FAIRNESS);
            logger.info("Requested setting shard lock fairness to {}; set to: {}", fairness, SHARD_LOCK_FAIRNESS);
        }

        @Override
        public String getLockFairness()
        {
            return "" + SHARD_LOCK_FAIRNESS;
        }
    }

    /// Trie serializer, used for mapping trie data cells to and from the various database objects.
    static class TrieSerializer implements ContentSerializer<Object>
    {
        final CellDataBufferManager manager;
        final MemtableShard owner;

        // Singletons mapped to special trie values (i.e. negative trie pointers)

        /// [TrieBackedRow#COMPLEX_COLUMN_MARKER]
        static final int COMPLEX_COLUMN_ID = 0;
        /// [LivenessInfo#EMPTY]
        static final int EMPTY_LIVENESS_ID = 1;
        /// [TrieTombstoneMarker.LevelMarker#ROW] on the left.
        static final int TOMBSTONE_ROW_MARKER_BEFORE_BRANCH = 2;
        /// [TrieTombstoneMarker.LevelMarker#ROW] on the right.
        static final int TOMBSTONE_ROW_MARKER_AFTER_BRANCH = 3;

        // Offset values

        // Offsets 0 to 24 are cells where the length is given in the offset. Lengths above 16 mean cell has no TTL.

        /// Counter cell whose length is stored in byte 15.
        static final byte TYPE_CELL_COUNTER = 0x19;

        /// Cell whose value does not fit and is stored externally.
        static final byte TYPE_CELL_EXTERNAL_VALUE = 0x1A;

        /// Content is [LivenessInfo]
        static final byte TYPE_LIVENESS_INFO = 0x1B;
        /// Content is [TrieTombstoneMarker], to be presented _before_ branch
        static final byte TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH = 0x1C;
        /// Content is [TrieTombstoneMarker], to be presented _after_ branch
        static final byte TYPE_TOMBSTONE_MARKER_AFTER_BRANCH = 0x1D;
        /// Content is [PartitionData]
        static final byte TYPE_PARTITION_DATA = 0x1E;
        // 0x1F for OFFSET_SPECIAL

        // Tombstone flags

        // The three values below match the ones in [TrieCellData], but they don't necessarily have to
        // (the equivalence is only used in [#dumpContent]).
        static final int OFFSET_TIMESTAMP = 0x18;
        static final int OFFSET_LOCAL_DELETION_TIME = 0x14;
        static final int OFFSET_TTL = 0x10;
        static final int OFFSET_TOMBSTONE_KIND = 0x13;

        /// Offset to add to the above for the left side of a tombstone marker
        static final int OFFSET_TOMBSTONE_LEFT = -0x00;
        /// Offset to add to the above for the right side of a tombstone marker
        static final int OFFSET_TOMBSTONE_RIGHT = -0x10;

        /// Byte that stores whether the tombstone marker is also a row marker
        static final int OFFSET_TOMBSTONE_IS_ROW_MARKER = 0x00;

        @VisibleForTesting
        public TrieSerializer(CellDataBufferManager manager, MemtableShard owner)
        {
            this.manager = manager;
            this.owner = owner;
        }

        @Override
        public int idIfSpecial(Object content, boolean shouldPresentAfterBranch)
        {
            if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            {
                assert !shouldPresentAfterBranch;
                return COMPLEX_COLUMN_ID;
            }
            if (content == LivenessInfo.EMPTY || content instanceof LivenessInfo && LivenessInfo.EMPTY.equals(content))
            {
                assert !shouldPresentAfterBranch;
                return EMPTY_LIVENESS_ID;
            }
            if (content == TrieTombstoneMarker.LevelMarker.ROW)
                return shouldPresentAfterBranch ? TOMBSTONE_ROW_MARKER_AFTER_BRANCH : TOMBSTONE_ROW_MARKER_BEFORE_BRANCH;

            // Everything else takes a trie cell.
            return -1;
        }

        @Override
        public Object special(int id)
        {
            switch (id)
            {
                case COMPLEX_COLUMN_ID:
                    return TrieBackedRow.COMPLEX_COLUMN_MARKER;
                case EMPTY_LIVENESS_ID:
                    return LivenessInfo.EMPTY;
                case TOMBSTONE_ROW_MARKER_BEFORE_BRANCH:
                case TOMBSTONE_ROW_MARKER_AFTER_BRANCH:
                    return TrieTombstoneMarker.LevelMarker.ROW;
                default:
                    throw new AssertionError("Unknown special ID " + id);
            }
        }

        @Override
        public boolean shouldPresentSpecialAfterBranch(int id)
        {
            return id == TOMBSTONE_ROW_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean shouldPreserveSpecialWithoutChildren(int id)
        {
            // All our specials are level markers that should not survive if the branch becomes empty.
            return false;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(int offset)
        {
            // Row markers that fall under a deletion turn into x->x markers with level id. If there is no
            // substructure (e.g. a complex column deletion), these should disappear.
            return offset != TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH && offset != TYPE_TOMBSTONE_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            if (buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER) == 0)
                return true;
            if (buffer.getLong(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_TIMESTAMP) !=
                buffer.getLong(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_TIMESTAMP))
                return true;
            if (buffer.getInt(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_LOCAL_DELETION_TIME) !=
                buffer.getInt(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_LOCAL_DELETION_TIME))
                return true;
            if (buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_TOMBSTONE_KIND) !=
                buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_TOMBSTONE_KIND))
                return true;
            return false;
        }

        @Override
        public int serialize(Object content, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int inBufferPos)
        throws TrieSpaceExhaustedException
        {
            assert !shouldPresentAfterBranch || content instanceof TrieTombstoneMarker;
            // most common first
            if (content instanceof CellData)
                return TrieCellData.serialize((CellData<?, ?>) content, buffer, inBufferPos, manager);
            else if (content instanceof LivenessInfo)
                return serializeLivenessInfo((LivenessInfo) content, buffer, inBufferPos);
            else if (content instanceof TrieTombstoneMarker)
                return serializeTombstoneMarker((TrieTombstoneMarker) content, shouldPresentAfterBranch, buffer, inBufferPos);
            else if (content instanceof PartitionData)
                return serializePartitionData((PartitionData) content, buffer, inBufferPos);
            else
                throw new AssertionError("Unknown trie content type: " + content);
        }

        private int serializeLivenessInfo(LivenessInfo livenessInfo, UnsafeBuffer buffer, int inBufferPos)
        {
            buffer.putLongOrdered(inBufferPos + OFFSET_TIMESTAMP, livenessInfo.timestamp());
            buffer.putIntOrdered(inBufferPos + OFFSET_LOCAL_DELETION_TIME, CellData.deletionTimeLongToUnsignedInteger(livenessInfo.localExpirationTime()));
            buffer.putIntOrdered(inBufferPos + OFFSET_TTL, livenessInfo.ttl());
            return TYPE_LIVENESS_INFO;
        }

        private int serializeTombstoneMarker(TrieTombstoneMarker marker, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int inBufferPos)
        {
            assert marker.isBoundary();
            TrieTombstoneMarker.Covering left = marker.leftDeletion();
            TrieTombstoneMarker.Covering right = marker.rightDeletion();
            serializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_LEFT, left);
            serializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_RIGHT, right);
            buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER, (byte) (marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW) ? 1 : 0));
            return shouldPresentAfterBranch ? TYPE_TOMBSTONE_MARKER_AFTER_BRANCH : TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH;
        }

        private void serializeTombstoneSide(UnsafeBuffer buffer, int inBufferPos, TrieTombstoneMarker.Covering markerSide)
        {
            if (markerSide != null)
            {
                buffer.putLongOrdered(inBufferPos + OFFSET_TIMESTAMP, markerSide.markedForDeleteAt());
                buffer.putIntOrdered(inBufferPos + OFFSET_LOCAL_DELETION_TIME, CellData.deletionTimeLongToUnsignedInteger(markerSide.localDeletionTime()));
                buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_KIND, (byte) markerSide.deletionKind().ordinal());
            }
            else
            {
                buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_KIND, (byte) -1);
            }
        }

        private int serializePartitionData(PartitionData partitionData, UnsafeBuffer buffer, int inBufferPos)
        {
            if (partitionData.buffer == null)
            {
                // We are creating a new partition. Link this buffer/inBufferPos with the argument, so that we can add
                // statistics as we descend into the partition.
                partitionData.buffer = buffer;
                partitionData.inBufferPos = inBufferPos;
                // we don't need to set anything else as the buffer is filled with 0s when allocated
            }
            else
            {
                // We are making a copy of another PartitionData object.
                buffer.putIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT, partitionData.rowCountIncludingStatic());
                buffer.putIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT, partitionData.tombstoneCount());
            }
            return TYPE_PARTITION_DATA;
        }

        @Override
        public int updateInPlace(UnsafeBuffer buffer, int inBufferPos, int offsetBits, Object newContent) throws TrieSpaceExhaustedException
        {
            // We can always set in place, but we may need to release previously held buffer.
            if (releaseNeeded(offsetBits))
                release(buffer, inBufferPos, offsetBits);

            return serialize(newContent, shouldPresentAfterBranch(offsetBits), buffer, inBufferPos);
        }

        @Override
        public void releaseSpecial(int id)
        {
            // nothing to do, our specials are fixed
        }

        @Override
        public Object deserialize(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            switch (offsetBits)
            {
                case TYPE_CELL_COUNTER:
                    return new TrieCellData.Counter(buffer, inBufferPos);
                case TYPE_CELL_EXTERNAL_VALUE:
                    return new TrieCellData.External(buffer, inBufferPos, manager);
                case TYPE_LIVENESS_INFO:
                    return deserializeLivenessInfo(buffer, inBufferPos);
                case TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH:
                case TYPE_TOMBSTONE_MARKER_AFTER_BRANCH:
                    return deserializeTombstoneMarker(buffer, inBufferPos);
                case TYPE_PARTITION_DATA:
                    return new PartitionData(owner, buffer, inBufferPos);
                default:
                    return TrieCellData.embedded(buffer, inBufferPos, offsetBits);
            }
        }

        private LivenessInfo deserializeLivenessInfo(UnsafeBuffer buffer, int inBufferPos)
        {
            long timestamp = buffer.getLong(inBufferPos + OFFSET_TIMESTAMP);
            long localExpirationTime = CellData.deletionTimeUnsignedIntegerToLong(buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME));
            int ttl = buffer.getInt(inBufferPos + OFFSET_TTL);
            return LivenessInfo.withExpirationTime(timestamp, ttl, localExpirationTime);
        }

        private TrieTombstoneMarker deserializeTombstoneMarker(UnsafeBuffer buffer, int inBufferPos)
        {
            TrieTombstoneMarker.Covering left = deserializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_LEFT);
            TrieTombstoneMarker.Covering right = deserializeTombstoneSide (buffer, inBufferPos + OFFSET_TOMBSTONE_RIGHT);
            TrieTombstoneMarker.LevelMarker levelMarker = buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER) != 0
                                                                   ? TrieTombstoneMarker.LevelMarker.ROW
                                                                   : null;
            return TrieTombstoneMarker.make(left, right, levelMarker);
        }

        private TrieTombstoneMarker.Covering deserializeTombstoneSide(UnsafeBuffer buffer, int inBufferPos)
        {
            byte kind = buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_KIND);
            if (kind < 0)
                return null;
            return TrieTombstoneMarker.covering(buffer.getLong(inBufferPos + OFFSET_TIMESTAMP),
                                                CellData.deletionTimeUnsignedIntegerToLong(buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME)),
                                                TrieTombstoneMarker.Kind.values()[kind]);
        }

        @Override
        public boolean shouldPresentAfterBranch(int offsetBits)
        {
            // only markers can be after branch
            return offsetBits == TYPE_TOMBSTONE_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean releaseNeeded(int offsetBits)
        {
            return manager.releaseNeeded() && offsetBits == TYPE_CELL_EXTERNAL_VALUE;
        }

        @Override
        public void release(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            TrieCellData.External.release(buffer, inBufferPos, manager);
        }

        @Override
        public void completeMutation()
        {
            manager.completeMutation();
        }

        @Override
        public void abortMutation()
        {
            manager.abortMutation();
        }

        @Override
        public long usedSizeOnHeap()
        {
            return manager.onHeapSize();
        }

        /// The values a store outside memtable memory holds are counted as off-heap memory, so that the per-update
        /// adjustment in [TrieMemtable#mergeUpdate] charges them to the shard's allocator. Values kept in memtable
        /// memory are charged by the allocator that holds them, and the native managers report 0.
        @Override
        public long usedSizeOffHeap()
        {
            return manager.valueLogBytes();
        }

        @Override
        public long unusedReservedOnHeapMemory()
        {
            return manager.unusedReservedOnHeapMemory();
        }

        @Override
        public void releaseReferencesUnsafe()
        {
            manager.releaseReferencesUnsafe();
        }

        @Override
        public String dumpSpecial(int id)
        {
            return "Payload: " + special(id).toString();
        }

        @Override
        public String dumpContent(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            // This method dumps the content of the cell using their most common interpretation. While the labels may be
            // incorrect for some types, the data is still presented.
            return String.format("Payload: length/type %02x data %s ttl %08x ldt %08x timestamp %016x",
                                 offsetBits,
                                 ByteBufferUtil.bytesToHex(buffer.byteBuffer()
                                                                 .duplicate()
                                                                 .position(inBufferPos + 0)
                                                                 .limit(inBufferPos + 16)),
                                 buffer.getInt(inBufferPos + OFFSET_TTL),
                                 buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME),
                                 buffer.getLong(inBufferPos + OFFSET_TIMESTAMP)
            );
        }
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    @VisibleForTesting
    public static abstract class CellDataBufferManager implements TrieCellData.ExternalBufferHandler
    {
        OpOrder.Group opOrderGroup;

        /// Where the values of the put in progress were written in the commit log, or null. Set by the shard's writer
        /// under the shard's write lock before the values of an update are stored, and cleared after them, also when
        /// the put fails.
        CommitLogValueLocations valueLocations;

        /// On-heap size of any additional structures used to store the references to data
        abstract long onHeapSize();

        /// If true, the release method will be called when a value is no longer in use
        abstract boolean releaseNeeded();

        /// See [org.apache.cassandra.db.tries.MemoryManager#completeMutation]
        abstract void completeMutation();
        /// See [org.apache.cassandra.db.tries.MemoryManager#abortMutation]
        abstract void abortMutation();

        /// See [org.apache.cassandra.db.tries.MemoryManager#unusedReservedOnHeapMemory]
        @VisibleForTesting
        abstract long unusedReservedOnHeapMemory();

        /// See [org.apache.cassandra.db.tries.ContentManager#releaseReferencesUnsafe]
        @VisibleForTesting
        abstract void releaseReferencesUnsafe();

        /// Value bytes this manager has written outside memtable memory: a non-decreasing counter, charged to the
        /// shard's off-heap memory (see [TrieSerializer#usedSizeOffHeap]) and read without synchronisation by the
        /// metrics gauge, which may still read it after [#close].
        long valueLogBytes()
        {
            return 0;
        }

        /// The part of [#valueLogBytes] that was copied because it could not be referenced where the write put it.
        /// Same threading as [#valueLogBytes].
        long fallbackBytes()
        {
            return 0;
        }

        /// Release any resources outside the memtable allocator. Called from [TrieMemtable#discard] after the memtable
        /// memory is released. Must be idempotent and should not throw.
        void close()
        {
        }
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    ///
    /// This option stores data in ByteBuffers allocated by the given [MemtableBufferAllocator] and keeps a list of the
    /// ByteBuffers it returned in a long-lived [ContentManagerPojo].
    /// It has on-heap presence that is proportional to the number of large data values.
    static class SlabBufferManager extends CellDataBufferManager
    {
        final MemtableBufferAllocator allocator;
        final long bufferSizeOnHeap;
        final ContentManagerPojo<ByteBuffer> buffers;

        @VisibleForTesting
        public SlabBufferManager(MemtableBufferAllocator allocator, OpOrder opOrder, long bufferSizeOnHeap)
        {
            this.allocator = allocator;
            this.bufferSizeOnHeap = bufferSizeOnHeap;
            this.buffers = new ContentManagerPojo<>(Predicates.alwaysTrue(), InMemoryBaseTrie.ExpectedLifetime.LONG,
                                                    opOrder);
        }

        @Override
        public long store(ByteBuffer buffer, int length) throws TrieSpaceExhaustedException
        {
            ByteBuffer cloned = allocator.allocate(length, opOrderGroup);
            FastByteOperations.copy(buffer, buffer.position(), cloned, cloned.position(), length);
            return buffers.addContent(cloned, false);
        }

        @Override
        public ByteBuffer load(long handle, int length)
        {
            return buffers.getContent((int) handle);
        }

        @Override
        long onHeapSize()
        {
            return buffers.usedSizeOnHeap() + buffers.valuesCount() * bufferSizeOnHeap;
        }

        @Override
        public boolean releaseNeeded()
        {
            return true;
        }

        @Override
        public void release(long handle, int length)
        {
            buffers.releaseContent((int) handle);
        }

        @Override
        public void completeMutation()
        {
            buffers.completeMutation();
        }

        @Override
        public void abortMutation()
        {
            buffers.abortMutation();
        }

        @Override
        @VisibleForTesting
        long unusedReservedOnHeapMemory()
        {
            return buffers.unusedReservedOnHeapMemory();
        }

        @Override
        @VisibleForTesting
        void releaseReferencesUnsafe()
        {
            buffers.releaseReferencesUnsafe();
        }
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    ///
    /// This option stores data in native memory using [NativeAllocator] and returns the memory address as handle.
    /// This storage method has no on-heap presence and is as efficient as it gets. Used when the memtable allocation
    /// type is `offheap_objects`.
    @VisibleForTesting
    public static class NativeBufferManager extends CellDataBufferManager
    {
        final NativeAllocator allocator;

        @VisibleForTesting
        public NativeBufferManager(NativeAllocator allocator)
        {
            this.allocator = allocator;
        }

        @Override
        public long store(ByteBuffer buffer, int length)
        {
            long address = allocator.allocate(length, opOrderGroup);
            MemoryUtil.setBytes(address, buffer);
            return address;
        }

        @Override
        public ByteBuffer load(long address, int length)
        {
            return MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN);
        }

        @Override
        long onHeapSize()
        {
            return 0;
        }

        @Override
        public boolean releaseNeeded()
        {
            return false;
        }

        @Override
        public void release(long handle, int length)
        {
            // Nothing to do as we can't release data in the allocator. Trie will remove its cells as needed.
        }

        @Override
        public void completeMutation()
        {
            // Nothing needed as we can't recycle allocator memory
        }

        @Override
        public void abortMutation()
        {
            // Nothing needed as we can't recycle allocator memory
        }

        @Override
        long unusedReservedOnHeapMemory()
        {
            return 0;
        }

        @Override
        void releaseReferencesUnsafe()
        {
            // no references held
        }
    }

    /// Trie dumper attaching paths and types to cells and a translation of the key for rows and partitions.
    static class Dumper extends TrieDumperWithPath.DeletionAware<Object, TrieTombstoneMarker>
    {
        final TableMetadata metadata;
        Columns columns;
        int rowKeyLength = 0;
        int partitionKeyLength = 0;

        Dumper(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public String contentToString(Object content)
        {
            if (content instanceof TrieCellData)
            {
                byte[] cellPath = Arrays.copyOfRange(keyBytes, rowKeyLength, keyPos);
                Cell<?> asCell = TrieBackedRow.cellFromCellData((TrieCellData) content, cellPath, cellPath.length, columns);
                return asCell.toString();
            }
            else if (content instanceof LivenessInfo)
            {
                rowKeyLength = keyPos;
                Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                            ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION, keyBytes, partitionKeyLength, keyPos - partitionKeyLength),
                                                                                            TrieBackedPartition.BYTE_COMPARABLE_VERSION);
                columns = metadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
                return content + " at " + clustering.toString(metadata);
            }
            else if (content instanceof PartitionData)
            {
                partitionKeyLength = keyPos;
                BufferDecoratedKey key = BufferDecoratedKey.fromByteComparable(ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION, keyBytes, 0, keyPos),
                                                                               TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                               metadata.partitioner);
                return content + " at " + metadata.partitionKeyType.getString(key.getKey());
            }

            return content.toString();
        }

        @Override
        public String deletionToString(TrieTombstoneMarker deletionMarker)
        {
            return deletionMarker.toString();
        }
    }

    /// Where values that do not fit a trie cell are kept: memtable memory (`native`, the default), an append-only
    /// per-shard store outside memtable memory (`file`; requires `offheap_objects`), or the commit log segments the
    /// values were written to, with such a store as fallback (`commitlog`; see [#COMMITLOG]).
    public enum ValueStorage
    {
        NATIVE
        {
            @Override
            CellDataBufferManager newCellDataManager(TableMetadata metadata, long memtableSeq, int shard)
            {
                return null;   // the shard picks NativeBufferManager / SlabBufferManager from its allocator
            }
        },
        FILE
        {
            @Override
            CellDataBufferManager newCellDataManager(TableMetadata metadata, long memtableSeq, int shard)
            {
                return new ValueStoreCellDataManager(FileValueStore.forShard(metadata, memtableSeq, shard));
            }

            @Override
            void onAccepted()
            {
                if (warned.compareAndSet(false, true))
                    logger.warn("TrieMemtable {}=file: file and commitlog value storage are experimental. Values are kept in files " +
                                "under the commit log directory, outside memtable memory, but count toward memtable_offheap_space: " +
                                "such a table flushes at the same value volume as with native storage unless memtable_offheap_space " +
                                "is raised, which raises the memory ceiling of native tables too, and MemtableOffHeapDataSize, " +
                                "AllMemtablesOffHeapDataSize and the memtable pool metrics include value bytes that are on disk and " +
                                "in the page cache rather than in memtable memory.",
                                VALUE_STORAGE_OPTION);
            }
        },
        /// Values are referenced in the memory-mapped commit log segment the write put them in, which the memtable
        /// keeps mapped until it is discarded, and copied as with [#FILE] when they cannot be found there. Requires
        /// `offheap_objects` and an uncompressed, unencrypted commit log with `commitlog_disk_access_mode: mmap`.
        /// Accepting it in a process that writes through the commit log turns on [SerializedValueOffsets#ENABLED] for
        /// the whole node until it restarts. See [`commitlog` value storage](./TrieMemtable.md#commitlog-value-storage).
        COMMITLOG
        {
            @Override
            CellDataBufferManager newCellDataManager(TableMetadata metadata, long memtableSeq, int shard)
            {
                return new ValueStoreCellDataManager(new CommitLogValueStore(FileValueStore.forShard(metadata, memtableSeq, shard)));
            }

            @Override
            void validate()
            {
                super.validate();
                if (!writesThroughCommitLog())
                    return;
                ParameterizedClass compression = DatabaseDescriptor.getCommitLogCompression();
                if (compression != null)
                    throw new ConfigurationException(String.format("TrieMemtable %s=commitlog requires an uncompressed commit log (configured commitlog_compression: %s)",
                                                                   VALUE_STORAGE_OPTION, compression.class_name));
                EncryptionContext encryption = DatabaseDescriptor.getEncryptionContext();
                if (encryption != null && encryption.isEnabled())
                    throw new ConfigurationException(String.format("TrieMemtable %s=commitlog requires an unencrypted commit log (transparent_data_encryption_options is enabled)",
                                                                   VALUE_STORAGE_OPTION));
                Config.DiskAccessMode accessMode = DatabaseDescriptor.getCommitLogWriteDiskAccessMode();
                if (accessMode != Config.DiskAccessMode.mmap)
                    throw new ConfigurationException(String.format("TrieMemtable %s=commitlog requires commitlog_disk_access_mode: mmap, but it is %s (auto is direct where direct I/O is supported; set legacy or mmap)",
                                                                   VALUE_STORAGE_OPTION, accessMode));
            }

            @Override
            void onAccepted()
            {
                if (writesThroughCommitLog())
                    SerializedValueOffsets.ENABLED = true;
                if (warned.compareAndSet(false, true))
                    logger.warn("TrieMemtable {}=commitlog: file and commitlog value storage are experimental. Values are kept outside " +
                                "memtable memory, in the commit log segments they were written to, or in files under the commit log " +
                                "directory for the values that cannot be referenced there, but count toward memtable_offheap_space: " +
                                "such a table flushes at the same value volume as with native storage unless memtable_offheap_space " +
                                "is raised, which raises the memory ceiling of native tables too, and MemtableOffHeapDataSize, " +
                                "AllMemtablesOffHeapDataSize and the memtable pool metrics include value bytes that are on disk and " +
                                "in the page cache rather than in memtable memory.",
                                VALUE_STORAGE_OPTION);
            }
        };

        /// Set once the warning that this value storage is experimental has been logged; never set for [#NATIVE].
        /// Package-private so that the constant bodies can reach it.
        final AtomicBoolean warned = new AtomicBoolean();

        /// The value storage named by the `value_storage` option `option`, ignoring case; [#NATIVE] if it is null.
        static ValueStorage fromOption(String option)
        {
            if (option == null)
                return NATIVE;
            try
            {
                return valueOf(option.trim().toUpperCase(Locale.ROOT));
            }
            catch (IllegalArgumentException e)
            {
                StringBuilder expected = new StringBuilder();
                for (ValueStorage storage : values())
                    expected.append(expected.length() == 0 ? "" : ", ").append(storage.name().toLowerCase(Locale.ROOT));
                throw new ConfigurationException(String.format("Invalid %s '%s' for TrieMemtable; expected one of %s",
                                                               VALUE_STORAGE_OPTION, option, expected));
            }
        }

        /// The cell data manager for one shard, or null to keep values in memtable memory. This is the only place
        /// that knows which store a constant maps to.
        abstract CellDataBufferManager newCellDataManager(TableMetadata metadata, long memtableSeq, int shard);

        /// Checks the configuration this value storage needs, so that an invalid setting is reported when the memtable
        /// is configured rather than when the first store is created.
        void validate()
        {
            if (this != NATIVE && DatabaseDescriptor.getMemtableAllocationType() != Config.MemtableAllocationType.offheap_objects)
                throw new ConfigurationException(requiresOffheapObjectsMessage());
        }

        /// Called by [TrieMemtable#factory] once the configuration has been accepted: [#FILE] and [#COMMITLOG] log,
        /// once each, that they are experimental, and [#COMMITLOG] turns on [SerializedValueOffsets#ENABLED] in a
        /// process that writes through the commit log.
        void onAccepted()
        {
        }

        private String requiresOffheapObjectsMessage()
        {
            return String.format("TrieMemtable %s=%s requires memtable_allocation_type: offheap_objects (configured: %s)",
                                 VALUE_STORAGE_OPTION,
                                 name().toLowerCase(Locale.ROOT),
                                 DatabaseDescriptor.getMemtableAllocationType());
        }

        /// Lets every value storage log its warning again.
        @VisibleForTesting
        static void resetWarnings()
        {
            for (ValueStorage valueStorage : values())
                valueStorage.warned.set(false);
        }
    }
}
