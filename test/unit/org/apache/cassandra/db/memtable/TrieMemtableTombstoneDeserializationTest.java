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

import org.junit.BeforeClass;
import org.junit.Test;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableBufferAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/// Checks which writes into a partition with an existing deletion branch need to deserialise the branch's tombstone
/// markers. Applying a write positions a cursor over the existing deletion branch and asks it for the deletion that
/// covers the written row, which is the preceding state of the next marker. When that marker has no preceding side,
/// as with the opening marker of a deleted row, the marker must not be deserialised.
public class TrieMemtableTombstoneDeserializationTest
{
    static final long DELETION_TIMESTAMP = 100;
    static final long OLDER_TIMESTAMP = 50;
    static final long NEWER_TIMESTAMP = 200;

    static TableMetadata metadata;
    static DecoratedKey partitionKey;
    static ColumnMetadata column;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "tbl")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .build();
        partitionKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(0));
        column = metadata.regularColumns().getSimple(0);
    }

    @Test
    public void testRowWriteBesideRowDeletionsDoesNotDeserializeMarkers()
    {
        Partition partition = new Partition();
        PartitionUpdate.Builder deletions = builder(3);
        for (int ck : new int[]{ 10, 20, 30 })
            deletions.add(deletedRow(ck, DELETION_TIMESTAMP));
        partition.apply(deletions.build());

        partition.serializer.markersDeserialized = 0;
        partition.apply(rowUpdate(15, NEWER_TIMESTAMP));
        partition.apply(rowUpdate(25, OLDER_TIMESTAMP));
        assertEquals(0, partition.serializer.markersDeserialized);

        TrieBackedPartition read = partition.read();
        assertNotNull(read.getRow(clustering(15)).getCell(column));
        assertNotNull(read.getRow(clustering(25)).getCell(column));
        assertFalse(read.getRow(clustering(20)).deletion().isLive());
    }

    @Test
    public void testRowDeletionBesideRowDeletionsDoesNotDeserializeMarkers()
    {
        Partition partition = new Partition();
        PartitionUpdate.Builder deletions = builder(2);
        for (int ck : new int[]{ 10, 30 })
            deletions.add(deletedRow(ck, DELETION_TIMESTAMP));
        partition.apply(deletions.build());

        partition.serializer.markersDeserialized = 0;
        PartitionUpdate.Builder deletion = builder(1);
        deletion.add(deletedRow(20, NEWER_TIMESTAMP));
        partition.apply(deletion.build());
        assertEquals(0, partition.serializer.markersDeserialized);

        TrieBackedPartition read = partition.read();
        for (int ck : new int[]{ 10, 20, 30 })
            assertFalse(read.getRow(clustering(ck)).deletion().isLive());
        assertEquals(NEWER_TIMESTAMP, read.getRow(clustering(20)).deletion().time().markedForDeleteAt());
    }

    @Test
    public void testRowWriteUnderRangeDeletionDeserializesMarkers()
    {
        Partition partition = new Partition();
        PartitionUpdate.Builder deletion = builder(0);
        deletion.add(new RangeTombstone(Slice.make(clustering(10), clustering(30)),
                                        DeletionTime.build(DELETION_TIMESTAMP, 0)));
        partition.apply(deletion.build());

        partition.serializer.markersDeserialized = 0;
        partition.apply(rowUpdate(15, NEWER_TIMESTAMP));
        assertTrue(partition.serializer.markersDeserialized > 0);

        // The covering deletion found through the marker must still delete older incoming data.
        partition.apply(rowUpdate(25, OLDER_TIMESTAMP));
        TrieBackedPartition read = partition.read();
        assertNotNull(read.getRow(clustering(15)).getCell(column));
        Row shadowed = read.getRow(clustering(25));
        assertTrue(shadowed == null || shadowed.getCell(column) == null);
    }

    @Test
    public void testPrecedingStateIsNullMatchesDeserializedMarker() throws TrieSpaceExhaustedException
    {
        TrieMemtable.TrieSerializer serializer = new TrieMemtable.TrieSerializer(null, null);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        int inBufferPos = 16; // tombstone sides are stored at negative offsets from the position
        TrieTombstoneMarker.Covering range = TrieTombstoneMarker.covering(DELETION_TIMESTAMP, 0, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker.Covering row = TrieTombstoneMarker.covering(NEWER_TIMESTAMP, 0, TrieTombstoneMarker.Kind.ROW);
        TrieTombstoneMarker[] markers = {
            TrieTombstoneMarker.make(null, row, TrieTombstoneMarker.LevelMarker.ROW),
            TrieTombstoneMarker.make(row, null, TrieTombstoneMarker.LevelMarker.ROW),
            TrieTombstoneMarker.make(range, row, TrieTombstoneMarker.LevelMarker.ROW),
            TrieTombstoneMarker.make(row, range, TrieTombstoneMarker.LevelMarker.ROW),
            TrieTombstoneMarker.make(range, range, TrieTombstoneMarker.LevelMarker.ROW),
            TrieTombstoneMarker.make(null, range, null),
            TrieTombstoneMarker.make(range, null, null),
            TrieTombstoneMarker.make(range, row, null),
        };
        for (TrieTombstoneMarker marker : markers)
        {
            for (boolean afterBranch : new boolean[]{ false, true })
            {
                int offsetBits = serializer.serialize(marker, afterBranch, buffer, inBufferPos);
                TrieTombstoneMarker deserialized = (TrieTombstoneMarker) serializer.deserialize(buffer, inBufferPos, offsetBits);
                assertEquals(marker, deserialized);
                for (Direction direction : Direction.values())
                    assertEquals(marker + " " + direction,
                                 deserialized.precedingState(direction) == null,
                                 serializer.precedingStateIsNull(buffer, inBufferPos, offsetBits, direction));
            }
        }
    }

    static Clustering<?> clustering(int ck)
    {
        return Clustering.make(Int32Type.instance.decompose(ck));
    }

    static PartitionUpdate.Builder builder(int rows)
    {
        return TriePartitionUpdate.FACTORY.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), rows);
    }

    static Row deletedRow(int ck, long timestamp)
    {
        Row.Builder row = TriePartitionUpdate.FACTORY.rowBuilder(metadata.regularColumns(), false);
        row.newRow(clustering(ck));
        row.addRowDeletion(Row.Deletion.regular(DeletionTime.build(timestamp, 0)));
        return row.build();
    }

    static PartitionUpdate rowUpdate(int ck, long timestamp)
    {
        Row.Builder row = TriePartitionUpdate.FACTORY.rowBuilder(metadata.regularColumns(), false);
        row.newRow(clustering(ck));
        row.addCell(BufferCell.live(column, timestamp, Int32Type.instance.decompose(ck)));
        PartitionUpdate.Builder update = builder(1);
        update.add(row.build());
        return update.build();
    }

    /// A memtable partition trie whose serializer counts the tombstone markers it deserialises.
    static class Partition
    {
        final OpOrder opOrder = new OpOrder();
        final MemtableAllocator allocator;
        final CountingSerializer serializer;
        final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie;
        final TriePartitionUpdater updater;

        Partition()
        {
            MemtablePool pool = AbstractAllocatorMemtable.createMemtableAllocatorPoolInternal(Config.MemtableAllocationType.heap_buffers,
                                                                                              1 << 20,
                                                                                              1 << 20,
                                                                                              0.25f,
                                                                                              () -> ImmediateFuture.failure(new IllegalStateException()));
            allocator = pool.newAllocator("test");
            serializer = new CountingSerializer(new TrieMemtable.SlabBufferManager((MemtableBufferAllocator) allocator,
                                                                                   opOrder,
                                                                                   BufferType.ON_HEAP.onHeapSizeWithoutData()));
            trie = InMemoryDeletionAwareTrie.longLived(TrieBackedPartition.BYTE_COMPARABLE_VERSION, BufferType.ON_HEAP, opOrder, serializer);
            updater = new TriePartitionUpdater(null, trie);
            try
            {
                trie.putRecursive(ByteComparable.EMPTY, new TrieMemtable.PartitionData(null), (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        void apply(PartitionUpdate update)
        {
            try (OpOrder.Group group = opOrder.start())
            {
                TrieMemtable.mergeUpdate(trie, allocator, TriePartitionUpdate.asMergableTrieWithoutPartitionPrefix(update), UpdateTransaction.NO_OP, group, updater);
            }
        }

        TrieBackedPartition read()
        {
            return new TrieBackedPartition(partitionKey,
                                           metadata.regularAndStaticColumns(),
                                           new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 0),
                                           1, // to avoid isEmpty true
                                           0,
                                           trie,
                                           metadata);
        }
    }

    static class CountingSerializer extends TrieMemtable.TrieSerializer
    {
        int markersDeserialized;

        CountingSerializer(TrieMemtable.CellDataBufferManager manager)
        {
            super(manager, null);
        }

        @Override
        public Object deserialize(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            if (offsetBits == TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH || offsetBits == TYPE_TOMBSTONE_MARKER_AFTER_BRANCH)
                ++markersDeserialized;
            return super.deserialize(buffer, inBufferPos, offsetBits);
        }
    }
}
