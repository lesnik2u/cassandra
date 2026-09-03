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
package org.apache.cassandra.db.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A row is grafted onto a trie-backed update with a recursive put when neither the row nor the destination can
 * contribute a deletion, and with the general merge otherwise; see
 * {@link org.apache.cassandra.db.partitions.TrieBackedPartition#putInTrie}. These tests cover both arms and the
 * transitions between them, by building the same content through the trie and the BTree factory and requiring the two
 * to describe the same partition.
 *
 * The two are compared at {@link MessagingService#VERSION_DS_20}, where a trie-backed update is written through
 * {@code UnfilteredRowIteratorSerializer} like any other, so the bytes of the two encodings must be identical.
 */
public class TriePartitionUpdateBuilderTest extends CQLTester
{
    private static final long TIMESTAMP = 1000L;
    private static final long NOW_IN_SEC = 1500L;

    /** No complex column and a fixed-length clustering: rows here are eligible for the graft. */
    private void createSimpleTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v int, w int, s int static, PRIMARY KEY(k, c))");
    }

    /** A complex column takes the whole table off the graft path: cell paths are unbounded. */
    private void createComplexTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v int, m map<text, text>, PRIMARY KEY(k, c))");
    }

    /** A variable-length clustering also takes the table off the graft path. */
    private void createTextClusteringTable()
    {
        createTable("CREATE TABLE %s (k text, c text, v int, PRIMARY KEY(k, c))");
    }

    @Test
    public void testSingleRow()
    {
        createSimpleTable();
        assertSameUpdate(add -> add.row(1).liveness(TIMESTAMP).cell("v", 11));
    }

    @Test
    public void testManyRows()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            for (int i = 0; i < 20; ++i)
                add.row(i).liveness(TIMESTAMP).cell("v", i).cell("w", i * 2);
        });
    }

    /** An UPDATE writes cells without primary key liveness info. */
    @Test
    public void testRowWithoutLiveness()
    {
        createSimpleTable();
        assertSameUpdate(add -> add.row(1).cell("v", 11));
    }

    @Test
    public void testStaticRow()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            add.staticRow().cell("s", 7);
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
        });
    }

    /** The same clustering added twice must be resolved by the same transformer on either arm. */
    @Test
    public void testRepeatedClustering()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
            add.row(1).liveness(TIMESTAMP + 1).cell("v", 22).cell("w", 33);
        });
    }

    /** A row deletion puts a deletion branch in the trie, so this row and every row after it takes the merge. */
    @Test
    public void testRowDeletionAmongLiveRows()
    {
        createSimpleTable();
        assertSameContent(add -> {
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
            add.row(2).deletion(TIMESTAMP);
            add.row(3).liveness(TIMESTAMP).cell("v", 33);
        });
    }

    @Test
    public void testPartitionDeletionBeforeRows()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            add.partitionDeletion(TIMESTAMP - 1);
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
            add.row(2).liveness(TIMESTAMP).cell("v", 22);
        });
    }

    @Test
    public void testPartitionDeletionAfterRows()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
            add.partitionDeletion(TIMESTAMP - 1);
            add.row(2).liveness(TIMESTAMP).cell("v", 22);
        });
    }

    @Test
    public void testRangeTombstoneAmongRows()
    {
        createSimpleTable();
        assertSameUpdate(add -> {
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
            add.rangeTombstone(5, 9, TIMESTAMP - 1);
            add.row(20).liveness(TIMESTAMP).cell("v", 22);
        });
    }

    /**
     * A deletion already in the update has to be applied to rows added after it, which only the merge does. These
     * shapes pin down that the graft is switched off for the rest of the partition as soon as a deletion goes in.
     * They cannot be compared against the BTree encoding, which keeps shadowed rows and only resolves them on read.
     */
    @Test
    public void testPartitionDeletionShadowsLaterRow()
    {
        createSimpleTable();
        PartitionUpdate update = build(new TriePartitionUpdate.TrieFactory(), add -> {
            add.partitionDeletion(TIMESTAMP + 1);
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
        });
        assertFalse("the shadowed row was kept", update.unfilteredIterator().hasNext());
    }

    @Test
    public void testRangeTombstoneShadowsLaterRow()
    {
        createSimpleTable();
        PartitionUpdate update = build(new TriePartitionUpdate.TrieFactory(), add -> {
            add.rangeTombstone(0, 10, TIMESTAMP + 1);
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
        });
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            while (iterator.hasNext())
                assertTrue("the shadowed row was kept", iterator.next().isRangeTombstoneMarker());
        }
    }

    @Test
    public void testRowDeletionShadowsLaterCell()
    {
        createSimpleTable();
        PartitionUpdate update = build(new TriePartitionUpdate.TrieFactory(), add -> {
            add.row(1).deletion(TIMESTAMP + 1);
            add.row(1).liveness(TIMESTAMP).cell("v", 11);
        });
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            assertTrue(iterator.hasNext());
            Row row = (Row) iterator.next();
            assertFalse("the shadowed cell was kept", row.cells().iterator().hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testComplexColumn()
    {
        createComplexTable();
        assertSameUpdate(add -> {
            add.row(1).liveness(TIMESTAMP).cell("v", 11).complexCell("m", "a", "1").complexCell("m", "b", "2");
            add.row(2).liveness(TIMESTAMP).complexDeletion("m", TIMESTAMP - 1).complexCell("m", "c", "3");
        });
    }

    @Test
    public void testTextClustering()
    {
        createTextClusteringTable();
        assertSameUpdate(add -> {
            add.row("a-long-clustering-value-0").liveness(TIMESTAMP).cell("v", 11);
            add.row("a-long-clustering-value-1").liveness(TIMESTAMP).cell("v", 22);
        });
    }

    private void assertSameUpdate(Consumer<Adder> content)
    {
        PartitionUpdate trie = assertSameContent(content);
        PartitionUpdate btree = build(new BTreePartitionUpdate.BTreeFactory(), content);
        assertEquals(btree.rowCount(), trie.rowCount());
        assertEquals(btree.operationCount(), trie.operationCount());
        // The counts are part of the header, so the bytes can only be compared where the counts agree.
        assertArrayEquals(serialize(btree), serialize(trie));
    }

    /**
     * Compares what the two encodings describe, without the row and operation counts. A row that carries only a
     * deletion is not counted by the trie update builder, which counts rows as they contribute liveness info; that
     * divergence predates the graft and is not what these tests are about.
     */
    private PartitionUpdate assertSameContent(Consumer<Adder> content)
    {
        PartitionUpdate trie = build(new TriePartitionUpdate.TrieFactory(), content);
        PartitionUpdate btree = build(new BTreePartitionUpdate.BTreeFactory(), content);

        assertTrue("expected a trie-backed update, got " + trie.getClass().getName(),
                   trie instanceof TriePartitionUpdate);
        assertEquals(btree.stats(), trie.stats());
        assertEquals(btree.partitionLevelDeletion(), trie.partitionLevelDeletion());
        assertEquals(btree.staticRow(), trie.staticRow());

        try (UnfilteredRowIterator expected = btree.unfilteredIterator();
             UnfilteredRowIterator actual = trie.unfilteredIterator())
        {
            while (expected.hasNext())
            {
                assertTrue("trie update ran out of content", actual.hasNext());
                assertEquals(expected.next(), actual.next());
            }
            assertTrue("trie update has extra content", !actual.hasNext());
        }
        return trie;
    }

    private PartitionUpdate build(PartitionUpdate.Factory factory, Consumer<Adder> content)
    {
        TableMetadata metadata = currentTableMetadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0"));
        PartitionUpdate.Builder builder = factory.builder(metadata, key, metadata.regularAndStaticColumns(), 4);
        Adder adder = new Adder(factory, metadata, builder);
        content.accept(adder);
        adder.flush();
        return builder.build();
    }

    private static byte[] serialize(PartitionUpdate update)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, MessagingService.VERSION_DS_20);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Adds content to a partition update through whichever factory is under test, so that the same test body can
     * build both encodings. Rows are accumulated and handed to the update builder when the next one starts.
     */
    private static class Adder
    {
        private final PartitionUpdate.Factory factory;
        private final TableMetadata metadata;
        private final PartitionUpdate.Builder builder;

        private Row.Builder rowBuilder;

        Adder(PartitionUpdate.Factory factory, TableMetadata metadata, PartitionUpdate.Builder builder)
        {
            this.factory = factory;
            this.metadata = metadata;
            this.builder = builder;
        }

        Adder row(int clustering)
        {
            return newRow(false, metadata.comparator.make(clustering));
        }

        Adder row(String clustering)
        {
            return newRow(false, metadata.comparator.make(clustering));
        }

        Adder staticRow()
        {
            return newRow(true, Clustering.STATIC_CLUSTERING);
        }

        private Adder newRow(boolean isStatic, Clustering<?> clustering)
        {
            flush();
            rowBuilder = factory.rowBuilder(isStatic ? metadata.staticColumns() : metadata.regularColumns(), true);
            rowBuilder.newRow(clustering);
            return this;
        }

        Adder liveness(long timestamp)
        {
            rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, NOW_IN_SEC));
            return this;
        }

        Adder deletion(long timestamp)
        {
            rowBuilder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(timestamp, NOW_IN_SEC)));
            return this;
        }

        Adder cell(String name, Object value)
        {
            ColumnMetadata column = column(name);
            rowBuilder.addCell(BufferCell.live(column, TIMESTAMP, decompose(value)));
            return this;
        }

        Adder complexCell(String name, String path, String value)
        {
            ColumnMetadata column = column(name);
            rowBuilder.addCell(BufferCell.live(column, TIMESTAMP, decompose(value),
                                               CellPath.create(decompose(path))));
            return this;
        }

        Adder complexDeletion(String name, long timestamp)
        {
            rowBuilder.addComplexDeletion(column(name), DeletionTime.build(timestamp, NOW_IN_SEC));
            return this;
        }

        Adder partitionDeletion(long timestamp)
        {
            flush();
            builder.addPartitionDeletion(DeletionTime.build(timestamp, NOW_IN_SEC));
            return this;
        }

        Adder rangeTombstone(int start, int end, long timestamp)
        {
            flush();
            Slice slice = Slice.make(ClusteringBound.inclusiveStartOf(metadata.comparator.make(start)),
                                     ClusteringBound.exclusiveEndOf(metadata.comparator.make(end)));
            builder.add(new RangeTombstone(slice, DeletionTime.build(timestamp, NOW_IN_SEC)));
            return this;
        }

        void flush()
        {
            if (rowBuilder != null)
            {
                builder.add(rowBuilder.build());
                rowBuilder = null;
            }
        }

        private ColumnMetadata column(String name)
        {
            return metadata.getColumn(ColumnIdentifier.getInterned(name, false));
        }

        private static ByteBuffer decompose(Object value)
        {
            return value instanceof Integer ? Int32Type.instance.decompose((Integer) value)
                                            : UTF8Type.instance.decompose((String) value);
        }
    }
}
