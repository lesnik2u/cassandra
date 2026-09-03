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
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * From {@link MessagingService#VERSION_DS_21} on a trie-backed update is always written in the trie encoding, with
 * format byte 1, whatever its shape and whatever the BTree encoding of it would have cost. These tests pin that down
 * and, above all, that {@code serializedSize} describes the bytes {@code serialize} then writes: the commit log
 * reserves the region the first reports and the second fills it.
 */
public class PartitionUpdateFormatTest extends CQLTester
{
    private static final int VERSION = MessagingService.VERSION_DS_21;

    private static final byte BTREE_FORMAT = 0;
    private static final byte TRIE_FORMAT = 1;

    private void createRichTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v int, m map<text, text>, " +
                    "s int static, ss set<text> static, PRIMARY KEY(k, c))");
    }

    /** A table of the shape a write workload uses: one row per partition, one blob column. */
    private void createBlobTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v blob, PRIMARY KEY(k, c))");
    }

    private TriePartitionUpdate singleRowBlobUpdate()
    {
        return build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", ByteBuffer.allocate(100));
        });
    }

    /** A partition whose clustering keys share long prefixes, which is what the trie encoding is smaller for. */
    private void createSharedPrefixTable()
    {
        createTable("CREATE TABLE %s (k text, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2))");
    }

    private TriePartitionUpdate sharedPrefixUpdate()
    {
        return build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            for (int i = 0; i < 10; ++i)
                builder.row("a-common-long-clustering-prefix-" + (i / 5), "suffix-" + i).add("v", i);
        });
    }

    private TriePartitionUpdate richUpdate()
    {
        return build(builder -> {
            // Older than everything below, so nothing it covers is shadowed away.
            builder.timestamp(1000).nowInSec(1500).delete();

            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("v", 11).add("m", ImmutableMap.of("a", "1", "b", "2"));
            builder.row(3).add("v", 33);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
            builder.addRangeTombstone().start(20).end(30).exclStart().inclEnd();
        });
    }

    private TriePartitionUpdate build(Consumer<PartitionUpdate.SimpleBuilder> content)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), "key0");
        content.accept(builder);
        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    /**
     * Every shape has to be written in the trie encoding, and the size reported for it has to be the number of bytes
     * that then get written.
     */
    @Test
    public void testTrieEncodingIsWrittenForEveryShape() throws Throwable
    {
        createRichTable();
        TableMetadata metadata = currentTableMetadata();

        assertTrieEncodingIsWritten("empty update",
                                    () -> TriePartitionUpdate.asTrieUpdate(PartitionUpdate.emptyUpdate(metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0")))));

        assertTrieEncodingIsWritten("one row", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
        }));

        assertTrieEncodingIsWritten("ten rows", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            for (int i = 0; i < 10; ++i)
                builder.row(i).add("v", i);
        }));

        assertTrieEncodingIsWritten("row with a ttl", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500).ttl(60);
            builder.row(1).add("v", 11);
        }));

        assertTrieEncodingIsWritten("range tombstone", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        }));

        assertTrieEncodingIsWritten("complex columns", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("m", ImmutableMap.of("a", "1", "b", "2"));
        }));

        assertTrieEncodingIsWritten("everything at once", this::richUpdate);

        createSharedPrefixTable();
        assertTrieEncodingIsWritten("shared clustering prefixes", this::sharedPrefixUpdate);

        createBlobTable();
        assertTrieEncodingIsWritten("single row with a 100 byte blob", this::singleRowBlobUpdate);
    }

    /**
     * The two shapes the encodings differ most on: a single row with a blob, where the trie's nodes and pointers are
     * overhead a partition of one row has nothing to amortise them over and the trie encoding is the larger of the
     * two, and clustering keys that share long prefixes, which the trie stores once and the BTree encoding repeats
     * per row. Both go out in the trie format; the size comparison is recorded here, not decisive.
     */
    @Test
    public void testTrieEncodingIsWrittenEvenWhereItIsTheLargerOne() throws Throwable
    {
        createBlobTable();
        assertTrue("the trie encoding is expected to be the larger one for a single-row update",
                   trieEncoded(singleRowBlobUpdate()).length > btreeEncoded(singleRowBlobUpdate()).length);
        assertTrieEncodingIsWritten("single row with a 100 byte blob", this::singleRowBlobUpdate);

        createSharedPrefixTable();
        assertTrue("the trie encoding is expected to be the smaller one when clustering keys share prefixes",
                   trieEncoded(sharedPrefixUpdate()).length < btreeEncoded(sharedPrefixUpdate()).length);
        assertTrieEncodingIsWritten("shared clustering prefixes", this::sharedPrefixUpdate);
    }

    /**
     * Sizing an update memoizes its size on it and lays its trie out, so the state a write starts from depends on
     * whether a sizing ran first. The bytes must not: a write that is not preceded by a sizing has to write the same
     * format and the same bytes as one that follows a sizing.
     */
    @Test
    public void testSerializingWithoutSizingFirstWritesTheSameBytes() throws Throwable
    {
        createRichTable();
        assertSameBytesWithAndWithoutSizingFirst("everything at once", this::richUpdate);
        assertSameBytesWithAndWithoutSizingFirst("one row", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
        }));

        // The two shapes the encodings differ most on: the layout a sizing retains is consumed by the write in both,
        // and consuming it may not change what gets written.
        createSharedPrefixTable();
        assertSameBytesWithAndWithoutSizingFirst("shared clustering prefixes", this::sharedPrefixUpdate);

        createBlobTable();
        assertSameBytesWithAndWithoutSizingFirst("single row with a 100 byte blob", this::singleRowBlobUpdate);
    }

    /**
     * Nothing writes a format-0 body for a trie-backed update any more, but the read path still has to accept one:
     * it can come from a peer or from a log written before the trie format existed. Either format has to come back
     * trie-backed on a table that asks for a trie memtable, and with the content that was written: a format-0 body is
     * built through the table's own partition update factory, a format-1 body by the trie serializer itself.
     */
    @Test
    public void testTrieBackedTableReadsBothFormatsBackAsTrieUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2)) WITH memtable = 'trie'");

        TriePartitionUpdate singleRow = build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row("c", "r").add("v", 1);
        });

        byte[] btreeFormat = btreeFormatStream(singleRow);
        assertEquals(BTREE_FORMAT, formatOf(singleRow, btreeFormat));
        assertTrue("an update written in the BTree format must read back trie-backed on a trie-backed table",
                   deserialize(btreeFormat) instanceof TriePartitionUpdate);
        assertEquals(singleRow, TriePartitionUpdate.asTrieUpdate(deserialize(btreeFormat)));

        byte[] written = serialize(singleRow);
        assertEquals(TRIE_FORMAT, formatOf(singleRow, written));
        assertTrue(deserialize(written) instanceof TriePartitionUpdate);
        assertEquals(singleRow, TriePartitionUpdate.asTrieUpdate(deserialize(written)));
    }

    /**
     * Size the update the way the mutation path does, write it, and check that the size describes the bytes written,
     * that the format byte is the trie encoding's, that the body is that encoding's bytes, and that it reads back.
     */
    private void assertTrieEncodingIsWritten(String shape, Supplier<TriePartitionUpdate> updates) throws IOException
    {
        // Measured on an update of its own: sizing one lays its trie out and memoizes, and the update that is
        // written below must be in the state a freshly built one is in.
        byte[] trieEncoded = trieEncoded(updates.get());

        TriePartitionUpdate update = updates.get();
        long size;
        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            size = PartitionUpdate.serializer.serializedSize(update, VERSION);
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            written = out.toByteArray();
        }

        assertEquals(shape + ": serializedSize must describe the bytes written", size, written.length);
        assertEquals(shape + ": format byte", TRIE_FORMAT, formatOf(update, written));
        assertArrayEquals(shape + ": body must be the trie encoding",
                          trieEncoded,
                          Arrays.copyOfRange(written, bodyOffset(update), written.length));

        assertEquals(shape + ": round trip", update, TriePartitionUpdate.asTrieUpdate(deserialize(written)));
    }

    private void assertSameBytesWithAndWithoutSizingFirst(String shape, Supplier<TriePartitionUpdate> updates) throws IOException
    {
        byte[] withoutSizing = serialize(updates.get());

        TriePartitionUpdate sizedFirst = updates.get();
        PartitionUpdate.serializer.serializedSize(sizedFirst, VERSION);
        assertArrayEquals(shape, withoutSizing, serialize(sizedFirst));

        // And a second write of the same update, which no longer has the layout the sizing retained.
        assertArrayEquals(shape, withoutSizing, serialize(sizedFirst));
    }

    /** The format byte, which the writer puts right after the table id. */
    private static byte formatOf(PartitionUpdate update, byte[] written)
    {
        return written[(int) update.metadata().id.serializedSize()];
    }

    /** The offset of the encoded update itself, past the table id and the format byte. */
    private static int bodyOffset(PartitionUpdate update)
    {
        return (int) update.metadata().id.serializedSize() + 1;
    }

    private static byte[] serialize(PartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            return out.toByteArray();
        }
    }

    /**
     * What a sender that does not have the trie format, or a log written before it, leaves for the read path:
     * the table id, a format byte of 0, and the update's BTree encoding. Assembled here because the writer no longer
     * produces it for a trie-backed update.
     */
    private static byte[] btreeFormatStream(TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            update.metadata().id.serialize(out);
            out.writeByte(BTREE_FORMAT);
            out.write(btreeEncoded(update));
            return out.toByteArray();
        }
    }

    private static PartitionUpdate deserialize(byte[] written) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(written))
        {
            return PartitionUpdate.serializer.deserialize(in, VERSION, DeserializationHelper.Flag.LOCAL);
        }
    }

    /** The update's body in the trie encoding, written out rather than sized, so that the sizes are not their own check. */
    private static byte[] trieEncoded(TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TriePartitionUpdateSerializer.serialize(update, out, VERSION);
            return out.toByteArray();
        }
    }

    /** The update's body in the BTree encoding, likewise. */
    private static byte[] btreeEncoded(TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer();
             UnfilteredRowIterator iter = update.unfilteredIterator())
        {
            UnfilteredRowIteratorSerializer.serializer.serialize(iter, null, out, VERSION, update.rowCount());
            return out.toByteArray();
        }
    }
}
