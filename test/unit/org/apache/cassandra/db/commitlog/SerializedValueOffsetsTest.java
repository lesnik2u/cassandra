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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.TrieCellData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.TeeDataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

import static org.apache.cassandra.Util.captureLog;
import static org.apache.cassandra.Util.randomBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/// Unless a test says otherwise, hand-built locations are attached an all-zero serialization by
/// [SerializedValueOffsets#finish]: their values are all zero bytes too, so that every record passes the spot check of
/// [SerializedValueOffsets#find].
public class SerializedValueOffsetsTest
{
    private static final String KEYSPACE = "SerializedValueOffsetsTest";
    private static final String TABLE = "t";
    private static final String OTHER_TABLE = "t2";
    private static final String COUNTER_TABLE = "c";
    private static final int VERSION = MessagingService.current_version;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE " + TABLE + " (pk int, ck int, v blob, w int, s text, vec vector<float, 8>, " +
                                                               "m map<int, blob>, st blob static, PRIMARY KEY (pk, ck))",
                                                               KEYSPACE),
                                    CreateTableStatement.parse("CREATE TABLE " + OTHER_TABLE + " (pk int PRIMARY KEY, b blob)", KEYSPACE),
                                    CreateTableStatement.parse("CREATE TABLE " + COUNTER_TABLE + " (pk int, ck int, n_local counter, n_global counter, PRIMARY KEY (pk, ck))",
                                                               KEYSPACE));
    }

    @Before
    @After
    public void disableRecording()
    {
        SerializedValueOffsets.ENABLED = false;
    }

    @Test
    public void recordAndFindHeapSlice()
    {
        byte[] array = new byte[64];
        ByteBuffer slice = ByteBuffer.wrap(array, 5, 20).slice();
        assertEquals(5, slice.arrayOffset());

        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(array, 5, 20, 100);
        locations.finish(new byte[128]);
        assertEquals(1, locations.size());
        assertEquals(100, locations.find(slice));

        ByteBuffer other = ByteBuffer.wrap(array, 6, 20).slice();
        assertEquals(-1, locations.find(other));
    }

    @Test
    public void recordAndFindDirect()
    {
        ByteBuffer direct = ByteBuffer.allocateDirect(32);

        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(direct, 0, 32, 7);
        locations.finish(new byte[64]);
        assertEquals(7, locations.find(direct));

        ByteBuffer otherDirect = ByteBuffer.allocateDirect(32);
        assertEquals(-1, locations.find(otherDirect));
    }

    @Test
    public void findRequiresEqualLength()
    {
        byte[] array = new byte[64];

        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(array, 5, 20, 100);
        locations.finish(new byte[128]);
        assertEquals(-1, locations.find(ByteBuffer.wrap(array, 5, 19).slice()));
        assertEquals(-1, locations.find(ByteBuffer.wrap(array, 5, 21).slice()));
        assertEquals(100, locations.find(ByteBuffer.wrap(array, 5, 20).slice()));
    }

    @Test
    public void absoluteOffsetAddsEntryStart()
    {
        byte[] array = new byte[64];
        SerializedValueOffsets relative = new SerializedValueOffsets();
        relative.record(array, 5, 20, 100);
        relative.finish(new byte[128]);

        CommitLogValueLocations locations = new CommitLogValueLocations(1L, new File("CommitLog-1.log"), 1 << 20, 1000L, relative);
        assertEquals(1100L, locations.absoluteOffsetOf(ByteBuffer.wrap(array, 5, 20).slice()));
        assertEquals(-1L, locations.absoluteOffsetOf(ByteBuffer.wrap(array, 6, 20).slice()));
    }

    @Test
    public void absoluteOffsetWithoutRelativeIsMinusOne()
    {
        byte[] array = new byte[64];
        CommitLogValueLocations locations = new CommitLogValueLocations(1L, new File("CommitLog-1.log"), 1 << 20, 1000L, null);
        assertEquals(-1L, locations.absoluteOffsetOf(ByteBuffer.wrap(array, 5, 20).slice()));
    }

    /// A recorder records the values serialized into, or deserialized from, the stream that carries it, and a second
    /// stream without a recorder records nothing.
    @Test
    public void recorderRecordsOnlyItsOwnStream() throws IOException
    {
        SerializedValueOffsets.ENABLED = true;
        PartitionUpdate update = sampleMutation().getPartitionUpdates().iterator().next();

        SerializedValueOffsets locations = new SerializedValueOffsets();
        DataOutputBuffer target = new DataOutputBuffer();
        target.recorder = locations;
        byte[] serialized;
        try (DataOutputBuffer other = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, other, VERSION);
            serialized = other.toByteArray();
        }
        assertEquals("a serialization into a buffer without a recorder is not recorded", 0, locations.size());

        PartitionUpdate.serializer.serialize(update, target, VERSION);
        assertTrue(locations.size() > 0);

        locations = new SerializedValueOffsets();
        TeeDataInputPlus targetIn = new TeeDataInputPlus(new DataInputBuffer(serialized), new DataOutputBuffer());
        targetIn.recorder = locations;
        TeeDataInputPlus otherIn = new TeeDataInputPlus(new DataInputBuffer(serialized), new DataOutputBuffer());
        PartitionUpdate.serializer.deserialize(otherIn, VERSION, DeserializationHelper.Flag.FROM_REMOTE);
        assertEquals("a deserialization from an input without a recorder is not recorded", 0, locations.size());

        PartitionUpdate.serializer.deserialize(targetIn, VERSION, DeserializationHelper.Flag.FROM_REMOTE);
        assertTrue(locations.size() > 0);
    }

    /// The cached serialization of a mutation is built in the thread's scratch buffer, which the commit log already
    /// holds, empty, when it serializes an uncached mutation: the recorder found on the buffer records none of the
    /// mutation's values and is the buffer's recorder again afterwards. The commit log's recorder is null; this test
    /// sets one so that its restoration is visible.
    @Test
    public void cachingASerializationRestoresTheScratchBufferRecorder()
    {
        SerializedValueOffsets.ENABLED = true;
        DataOutputBuffer scratch = DataOutputBuffer.scratchBuffer.get();
        SerializedValueOffsets outer = new SerializedValueOffsets();
        scratch.recorder = outer;
        try
        {
            Mutation mutation = sampleMutation();
            Mutation.serializer.serializedSize(mutation, VERSION);   // builds the cached serialization
            assertEquals(SAMPLE_RECORDED, Mutation.serializer.offsetsFor(mutation, VERSION).size());
            assertSame(outer, scratch.recorder);
            assertEquals(0, outer.size());
        }
        finally
        {
            scratch.recorder = null;
        }
    }

    @Test
    public void recordGrowsBeyondInitialCapacity()
    {
        byte[] array = new byte[1024];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        for (int i = 0; i < 20; ++i)
            locations.record(array, i * 40, 20 + i, 1000 + i);
        locations.finish(new byte[1100]);

        assertEquals(20, locations.size());
        for (int i = 0; i < 20; ++i)
            assertEquals(1000 + i, locations.find(ByteBuffer.wrap(array, i * 40, 20 + i).slice()));
    }

    @Test
    public void findFindsEveryValueInAnyOrder()
    {
        int n = 1000;
        byte[][] arrays = new byte[n][];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        for (int i = 0; i < n; ++i)
        {
            arrays[i] = new byte[20];
            locations.recordValue(arrays[i], 20, i * 30);
        }
        locations.finish(new byte[n * 30]);

        for (int i = 0; i < n; ++i)
            assertEquals(i * 30, locations.find(ByteBuffer.wrap(arrays[i])));

        // out of order, the scan wraps around and still finds every value
        for (int i = n - 1; i >= 0; --i)
            assertEquals(i * 30, locations.find(ByteBuffer.wrap(arrays[i])));

        assertEquals(-1, locations.find(ByteBuffer.wrap(new byte[20])));
    }

    /// A lookup scans from the record after the previous hit, so that values looked up in the order they were recorded
    /// take one comparison each: a value recorded twice is found at its two records in turn.
    @Test
    public void findResumesAfterThePreviousHit()
    {
        byte[] array = new byte[20];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(array, 0, 20, 0);
        locations.record(array, 0, 20, 30);
        locations.finish(new byte[50]);

        for (int i = 0; i < 3; ++i)
        {
            assertEquals(0, locations.find(ByteBuffer.wrap(array)));
            assertEquals(30, locations.find(ByteBuffer.wrap(array)));
        }

        // a miss leaves the scan where the previous hit left it
        assertEquals(0, locations.find(ByteBuffer.wrap(array)));
        assertEquals(-1, locations.find(ByteBuffer.wrap(new byte[20])));
        assertEquals(30, locations.find(ByteBuffer.wrap(array)));
    }

    /// [SerializedValueOffsets#find] may be called concurrently on the locations of a cached serialization, whose scan
    /// start is updated without synchronization: every lookup must still find its value. A scan start read twice could
    /// be the record count, past the arrays; that race shows reliably only in interpreted code (`-Xint`), as compiled
    /// code may read the field once, so in a default run this is a smoke test.
    @Test
    public void concurrentFindsFindEveryValue() throws InterruptedException
    {
        int n = 8;   // the initial capacity: a scan starting at the record count would index past the arrays
        byte[][] arrays = new byte[n][];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        for (int i = 0; i < n; ++i)
        {
            arrays[i] = new byte[20];
            locations.recordValue(arrays[i], 20, i * 30);
        }
        locations.finish(new byte[n * 30]);

        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        Thread[] threads = new Thread[4];
        for (int t = 0; t < threads.length; ++t)
        {
            int seed = t;
            threads[t] = new Thread(() -> {
                try
                {
                    Random random = new Random(seed);
                    for (int i = 0; i < 100_000; ++i)
                    {
                        int index = random.nextInt(n);
                        int found = locations.find(ByteBuffer.wrap(arrays[index]));
                        if (found != index * 30)
                            throw new AssertionError("value " + index + " found at " + found);
                    }
                }
                catch (Throwable e)
                {
                    errors.add(e);
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads)
            thread.join();
        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }

    /// [SerializedValueOffsets#MIN_RECORDED_LENGTH] is the shortest value that is not embedded in a trie cell, and the
    /// spot check of [SerializedValueOffsets#find] reads 8 bytes at each end of the value.
    @Test
    public void minRecordedLengthMatchesTrieCellData()
    {
        assertEquals(TrieCellData.OFFSET_TTL + 1, SerializedValueOffsets.MIN_RECORDED_LENGTH);
        assertTrue(SerializedValueOffsets.MIN_RECORDED_LENGTH >= Long.BYTES);
    }

    @Test
    public void findDoesNotLookUpValuesShorterThanRecorded()
    {
        byte[] array = new byte[64];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(array, 0, SerializedValueOffsets.MIN_RECORDED_LENGTH - 1, 7);
        locations.record(array, 32, SerializedValueOffsets.MIN_RECORDED_LENGTH, 8);
        locations.finish(new byte[32]);

        assertEquals(-1, locations.find(ByteBuffer.wrap(array, 0, SerializedValueOffsets.MIN_RECORDED_LENGTH - 1)));
        assertEquals(8, locations.find(ByteBuffer.wrap(array, 32, SerializedValueOffsets.MIN_RECORDED_LENGTH)));
    }

    @Test
    public void recordValueKeysAsFind()
    {
        byte[] array = new byte[32];
        ByteBuffer slice = ByteBuffer.wrap(new byte[64], 5, 20).slice();
        ByteBuffer direct = ByteBuffer.allocateDirect(24);
        direct.position(4);
        ByteBuffer readOnly = ByteBuffer.wrap(new byte[40]).asReadOnlyBuffer();
        readOnly.position(10);
        assertFalse(readOnly.hasArray());

        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.recordValue(array, 32, 1);
        locations.recordValue(slice, 20, 2);
        locations.recordValue(direct, 20, 3);
        locations.recordValue(readOnly, 30, 4);
        locations.recordValue("not a value", 11, 5);
        locations.finish(new byte[64]);

        assertEquals(4, locations.size());
        assertEquals(1, locations.find(ByteBuffer.wrap(array)));
        assertEquals(2, locations.find(slice.duplicate()));
        assertEquals(3, locations.find(direct));
        assertEquals(4, locations.find(readOnly));

        // identity-keyed buffers are matched at the position they were recorded at
        direct.position(0).limit(20);
        assertEquals(-1, locations.find(direct));
        readOnly.position(0).limit(30);
        assertEquals(-1, locations.find(readOnly));
    }

    @Test
    public void nothingIsFoundBeforeTheSerializationIsAttached()
    {
        byte[] array = new byte[64];
        SerializedValueOffsets locations = new SerializedValueOffsets();
        locations.record(array, 5, 20, 100);

        assertEquals(-1, locations.find(ByteBuffer.wrap(array, 5, 20)));
        locations.finish(new byte[128]);
        assertEquals(100, locations.find(ByteBuffer.wrap(array, 5, 20)));
    }

    @Test
    public void finishWithoutRecordsIsNull()
    {
        assertNull(new SerializedValueOffsets().finish(new byte[10]));

        SerializedValueOffsets.ENABLED = true;
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, OTHER_TABLE);
        PartitionUpdate.SimpleBuilder update = PartitionUpdate.simpleBuilder(metadata, 1);
        update.row().add("b", randomBytes(SerializedValueOffsets.MIN_RECORDED_LENGTH - 1, 1));
        Mutation shortValues = new Mutation(update.build());
        Mutation.serializer.serializedSize(shortValues, VERSION);   // builds the cached serialization
        assertNull("no value long enough to be recorded", Mutation.serializer.offsetsFor(shortValues, VERSION));
    }

    /// The spot check of [SerializedValueOffsets#find] compares the first and the last 8 bytes of the value with the
    /// bytes at its recorded offset in the cached serialization of the mutation: a value whose bytes differ there is
    /// not found, which is logged at most once a minute.
    ///
    /// The log is rate-limited for the whole JVM, so this test relies on causing the first mismatch in the JVM: it
    /// must stay the only test of the class that causes one (each test class runs in a JVM of its own). A run of
    /// several classes in one JVM, as an IDE may do, can see the message swallowed.
    @Test
    public void findRejectsAValueWhoseSerializedBytesDoNotMatch() throws IOException
    {
        SerializedValueOffsets.ENABLED = true;
        Mutation mutation = sampleMutation();
        byte[] serialized = cachedSerialization(mutation);
        SerializedValueOffsets locations = Mutation.serializer.offsetsFor(mutation, VERSION);
        ByteBuffer value = null;
        for (Cell<?> cell : cells(mutation))
        {
            if (cell.valueSize() == 100)
                value = cell.buffer();
        }
        assertNotNull(value);
        int offset = locations.find(value);
        assertEquals(value, ByteBuffer.wrap(serialized, offset, 100));

        ByteBuffer checked = value;
        List<ILoggingEvent> events = captureLog(SerializedValueOffsets.class, () -> {
            for (int tampered : new int[]{ offset, offset + 7, offset + 92, offset + 99 })
            {
                serialized[tampered] ^= 1;
                assertEquals("byte " + (tampered - offset), -1, locations.find(checked));
                serialized[tampered] ^= 1;
                assertEquals(offset, locations.find(checked));
            }
        });
        assertEquals("four mismatches are logged once", 1, events.size());
        assertEquals(Level.ERROR, events.get(0).getLevel());
        String message = events.get(0).getFormattedMessage();
        assertTrue(message, message.contains("value log fallback metric"));
        assertTrue(message, message.contains("at most once a minute"));
    }

    @Test
    public void serializedCellsResolve() throws IOException
    {
        SerializedValueOffsets.ENABLED = true;
        Mutation mutation = sampleMutation();
        byte[] serialized = serialize(mutation);

        SerializedValueOffsets locations = Mutation.serializer.offsetsFor(mutation, VERSION);
        assertNotNull(locations);
        assertEquals(SAMPLE_RECORDED, assertCellsResolve(mutation, serialized, locations, null));
        assertEquals(SAMPLE_RECORDED, locations.size());

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            assertSame(locations, Mutation.serializer.serializeWithOffsets(mutation, out, VERSION));
            assertArrayEquals(serialized, out.toByteArray());
        }
    }

    @Test
    public void deserializedCellsResolve() throws IOException
    {
        SerializedValueOffsets.ENABLED = true;
        byte[] serialized = serialize(sampleMutation());

        Mutation deserialized = Mutation.serializer.deserialize(new DataInputBuffer(serialized), VERSION, DeserializationHelper.Flag.FROM_REMOTE);
        SerializedValueOffsets locations = Mutation.serializer.offsetsFor(deserialized, VERSION);
        assertNotNull(locations);
        assertArrayEquals(serialized, serialize(deserialized));
        // FROM_REMOTE replaces a counter context with local shards by a cleared copy, whose bytes are not in the
        // serialization; it was recorded as read, but the cell's final value must not resolve
        assertEquals(SAMPLE_RECORDED - 1, assertCellsResolve(deserialized, serialized, locations, "n_local"));
        assertEquals(SAMPLE_RECORDED, locations.size());

        // An ArrayCell wraps its array in a new buffer on every call; each one must resolve.
        for (Cell<?> cell : cells(deserialized))
        {
            if (cell.valueSize() < SerializedValueOffsets.MIN_RECORDED_LENGTH || cell.column().name.toString().equals("n_local"))
                continue;
            ByteBuffer first = cell.buffer();
            ByteBuffer second = cell.buffer();
            assertNotSame(first, second);
            assertTrue(locations.find(first) >= 0);
            assertEquals(locations.find(first), locations.find(second));
        }
    }

    @Test
    public void notRecordedWhenDisabled() throws IOException
    {
        Mutation mutation = sampleMutation();
        byte[] serialized = serialize(mutation);
        assertNull(Mutation.serializer.offsetsFor(mutation, VERSION));

        Mutation deserialized = Mutation.serializer.deserialize(new DataInputBuffer(serialized), VERSION);
        assertNull(Mutation.serializer.offsetsFor(deserialized, VERSION));
    }

    /// Checks that every cell value of at least [SerializedValueOffsets#MIN_RECORDED_LENGTH] bytes, except the one of
    /// column `unresolved`, is found at the bytes equal to it in `serialized`, and that no other value is found.
    /// Returns the number found.
    private static int assertCellsResolve(Mutation mutation, byte[] serialized, SerializedValueOffsets locations, String unresolved)
    {
        int found = 0;
        int notFound = 0;
        for (Cell<?> cell : cells(mutation))
        {
            String name = cell.column().name.toString();
            ByteBuffer value = cell.buffer();
            int length = value.remaining();
            if (length >= SerializedValueOffsets.MIN_RECORDED_LENGTH && !name.equals(unresolved))
            {
                int offset = locations.find(value);
                assertTrue(name + " not found", offset >= 0);
                assertEquals(name, value, ByteBuffer.wrap(serialized, offset, length));
                ++found;
            }
            else
            {
                assertEquals(name, -1, locations.find(value));
                ++notFound;
            }
        }
        assertEquals(SAMPLE_CELLS, found + notFound);
        return found;
    }

    private static List<Cell<?>> cells(Mutation mutation)
    {
        List<Cell<?>> cells = new ArrayList<>();
        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            for (Cell<?> cell : update.staticRow().cells())
                cells.add(cell);
            for (Row row : update.rows())
                for (Cell<?> cell : row.cells())
                    cells.add(cell);
        }
        return cells;
    }

    /// The array of the cached serialization of `mutation`, against which its [SerializedValueOffsets] check their
    /// hits.
    private static byte[] cachedSerialization(Mutation mutation) throws IOException
    {
        byte[][] written = new byte[1][];
        try (DataOutputBuffer out = new DataOutputBuffer()
        {
            @Override
            public void write(byte[] b) throws IOException
            {
                // a cached serialization is written as its whole array
                written[0] = b;
                super.write(b);
            }
        })
        {
            Mutation.serializer.serialize(mutation, out, VERSION);
        }
        assertNotNull(written[0]);
        return written[0];
    }

    private static byte[] serialize(Mutation mutation) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(mutation, out, VERSION);
            return out.toByteArray();
        }
    }

    /// Values of at least [SerializedValueOffsets#MIN_RECORDED_LENGTH] bytes in [#sampleMutation]: st, v (100, 17, 30
    /// and 40 bytes), the long s, vec (fixed length), one map value, b, n_local and n_global. Not recorded: both w, the
    /// short s, v of 16 bytes and empty, and the short map value.
    private static final int SAMPLE_RECORDED = 11;
    private static final int SAMPLE_CELLS = 17;

    private static Mutation sampleMutation()
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, key);
        PartitionUpdate.SimpleBuilder update = builder.update(metadata);
        update.row()
              .add("st", randomBytes(25, 25));
        update.row(0)
              .add("v", randomBytes(100, 100))
              .add("w", 7)
              .add("s", "a text value long enough to be recorded")
              .add("vec", randomBytes(32, 32))
              .add("m", ImmutableMap.of(1, randomBytes(20, 20), 2, randomBytes(10, 10)));
        update.row(1)
              .add("v", randomBytes(17, 17))
              .add("s", "short");
        update.row(2)
              .add("v", randomBytes(16, 16));
        update.row(3)
              .add("v", ByteBufferUtil.EMPTY_BYTE_BUFFER);
        // expiring row: the cell uses the row's TTL
        update.row(4)
              .ttl(1000)
              .add("v", randomBytes(30, 30));
        // expiring cell in a row that does not expire: the cell carries its own TTL
        update.row(5)
              .add("w", 3)
              .ttl(1000)
              .add("v", randomBytes(40, 40));
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, OTHER_TABLE))
               .row()
               .add("b", randomBytes(50, 50));

        Mutation.PartitionUpdateCollector collector = new Mutation.PartitionUpdateCollector(KEYSPACE, key);
        for (PartitionUpdate built : builder.build().getPartitionUpdates())
            collector.add(built);
        collector.add(counterUpdate(key));
        return collector.build();
    }

    /// A counter row with a context holding a local shard (n_local), which a remote replica clears, and a global-only
    /// one (n_global), which it keeps as is.
    private static PartitionUpdate counterUpdate(DecoratedKey key)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, COUNTER_TABLE);
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(Clustering.make(Int32Type.instance.decompose(0)));
        row.addCell(BufferCell.live(metadata.getColumn(ByteBufferUtil.bytes("n_local")), 1, CounterContext.instance().createUpdate(5)));
        row.addCell(BufferCell.live(metadata.getColumn(ByteBufferUtil.bytes("n_global")), 1, CounterContext.instance().createGlobal(CounterId.fromInt(1), 1, 5)));
        return PartitionUpdate.singleRowUpdate(metadata, key, row.build());
    }
}
