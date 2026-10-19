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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.Util.randomBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommitLogValueLocationsTest
{
    private static final String KEYSPACE = "CommitLogValueLocationsTest";
    private static final String TABLE = "Standard1";
    private static final String SKIPPING_TABLE = "SkipsCommitLog";

    /// A skiplist memtable whose writes skip the commit log.
    private static final Memtable.Factory SKIPPING_FACTORY = new Memtable.Factory()
    {
        @Override
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Memtable.Owner owner)
        {
            return SkipListMemtable.FACTORY.create(commitLogLowerBound, metadataRef, owner);
        }

        @Override
        public boolean writesShouldSkipCommitLog()
        {
            return true;
        }
    };

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE, 0, AsciiType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE, SKIPPING_TABLE, 0, AsciiType.instance, BytesType.instance)
                                                .memtable(MemtableParams.forTesting(SKIPPING_FACTORY, "skiplist")));
    }

    @Before
    public void enableRecording() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        SerializedValueOffsets.ENABLED = true;
    }

    @After
    public void disableRecording()
    {
        SerializedValueOffsets.ENABLED = false;
    }

    /// Whether the segments are memory-mapped, the only ones for which value locations are produced. Compressed and
    /// encrypted segments (e.g. the test-compression variant) never are.
    private static boolean mappedSegments()
    {
        CommitLog.Configuration configuration = CommitLog.instance.configuration;
        return !configuration.useCompression()
               && !configuration.useEncryption()
               && configuration.diskAccessMode == Config.DiskAccessMode.mmap;
    }

    @Test
    public void largeValueResolvesToSegmentBytes() throws IOException
    {
        Mutation mutation = mutation(TABLE, 1024);
        CommitLog.Added added = CommitLog.instance.addWithLocations(mutation);
        assertNotNull(added.position);

        CommitLogValueLocations locations = added.locations;
        if (!mappedSegments())
        {
            assertNull(locations);
            return;
        }

        assertNotNull(locations);
        int version = StorageCompatibilityMode.current().storageMessagingVersion();
        assertEquals("the one value is recorded", 1, Mutation.serializer.offsetsFor(mutation, version).size());
        assertEquals(added.position.segmentId, locations.segmentId());
        assertEquals(DatabaseDescriptor.getCommitLogSegmentSize(), locations.segmentSize());
        assertTrue(locations.segmentFile().exists());

        // The entry is [int size][int crc][mutation][int crc] and the position after the write is the end of the entry.
        int serializedSize = (int) Mutation.serializer.serializedSize(mutation, version);
        long entryStart = added.position.position - (CommitLogSegment.ENTRY_OVERHEAD_SIZE - CommitLogSegment.ENTRY_HEADER_SIZE) - serializedSize;
        ByteBuffer value = value(mutation);
        long offset = locations.absoluteOffsetOf(value);
        assertTrue(offset > entryStart);
        assertTrue(offset + value.remaining() <= entryStart + serializedSize);

        CommitLog.instance.sync(true);
        try (RandomAccessFile file = new RandomAccessFile(locations.segmentFile().toJavaIOFile(), "r"))
        {
            file.seek(entryStart - CommitLogSegment.ENTRY_HEADER_SIZE);
            assertEquals(serializedSize, file.readInt());

            byte[] bytes = new byte[value.remaining()];
            file.seek(offset);
            file.readFully(bytes);
            assertEquals(value, ByteBuffer.wrap(bytes));
        }
    }

    @Test
    public void smallValuesRecordNothing()
    {
        CommitLog.Added added = CommitLog.instance.addWithLocations(mutation(TABLE, SerializedValueOffsets.MIN_RECORDED_LENGTH - 1));
        assertNotNull(added.position);
        assertNull("no locations for an entry without recorded values", added.locations);
    }

    @Test
    public void uncachedSerializationHasNoLocations()
    {
        // Larger than CACHEABLE_MUTATION_SIZE_LIMIT, so the mutation is serialized without a cached form.
        Mutation mutation = mutation(TABLE, 1_500_000);
        CommitLog.Added added = CommitLog.instance.addWithLocations(mutation);
        assertNotNull(added.position);
        assertNull(added.locations);
    }

    @Test
    public void writeContextCarriesLocations()
    {
        Mutation mutation = mutation(TABLE, 1024);
        try (WriteContext context = Keyspace.open(KEYSPACE).getWriteHandler().beginWrite(mutation, WriteOptions.DEFAULT))
        {
            CassandraWriteContext cassandraContext = CassandraWriteContext.fromContext(context);
            CommitLogValueLocations locations = cassandraContext.valueLocations();
            assertNotNull(cassandraContext.getPosition());
            if (mappedSegments())
            {
                assertNotNull(locations);
                assertEquals(cassandraContext.getPosition().segmentId, locations.segmentId());
                assertTrue(locations.absoluteOffsetOf(value(mutation)) >= 0);
            }
            else
            {
                assertNull(locations);
            }
        }

        try (WriteContext context = Keyspace.open(KEYSPACE).getWriteHandler().beginWrite(mutation(TABLE, 1024), WriteOptions.DEFAULT_WITHOUT_COMMITLOG))
        {
            CassandraWriteContext cassandraContext = CassandraWriteContext.fromContext(context);
            assertNull(cassandraContext.getPosition());
            assertNull(cassandraContext.valueLocations());
        }

        SerializedValueOffsets.ENABLED = false;
        try (WriteContext context = Keyspace.open(KEYSPACE).getWriteHandler().beginWrite(mutation(TABLE, 1024), WriteOptions.DEFAULT))
        {
            CassandraWriteContext cassandraContext = CassandraWriteContext.fromContext(context);
            assertNotNull(cassandraContext.getPosition());
            assertNull(cassandraContext.valueLocations());
        }
    }

    @Test
    public void writeContextOmitsTablesSkippingCommitLog()
    {
        Mutation logged = mutation(TABLE, 1024);
        Mutation skipped = mutation(SKIPPING_TABLE, 2048);
        Mutation.PartitionUpdateCollector collector = new Mutation.PartitionUpdateCollector(KEYSPACE, logged.key());
        for (PartitionUpdate update : logged.getPartitionUpdates())
            collector.add(update);
        for (PartitionUpdate update : skipped.getPartitionUpdates())
            collector.add(update);
        Mutation mutation = collector.build();

        try (WriteContext context = Keyspace.open(KEYSPACE).getWriteHandler().beginWrite(mutation, WriteOptions.DEFAULT))
        {
            CassandraWriteContext cassandraContext = CassandraWriteContext.fromContext(context);
            assertNotNull(cassandraContext.getPosition());
            CommitLogValueLocations locations = cassandraContext.valueLocations();
            if (!mappedSegments())
            {
                assertNull(locations);
                return;
            }
            // the commit log gets the mutation without the skipping table's update, a new mutation with the same cells:
            // the value of the logged update is recorded, the larger one of the skipped update is not
            assertNotNull(locations);
            assertTrue(locations.absoluteOffsetOf(value(logged)) >= 0);
            assertEquals(-1L, locations.absoluteOffsetOf(value(skipped)));
        }
    }

    private static Mutation mutation(String table, int valueLength)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
        return new RowUpdateBuilder(metadata, 0, "key")
               .clustering("c")
               .add("val", randomBytes(valueLength, valueLength))
               .build();
    }

    private static ByteBuffer value(Mutation mutation)
    {
        Row row = mutation.getPartitionUpdates().iterator().next().rows().iterator().next();
        Cell<?> cell = row.cells().iterator().next();
        return cell.buffer();
    }
}
