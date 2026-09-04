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

package org.apache.cassandra.test.microbench.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Measures what a {@link PartitionUpdate} costs after it is built — the sizing and writing the write path does with
 * it, and the reading a local apply does — for the three ways a write can be assembled.
 *
 * The schema is the one {@link PartitionUpdateBuildBench} uses: a bigint partition key, a single high-entropy
 * bigint clustering and one 100-byte text value.
 *
 * The axes:
 *
 * <ul>
 * <li>{@code format} — {@code btree} and {@code trie} are the two update implementations; {@code trie_inbuffer} is
 *     the trie one with {@link TriePartitionUpdate#IN_BUFFER_ON_BUILD} on, which lays the trie out in the on-disk
 *     encoding at {@code build()} and backs the update with those bytes. The two trie arms differ in nothing
 *     else.</li>
 * <li>{@code rows} — {@code 1} is the shape of the workload the format is tuned for; 10 shows the per-row
 *     scaling.</li>
 * <li>{@code serializations} — how many times the update is sized and written: 1 is a commit-log-only write, 3 is
 *     the commit log plus two replicas. It is what separates the two trie arms, since the in-memory one lays the
 *     trie out again for every write and the in-buffer one lays it out once, at build. It does not apply to
 *     {@link #buildAndRead}, which runs once per arm regardless.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Thread)
public class PartitionUpdateWritePathBench
{
    static
    {
        DatabaseDescriptor.toolInitialization();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final String KEYSPACE = "keyspace_pu_write_path_bench";
    private static final int VALUE_SIZE = 100;
    private static final long TIMESTAMP = 1234567890L;
    private static final long NOW_IN_SEC = 1600000000L;

    @Param({ "btree", "trie", "trie_inbuffer" })
    String format;

    @Param({ "1", "10" })
    int rows;

    @Param({ "1", "3" })
    int serializations;

    private PartitionUpdate.Factory factory;
    private TableMetadata metadata;
    private DecoratedKey key;
    private ColumnMetadata valueColumn;
    private int version;

    /** Clusterings of the rows to add, in comparator order. */
    private Clustering<?>[] clusterings;
    /** The cells to add, one per row, paired with {@link #clusterings}. */
    private Cell<?>[] cells;
    /** The same rows, built once in setup: this benchmark prices what happens after the update is assembled. */
    private Row[] prebuiltRows;
    /** Reused across the writes of one invocation, as the write path reuses one buffer per thread. */
    private DataOutputBuffer out;

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1)), false);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        metadata = CreateTableStatement.parse("CREATE TABLE blobs " +
                                              "( key bigint," +
                                              "ckey bigint," +
                                              "val text, " +
                                              "PRIMARY KEY(key, ckey));", KEYSPACE)
                                       .build();
        SchemaTestUtil.addOrUpdateKeyspace(ksm.withSwapped(ksm.tables.with(metadata)), false);

        factory = isTrie() ? new TriePartitionUpdate.TrieFactory()
                           : new BTreePartitionUpdate.BTreeFactory();
        version = MessagingService.current_version;

        key = metadata.partitioner.decorateKey(LongType.instance.decompose(1L));
        valueColumn = metadata.getColumn(org.apache.cassandra.cql3.ColumnIdentifier.getInterned("val", false));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < VALUE_SIZE; ++i)
            sb.append((char) ('a' + i % 26));
        ByteBuffer value = UTF8Type.instance.decompose(sb.toString());

        clusterings = new Clustering<?>[rows];
        cells = new Cell<?>[rows];
        for (int i = 0; i < rows; ++i)
        {
            // High-entropy clusterings: the workload's clustering values are unrelated bigints.
            long ckey = i * 0x9E3779B97F4A7C15L;
            clusterings[i] = metadata.comparator.make(ckey);
            cells[i] = BufferCell.live(valueColumn, TIMESTAMP, value);
        }
        // Rows are added in comparator order in the write path; do the same here.
        sortByClustering();

        prebuiltRows = new Row[rows];
        for (int i = 0; i < rows; ++i)
            prebuiltRows[i] = buildRow(i);

        out = new DataOutputBuffer();

        checkArms();

        // Set the arm last: everything above must not depend on which one is being measured.
        TriePartitionUpdate.IN_BUFFER_ON_BUILD = "trie_inbuffer".equals(format);
    }

    /**
     * The arms must not silently collapse into each other, and the two trie ones must produce the same update: if
     * building in a buffer changed what is written, the numbers below would be comparing two different things.
     */
    private void checkArms() throws IOException
    {
        if (version < MessagingService.VERSION_DS_21)
            throw new IllegalStateException("The trie update format is only written from VERSION_DS_21 on; " +
                                            "messaging version is " + version);

        boolean wantInBuffer = "trie_inbuffer".equals(format);
        TriePartitionUpdate.IN_BUFFER_ON_BUILD = wantInBuffer;
        PartitionUpdate update = buildUpdate();
        if (!expectedType().isInstance(update))
            throw new IllegalStateException("format=" + format + " produced " + update.getClass().getName());
        if (update.rowCount() != rows)
            throw new IllegalStateException("built " + update.rowCount() + " rows, expected " + rows);

        if (isTrie())
        {
            boolean isInBuffer = !(((TriePartitionUpdate) update).trie() instanceof InMemoryDeletionAwareTrie);
            if (isInBuffer != wantInBuffer)
                throw new IllegalStateException("format=" + format + " produced an update backed by " +
                                                ((TriePartitionUpdate) update).trie().getClass().getName());

            TriePartitionUpdate.IN_BUFFER_ON_BUILD = false;
            byte[] fromTrie = serializeOnce(buildUpdate());
            TriePartitionUpdate.IN_BUFFER_ON_BUILD = true;
            byte[] fromBuffer = serializeOnce(buildUpdate());
            if (!Arrays.equals(fromTrie, fromBuffer))
                throw new IllegalStateException("the two trie arms serialize to different bytes: " +
                                                fromTrie.length + " vs " + fromBuffer.length + " bytes");
        }
    }

    private byte[] serializeOnce(PartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, buffer, version);
            return buffer.toByteArray();
        }
    }

    private boolean isTrie()
    {
        return !"btree".equals(format);
    }

    private void sortByClustering()
    {
        for (int i = 1; i < rows; ++i)
        {
            Clustering<?> c = clusterings[i];
            Cell<?> cell = cells[i];
            int j = i - 1;
            while (j >= 0 && metadata.comparator.compare(clusterings[j], c) > 0)
            {
                clusterings[j + 1] = clusterings[j];
                cells[j + 1] = cells[j];
                --j;
            }
            clusterings[j + 1] = c;
            cells[j + 1] = cell;
        }
    }

    private Class<? extends PartitionUpdate> expectedType()
    {
        return isTrie() ? TriePartitionUpdate.class : BTreePartitionUpdate.class;
    }

    private Row buildRow(int i)
    {
        Row.Builder rowBuilder = factory.rowBuilder(metadata.regularColumns(), false);
        rowBuilder.newRow(clusterings[i]);
        rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(TIMESTAMP, NOW_IN_SEC));
        rowBuilder.addCell(cells[i]);
        return rowBuilder.build();
    }

    private PartitionUpdate buildUpdate()
    {
        PartitionUpdate.Builder builder = factory.builder(metadata, key, metadata.regularAndStaticColumns(), rows);
        for (int i = 0; i < rows; ++i)
            builder.add(prebuiltRows[i]);
        return builder.build();
    }

    /**
     * Build the update and then size and write it {@code serializations} times, as
     * {@link org.apache.cassandra.db.Mutation} does for the commit log and once more per replica. Sizing reserves
     * the region the write fills, so the two always come in that order.
     */
    @Benchmark
    public long buildSizeAndSerialize() throws IOException
    {
        PartitionUpdate update = buildUpdate();
        long size = 0;
        for (int i = 0; i < serializations; ++i)
        {
            size += PartitionUpdate.serializer.serializedSize(update, version);
            out.clear();
            PartitionUpdate.serializer.serialize(update, out, version);
            size += out.getLength();
        }
        return size;
    }

    /**
     * Build the update and then read all of it, the way a local apply walks it into the memtable. This is the side
     * the in-buffer arm pays for: its rows and cells are decoded out of the layout rather than dereferenced.
     */
    @Benchmark
    public long buildAndRead()
    {
        PartitionUpdate update = buildUpdate();
        long sum = 0;
        for (ColumnData cd : update.staticRow())
            sum += cd.dataSize();
        for (Row row : update.rows())
        {
            sum += row.primaryKeyLivenessInfo().timestamp();
            for (ColumnData cd : row)
                sum += cd.dataSize();
        }
        return sum;
    }

    public static void main(String... args) throws RunnerException
    {
        new Runner(new OptionsBuilder().include(PartitionUpdateWritePathBench.class.getSimpleName())
                                       .build()).run();
    }
}
