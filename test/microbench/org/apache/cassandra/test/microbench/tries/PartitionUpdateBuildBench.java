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

import java.nio.ByteBuffer;
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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
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
 * Measures the cost of assembling a {@link PartitionUpdate} — what
 * {@code ModificationStatement.getMutations} pays per mutation, before anything is serialized or
 * applied.
 *
 * The schema mirrors the write workload the trie update format is tuned for: a bigint partition
 * key, a single high-entropy bigint clustering (so no two clusterings share a prefix and there is
 * nothing for the trie to amortize) and one 100-byte text value.
 *
 * The axes:
 *
 * <ul>
 * <li>{@code format} — whether rows and update are built through
 *     {@link TriePartitionUpdate.TrieFactory} or {@link BTreePartitionUpdate.BTreeFactory}. The
 *     factory is selected explicitly rather than through table params, so the arm cannot silently
 *     measure the other implementation.</li>
 * <li>{@code rows} — {@code 1} is the case that matters: every mutation of the workload above is a
 *     single-row insert. 10 and 100 are there to show how the per-row cost scales.</li>
 * </ul>
 *
 * There is no messaging-version axis: building an update does not depend on it.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Thread)
public class PartitionUpdateBuildBench
{
    static
    {
        DatabaseDescriptor.toolInitialization();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final String KEYSPACE = "keyspace_pu_build_bench";
    private static final int VALUE_SIZE = 100;
    private static final long TIMESTAMP = 1234567890L;
    private static final long NOW_IN_SEC = 1600000000L;

    @Param({ "btree", "trie" })
    String format;

    @Param({ "1", "10", "100" })
    int rows;

    private PartitionUpdate.Factory factory;
    private TableMetadata metadata;
    private DecoratedKey key;
    private ColumnMetadata valueColumn;

    /** Clusterings of the rows to add, in comparator order. */
    private Clustering<?>[] clusterings;
    /** The cells to add, one per row, paired with {@link #clusterings}. */
    private Cell<?>[] cells;
    /** The same rows, built once in setup, for the arm that prices the update alone. */
    private Row[] prebuiltRows;

    @Setup(Level.Trial)
    public void setup()
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

        factory = "trie".equals(format) ? new TriePartitionUpdate.TrieFactory()
                                        : new BTreePartitionUpdate.BTreeFactory();

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

        PartitionUpdate update = buildUpdate();
        if (!expectedType().isInstance(update))
            throw new IllegalStateException("format=" + format + " produced " + update.getClass().getName());
        if (update.rowCount() != rows)
            throw new IllegalStateException("built " + update.rowCount() + " rows, expected " + rows);
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
        return "trie".equals(format) ? TriePartitionUpdate.class : BTreePartitionUpdate.class;
    }

    private Row buildRow(int i)
    {
        Row.Builder rowBuilder = factory.rowBuilder(metadata.regularColumns(), false);
        rowBuilder.newRow(clusterings[i]);
        rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(TIMESTAMP, NOW_IN_SEC));
        rowBuilder.addCell(cells[i]);
        return rowBuilder.build();
    }

    /**
     * The update builder alone: construction, one graft per row, and {@code build()}. This is the
     * part {@code TriePartitionUpdate.Builder} owns.
     */
    @Benchmark
    public PartitionUpdate buildUpdate()
    {
        PartitionUpdate.Builder builder = factory.builder(metadata, key, metadata.regularAndStaticColumns(), rows);
        for (int i = 0; i < rows; ++i)
            builder.add(prebuiltRows[i]);
        return builder.build();
    }

    /**
     * Rows and update together — what the write path actually performs, since
     * {@code UpdateParameters} builds each row through the same factory immediately before it is
     * added.
     */
    @Benchmark
    public PartitionUpdate buildRowsAndUpdate()
    {
        PartitionUpdate.Builder builder = factory.builder(metadata, key, metadata.regularAndStaticColumns(), rows);
        for (int i = 0; i < rows; ++i)
            builder.add(buildRow(i));
        return builder.build();
    }

    public static void main(String... args) throws RunnerException
    {
        new Runner(new OptionsBuilder().include(PartitionUpdateBuildBench.class.getSimpleName())
                                       .build()).run();
    }
}
