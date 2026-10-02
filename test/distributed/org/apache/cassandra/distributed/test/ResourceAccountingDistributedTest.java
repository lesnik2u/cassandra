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

package org.apache.cassandra.distributed.test;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Two healthy nodes, with LZ4 internode compression, writing and reading rows larger than a frame, repairing and
 * streaming, and serving those rows to a native protocol v5 client that asked for LZ4, report nothing from the
 * resource accounting check run every few milliseconds by the watchdog. As in
 * {@code ResourceAccountingNoFalsePositiveTest}, an invariant must fail for {@link #PERSIST_MILLIS} of wall time to be
 * reported, as with the default persistence, 2 checks at 5 s, it must fail for 5 s.
 * <p>
 * A full LZ4 frame of the native protocol asks the networking buffer pool for 12 + maxCompressedLength(131060) = 131601
 * bytes, over a pooled chunk's 128 KiB, so the frame is unpooled, and is then trimmed to its compressed size. Each
 * node's networking pool must count a miss while the client reads, which shows that both nodes took that path.
 * Internode frames are smaller, at most 64 KiB for small messages and 32 KiB for large ones, so they are unpooled only
 * once the pool is full. Until {@code BufferPool}'s {@code LocalPool.putUnusedPortion} leaves the overflow count alone
 * when trimming an unpooled buffer, each such frame takes what it trims off that count for good, and this test reports
 * {@code buffer-pool.networking.overflow}.
 */
public class ResourceAccountingDistributedTest extends TestBaseImpl
{
    private static final long CHECK_INTERVAL_MILLIS = 5;
    // the watchdog's checks run with a fixed delay, so the 1st and the last are at least this far apart
    private static final long PERSIST_MILLIS = 250;
    private static final int PERSIST_CHECKS = (int) (PERSIST_MILLIS / CHECK_INTERVAL_MILLIS) + 1;
    private static final int ROWS = 40;
    private static final int VALUE_CHARS = 300 << 10;   // over a frame's 128 KiB

    @Test
    public void testBusyClusterReportsNothing() throws Throwable
    {
        try (WithProperties properties = new WithProperties();
             Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                       .set("internode_compression", "all"))
                                           .start()))
        {
            // read by each node's watchdog when it starts
            properties.set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, CHECK_INTERVAL_MILLIS)
                      .set(RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS, PERSIST_CHECKS)
                      .set(RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS, PERSIST_CHECKS);
            try
            {
                for (IInvokableInstance node : cluster)
                {
                    node.runOnInstance(() -> ExecutorLivenessWatchdog.start());
                    assertEquals(1, node.logs().grep("Resource accounting check started: checking every " +
                                                     CHECK_INTERVAL_MILLIS + "ms").getResult().size());
                }

                cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, " +
                                                  "PRIMARY KEY (pk, ck))"));
                Random random = new Random(0);

                // writes and reads of every replica, of rows over a frame
                for (int pk = 0; pk < ROWS; pk++)
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, 0, ?)"),
                                                   ConsistencyLevel.ALL, pk, value(random));
                for (int pk = 0; pk < ROWS; pk++)
                    cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                                                   ConsistencyLevel.ALL, pk);

                // rows only node 1 has, which a full repair streams to node 2
                for (int pk = 0; pk < ROWS; pk++)
                    cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, 1, ?)"),
                                                   pk, value(random));
                cluster.forEach(node -> node.flush(KEYSPACE));
                cluster.get(1).nodetoolResult("repair", "--full", KEYSPACE).asserts().success();
                assertEquals(2L * ROWS,
                             cluster.get(2).executeInternal(withKeyspace("SELECT count(*) FROM %s.tbl"))[0][0]);

                // responses over a frame to a client that asked for LZ4, coordinated by both nodes
                long[] misses = new long[cluster.size()];
                for (int i = 0; i < misses.length; i++)
                    misses[i] = networkingMisses(cluster.get(i + 1));
                com.datastax.driver.core.Cluster.Builder builder =
                    com.datastax.driver.core.Cluster.builder()
                                                    .addContactPoint("127.0.0.1")
                                                    .withProtocolVersion(ProtocolVersion.V5)
                                                    .withCompression(ProtocolOptions.Compression.LZ4);
                try (com.datastax.driver.core.Cluster client = builder.build(); Session session = client.connect())
                {
                    String select = withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?");
                    for (int pk = 0; pk < ROWS; pk++)
                        assertEquals(2, session.execute(select, pk).all().size());
                }
                // each node asked its networking pool for more than a pooled chunk, so for an unpooled buffer
                for (int i = 0; i < misses.length; i++)
                    assertTrue("node " + (i + 1) + "'s networking buffer pool counted no miss",
                               networkingMisses(cluster.get(i + 1)) > misses[i]);

                // long enough for a failure that lasts to be reported
                Thread.sleep(8 * PERSIST_MILLIS);
                for (IInvokableInstance node : cluster)
                    assertEquals(List.of(), node.logs().grep("Resource accounting(:| check failed)").getResult());
            }
            finally
            {
                for (IInvokableInstance node : cluster)
                    node.runOnInstance(() -> ExecutorLivenessWatchdog.stop());
            }
        }
    }

    private static long networkingMisses(IInvokableInstance node)
    {
        String name = "org.apache.cassandra.metrics.BufferPool.Misses.networking";
        return node.callOnInstance(() -> CassandraMetricsRegistry.Metrics.getMeters().get(name).getCount());
    }

    private static String value(Random random)
    {
        StringBuilder value = new StringBuilder(VALUE_CHARS);
        while (value.length() < VALUE_CHARS)
            value.append((char) ('a' + random.nextInt(26)));
        return value.toString();
    }
}
