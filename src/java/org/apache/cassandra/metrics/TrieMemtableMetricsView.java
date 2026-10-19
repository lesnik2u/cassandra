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

package org.apache.cassandra.metrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TrieMemtableMetricsView
{
    private static final String UNCONTENDED_PUTS = "Uncontended memtable puts";
    private static final String CONTENDED_PUTS = "Contended memtable puts";
    private static final String CONTENTION_TIME = "Contention time";
    private static final String LAST_FLUSH_SHARD_SIZES = "Shard sizes during last flush";
    private static final String VALUE_LOG_BYTES = "Value log bytes";
    private static final String VALUE_LOG_FALLBACK_BYTES = "Value log fallback bytes";

    private static final Map<String, TrieMemtableMetricsView> perTableMetrics = new ConcurrentHashMap<>();

    // the number of memtable puts that did not need to wait on write lock
    public final Counter uncontendedPuts;

    // the number of memtable puts that needed to wait on write lock
    public final Counter contendedPuts;

    // shard put contention measurements
    public final LatencyMetrics contentionTime;

    // shard sizes distribution
    public final MinMaxAvgMetric lastFlushShardDataSizes;

    // value bytes written outside memtable memory, summed over this table's memtables that keep values there and
    // have not been discarded yet, including those switched out and waiting for or being flushed
    public final Gauge<Long> valueLogBytes;

    // the part of valueLogBytes copied by commitlog value storage because the values could not be referenced in the
    // commit log, summed over the same memtables
    public final Gauge<Long> valueLogFallbackBytes;

    private final Set<ValueLogSource> valueLogSources = ConcurrentHashMap.newKeySet();

    private final TrieMemtableMetricNameFactory factory;
    private final String keyspace;
    private final String table;

    public static TrieMemtableMetricsView getOrCreate(String keyspace, String table)
    {
        return perTableMetrics.computeIfAbsent(getKey(keyspace, table), k -> new TrieMemtableMetricsView(keyspace, table));
    }

    private TrieMemtableMetricsView(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
        factory = new TrieMemtableMetricNameFactory(keyspace, table);
        
        uncontendedPuts = Metrics.counter(factory.createMetricName(UNCONTENDED_PUTS));
        contendedPuts = Metrics.counter(factory.createMetricName(CONTENDED_PUTS));
        contentionTime = new LatencyMetrics(factory, CONTENTION_TIME);
        lastFlushShardDataSizes = new MinMaxAvgMetric(factory, LAST_FLUSH_SHARD_SIZES);
        valueLogBytes = Metrics.register(factory.createMetricName(VALUE_LOG_BYTES), this::sumValueLogBytes);
        valueLogFallbackBytes = Metrics.register(factory.createMetricName(VALUE_LOG_FALLBACK_BYTES), this::sumValueLogFallbackBytes);
    }

    /// Include the given memtable in [#valueLogBytes] and [#valueLogFallbackBytes] until [#removeValueLogSource] is
    /// called.
    public void addValueLogSource(ValueLogSource source)
    {
        valueLogSources.add(source);
    }

    public void removeValueLogSource(ValueLogSource source)
    {
        valueLogSources.remove(source);
    }

    private long sumValueLogBytes()
    {
        long total = 0;
        for (ValueLogSource source : valueLogSources)
            total += source.valueLogBytes();
        return total;
    }

    private long sumValueLogFallbackBytes()
    {
        long total = 0;
        for (ValueLogSource source : valueLogSources)
            total += source.valueLogFallbackBytes();
        return total;
    }

    public void release()
    {
        perTableMetrics.remove(getKey(keyspace, table));

        Metrics.remove(factory.createMetricName(UNCONTENDED_PUTS));
        Metrics.remove(factory.createMetricName(CONTENDED_PUTS));
        contentionTime.release();
        lastFlushShardDataSizes.release();
        Metrics.remove(factory.createMetricName(VALUE_LOG_BYTES));
        Metrics.remove(factory.createMetricName(VALUE_LOG_FALLBACK_BYTES));
    }

    /// A memtable that keeps values outside memtable memory, summed by [#valueLogBytes] and [#valueLogFallbackBytes].
    /// Both are read by the gauges from any thread, also after the memtable has been discarded.
    public interface ValueLogSource
    {
        long valueLogBytes();

        long valueLogFallbackBytes();
    }

    static class TrieMemtableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspace;
        private final String table;

        TrieMemtableMetricNameFactory(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            String type = "TrieMemtable";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspace);
            mbeanName.append(",scope=").append(table);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspace + "." + table, mbeanName.toString());
        }
    }

    private static String getKey(String keyspace, String table)
    {
        return keyspace + "." + table;
    }
}
