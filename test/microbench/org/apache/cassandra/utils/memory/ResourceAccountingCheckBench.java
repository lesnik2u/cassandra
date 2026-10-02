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
package org.apache.cassandra.utils.memory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.BufferPoolBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.BufferPoolSource;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.ChunkCacheBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.Finding;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.MemtablePoolBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.SubPoolBalance;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Cost of one {@link ResourceAccountingCheck} check, without the logging.
 * <ul>
 * <li>{@code node}: the node's own sources, as the watchdog runs it: the memtable pool, the two global buffer pools
 * and the chunk cache, all healthy.</li>
 * <li>{@code healthy}: fake sources, all healthy, so the cost of the check itself.</li>
 * <li>{@code failing}: fake sources, every invariant failing at every check, the negative-release count growing,
 * each already reported, so this check reports nothing: the state for the report interval after a report.</li>
 * <li>{@code reporting}: as failing, but with no report interval, so every check reports every invariant: the cost
 * of the check that reports.</li>
 * </ul>
 *
 * Run in-process (forked JMH cannot reach the host on this machine):
 *   jmh-profile.sh {@code <repo>} ResourceAccountingCheckBench -- -prof gc
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(0)
@Threads(1)
@State(Scope.Benchmark)
public class ResourceAccountingCheckBench
{
    @Param({ "node", "healthy", "failing", "reporting" })
    public String scenario;

    private ResourceAccountingCheck check;
    private long negativeReleases;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        if ("node".equals(scenario))
        {
            check = ResourceAccountingCheck.forNode(2, 12, SECONDS.toNanos(600));
        }
        else
        {
            boolean failing = !"healthy".equals(scenario);
            long reportIntervalNanos = "reporting".equals(scenario) ? 0 : HOURS.toNanos(1);
            SubPoolBalance subPool = failing ? new SubPoolBalance(-1, -1, 1 << 30) : new SubPoolBalance(1 << 20, 0, 1 << 30);
            BufferPoolBalance bufferPool = failing ? new BufferPoolBalance(-1, -2, -1) : new BufferPoolBalance(1 << 20, 2 << 20, 0);
            ChunkCacheBalance chunkCache = failing ? new ChunkCacheBalance(1L << 40, 1 << 30) : new ChunkCacheBalance(1 << 20, 1 << 30);
            MemtablePoolBalance memtablePool = new MemtablePoolBalance(subPool, subPool);
            check = new ResourceAccountingCheck(() -> memtablePool,
                                                () -> failing ? ++negativeReleases : 0L,
                                                List.of(new BufferPoolSource("chunk-cache", () -> bufferPool),
                                                        new BufferPoolSource("networking", () -> bufferPool)),
                                                () -> chunkCache,
                                                2, 2, reportIntervalNanos);
        }
        // past every persistence requirement, so a failing invariant is reported or held back by its rate limit
        for (int i = 0; i < 3; i++)
            check();
        System.out.printf("%n%s: %d findings per check%n", scenario, check().size());
    }

    @Benchmark
    public List<Finding> check()
    {
        return check.check(preciseTime.now());
    }
}
