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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.BufferPoolBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.BufferPoolSource;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.ChunkCacheBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.Finding;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.MemtablePoolBalance;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck.SubPoolBalance;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResourceAccountingCheckTest
{
    private static final long CHECK_INTERVAL = SECONDS.toNanos(5);
    private static final long REPORT_INTERVAL = SECONDS.toNanos(600);
    private static final int PERSIST_CHECKS = 2;
    private static final int DRIFT_CHECKS = 12;
    // an arbitrary precise-clock reading, far from 0 and from overflow
    private static final long START = SECONDS.toNanos(1_000_000);
    private static final long MiB = 1L << 20;

    // the balances the fake sources return; each test makes them fail
    private SubPoolBalance onHeap = new SubPoolBalance(1000, 200, 10_000);
    private SubPoolBalance offHeap = new SubPoolBalance(0, 0, 0);
    private long negativeReleases;
    private BufferPoolBalance chunkCachePool = new BufferPoolBalance(4096, 2 * MiB, 0);
    private BufferPoolBalance networkingPool = new BufferPoolBalance(0, 0, 8192);
    private ChunkCacheBalance chunkCache = new ChunkCacheBalance(400 * MiB, 480 * MiB);

    private long now = START;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private ResourceAccountingCheck check()
    {
        return check(() -> new MemtablePoolBalance(onHeap, offHeap));
    }

    private ResourceAccountingCheck check(Supplier<MemtablePoolBalance> memtablePool)
    {
        return new ResourceAccountingCheck(memtablePool,
                                           () -> negativeReleases,
                                           List.of(new BufferPoolSource("chunk-cache", () -> chunkCachePool),
                                                   new BufferPoolSource("networking", () -> networkingPool)),
                                           () -> chunkCache,
                                           PERSIST_CHECKS, DRIFT_CHECKS, REPORT_INTERVAL);
    }

    // the next check, a check interval after the last
    private List<Finding> next(ResourceAccountingCheck check)
    {
        List<Finding> findings = check.check(now);
        now += CHECK_INTERVAL;
        return findings;
    }

    private static List<String> invariants(List<Finding> findings)
    {
        return findings.stream().map(f -> f.invariant).collect(Collectors.toList());
    }

    @Test
    public void testHealthyNoFinding()
    {
        ResourceAccountingCheck check = check();
        for (int i = 0; i < 20; i++)
            assertEquals(List.of(), next(check));
        assertEquals(0, check.failingCount());
    }

    @Test
    public void testFailingOnceNoFinding()
    {
        ResourceAccountingCheck check = check();
        onHeap = new SubPoolBalance(-1, 0, 10_000);
        assertEquals(List.of(), next(check));
        assertEquals(1, check.failingCount());
        onHeap = new SubPoolBalance(1, 0, 10_000);
        // holding again after an unreported failure is not reported either
        assertEquals(List.of(), next(check));
        assertEquals(0, check.failingCount());
        for (int i = 0; i < 5; i++)
            assertEquals(List.of(), next(check));
    }

    @Test
    public void testPersistentFailureReportedOnce()
    {
        ResourceAccountingCheck check = check();
        onHeap = new SubPoolBalance(-5, 7, 10_000);
        assertEquals(List.of(), next(check));
        List<Finding> findings = next(check);
        assertEquals(1, findings.size());
        Finding finding = findings.get(0);
        assertEquals(ResourceAccountingCheck.ON_HEAP_ALLOCATED, finding.invariant);
        assertEquals(Finding.Kind.IMPOSSIBLE_STATE, finding.kind);
        assertEquals("Resource accounting: impossible state: memtable.on-heap.allocated: allocated -5 is negative " +
                     "(allocated -5, reclaiming 7, limit 10000); failed 2 consecutive checks, the first 5.0s ago",
                     finding.message);

        // still failing: not again within the report interval
        int checks = (int) (REPORT_INTERVAL / CHECK_INTERVAL) - 1;
        for (int i = 0; i < checks; i++)
            assertEquals(List.of(), next(check));

        // then again, with how long it has been failing
        findings = next(check);
        assertEquals(1, findings.size());
        assertEquals("Resource accounting: impossible state: memtable.on-heap.allocated: allocated -5 is negative " +
                     "(allocated -5, reclaiming 7, limit 10000); failed " + (checks + 3) +
                     " consecutive checks, the first 605.0s ago",
                     findings.get(0).message);
    }

    @Test
    public void testRecoveryReportedOnceThenSilence()
    {
        ResourceAccountingCheck check = check();
        offHeap = new SubPoolBalance(10, -3, 100);
        assertEquals(List.of(), next(check));
        assertEquals(List.of(ResourceAccountingCheck.OFF_HEAP_RECLAIMING), invariants(next(check)));
        assertEquals(List.of(), next(check));

        offHeap = new SubPoolBalance(10, 3, 100);
        List<Finding> findings = next(check);
        assertEquals(1, findings.size());
        assertEquals(ResourceAccountingCheck.OFF_HEAP_RECLAIMING, findings.get(0).invariant);
        assertEquals(Finding.Kind.RECOVERED, findings.get(0).kind);
        assertEquals("Resource accounting: memtable.off-heap.reclaiming holds again, after failing 3 consecutive " +
                     "checks over 15.0s", findings.get(0).message);
        for (int i = 0; i < 5; i++)
            assertEquals(List.of(), next(check));
        assertEquals(0, check.failingCount());
    }

    @Test
    public void testFailingAgainAfterRecoveryIsRateLimited()
    {
        ResourceAccountingCheck check = check();
        onHeap = new SubPoolBalance(-1, 0, 100);
        next(check);
        assertEquals(1, next(check).size());
        onHeap = new SubPoolBalance(1, 0, 100);
        assertEquals(Finding.Kind.RECOVERED, next(check).get(0).kind);

        // fails again within the report interval of its report: not reported, and so no recovery either
        onHeap = new SubPoolBalance(-1, 0, 100);
        assertEquals(List.of(), next(check));
        assertEquals(List.of(), next(check));
        onHeap = new SubPoolBalance(1, 0, 100);
        assertEquals(List.of(), next(check));

        // after the interval, a new failure is reported again
        now += REPORT_INTERVAL;
        onHeap = new SubPoolBalance(-1, 0, 100);
        assertEquals(List.of(), next(check));
        assertEquals(List.of(ResourceAccountingCheck.ON_HEAP_ALLOCATED), invariants(next(check)));
    }

    @Test
    public void testEveryImpossibleState()
    {
        assertImpossible(() -> onHeap = new SubPoolBalance(-1, 0, 100), ResourceAccountingCheck.ON_HEAP_ALLOCATED,
                         "allocated -1 is negative (allocated -1, reclaiming 0, limit 100)");
        assertImpossible(() -> onHeap = new SubPoolBalance(1, -2, 100), ResourceAccountingCheck.ON_HEAP_RECLAIMING,
                         "reclaiming -2 is negative (allocated 1, reclaiming -2, limit 100)");
        assertImpossible(() -> offHeap = new SubPoolBalance(-3, 0, 100), ResourceAccountingCheck.OFF_HEAP_ALLOCATED,
                         "allocated -3 is negative (allocated -3, reclaiming 0, limit 100)");
        assertImpossible(() -> offHeap = new SubPoolBalance(0, -4, 0), ResourceAccountingCheck.OFF_HEAP_RECLAIMING,
                         "reclaiming -4 is negative (allocated 0, reclaiming -4, limit 0)");
        assertImpossible(() -> chunkCachePool = new BufferPoolBalance(-1, 0, 0), "buffer-pool.chunk-cache.memory-in-use",
                         "pooled memory in use -1 is negative (pooled memory in use -1, pooled memory allocated 0, overflow memory 0)");
        assertImpossible(() -> chunkCachePool = new BufferPoolBalance(0, 0, -2), "buffer-pool.chunk-cache.overflow",
                         "overflow memory -2 is negative (pooled memory in use 0, pooled memory allocated 0, overflow memory -2)");
        assertImpossible(() -> chunkCachePool = new BufferPoolBalance(3, 2, 0), "buffer-pool.chunk-cache.memory-in-use-within-allocated",
                         "pooled memory in use 3 is over the pooled memory allocated 2 (pooled memory in use 3, pooled memory allocated 2, overflow memory 0)");
        assertImpossible(() -> networkingPool = new BufferPoolBalance(-1, 0, 0), "buffer-pool.networking.memory-in-use",
                         "pooled memory in use -1 is negative (pooled memory in use -1, pooled memory allocated 0, overflow memory 0)");
        assertImpossible(() -> networkingPool = new BufferPoolBalance(0, 0, -2), "buffer-pool.networking.overflow",
                         "overflow memory -2 is negative (pooled memory in use 0, pooled memory allocated 0, overflow memory -2)");
        assertImpossible(() -> networkingPool = new BufferPoolBalance(3, 2, 0), "buffer-pool.networking.memory-in-use-within-allocated",
                         "pooled memory in use 3 is over the pooled memory allocated 2 (pooled memory in use 3, pooled memory allocated 2, overflow memory 0)");
    }

    // the given invariant, failing at persist checks, is reported once, alone, with the given state
    private void assertImpossible(Runnable fail, String invariant, String state)
    {
        SubPoolBalance healthyOnHeap = onHeap;
        SubPoolBalance healthyOffHeap = offHeap;
        BufferPoolBalance healthyChunkCachePool = chunkCachePool;
        BufferPoolBalance healthyNetworkingPool = networkingPool;
        ResourceAccountingCheck check = check();
        fail.run();
        assertEquals(invariant, List.of(), next(check));
        List<Finding> findings = next(check);
        assertEquals(invariant, List.of(invariant), invariants(findings));
        assertEquals(Finding.Kind.IMPOSSIBLE_STATE, findings.get(0).kind);
        assertEquals("Resource accounting: impossible state: " + invariant + ": " + state +
                     "; failed 2 consecutive checks, the first 5.0s ago", findings.get(0).message);
        onHeap = healthyOnHeap;
        offHeap = healthyOffHeap;
        chunkCachePool = healthyChunkCachePool;
        networkingPool = healthyNetworkingPool;
        assertEquals(List.of(invariant), invariants(next(check)));   // its recovery
    }

    @Test
    public void testBoundsHold()
    {
        // zero is no violation, and neither is memory in use equal to the memory allocated
        onHeap = new SubPoolBalance(0, 0, 100);
        offHeap = new SubPoolBalance(0, 0, 0);
        chunkCachePool = new BufferPoolBalance(2 * MiB, 2 * MiB, 0);
        networkingPool = new BufferPoolBalance(0, 0, 0);
        ResourceAccountingCheck check = check();
        for (int i = 0; i < 3; i++)
            assertEquals(List.of(), next(check));
    }

    @Test
    public void testDriftNeedsDriftChecks()
    {
        ResourceAccountingCheck check = check();
        // a capacity of 480 MiB: the slack is the capacity again
        chunkCache = new ChunkCacheBalance(960 * MiB + 1, 480 * MiB);
        for (int i = 1; i < DRIFT_CHECKS; i++)
            assertEquals("check " + i, List.of(), next(check));
        List<Finding> findings = next(check);
        assertEquals(1, findings.size());
        Finding finding = findings.get(0);
        assertEquals(ResourceAccountingCheck.CHUNK_CACHE_DRIFT, finding.invariant);
        assertEquals(Finding.Kind.DRIFT, finding.kind);
        assertEquals("Resource accounting: drift: chunk-cache.drift: the chunk cache's buffer pool has 1006632961 " +
                     "bytes in use, over the cache's capacity of 503316480 by 503316481, more than the 503316480 " +
                     "that chunks held by readers or being loaded, and hints being read, account for; failed 12 " +
                     "consecutive checks, the first 55.0s ago", finding.message);

        chunkCache = new ChunkCacheBalance(960 * MiB, 480 * MiB);
        assertEquals(List.of(Finding.Kind.RECOVERED), next(check).stream().map(f -> f.kind).collect(Collectors.toList()));
    }

    @Test
    public void testDriftSlackIsAtLeast64MiB()
    {
        ResourceAccountingCheck check = check();
        chunkCache = new ChunkCacheBalance(16 * MiB + 64 * MiB, 16 * MiB);
        for (int i = 0; i < DRIFT_CHECKS + 1; i++)
            assertEquals(List.of(), next(check));
        chunkCache = new ChunkCacheBalance(16 * MiB + 64 * MiB + 1, 16 * MiB);
        for (int i = 1; i < DRIFT_CHECKS; i++)
            assertEquals(List.of(), next(check));
        assertEquals(List.of(ResourceAccountingCheck.CHUNK_CACHE_DRIFT), invariants(next(check)));
    }

    @Test
    public void testNegativeReleasesReportedAtOnceWithTheirDelta()
    {
        ResourceAccountingCheck check = check();
        assertEquals(List.of(), next(check));

        negativeReleases = 3;
        List<Finding> findings = next(check);
        assertEquals(1, findings.size());
        assertEquals(ResourceAccountingCheck.NEGATIVE_RELEASE, findings.get(0).invariant);
        assertEquals(Finding.Kind.IMPOSSIBLE_STATE, findings.get(0).kind);
        assertEquals("Resource accounting: impossible state: memtable.negative-release: 3 negative releases from " +
                     "the memtable pool since the last report, the first seen 0.0s ago; 3 since startup; each was " +
                     "let through, which keeps the pool's balance, and logged with its stack by " +
                     AccountingViolations.class.getName() + " at most once a minute",
                     findings.get(0).message);

        // none since: nothing, and no recovery, as these are events rather than a state
        for (int i = 0; i < 3; i++)
            assertEquals(List.of(), next(check));

        // more within the report interval are held back, not lost, and reported with when the first was seen
        long firstSeen = now;
        negativeReleases = 4;
        assertEquals(List.of(), next(check));
        negativeReleases = 5;
        assertEquals(List.of(), next(check));
        now = START + CHECK_INTERVAL + REPORT_INTERVAL;
        findings = next(check);
        assertEquals(1, findings.size());
        assertTrue(findings.get(0).message, findings.get(0).message.contains(
                   ": 2 negative releases from the memtable pool since the last report, the first seen " +
                   (now - CHECK_INTERVAL - firstSeen) / 1_000_000_000 + ".0s ago; 5 since startup;"));
        for (int i = 0; i < 3; i++)
            assertEquals(List.of(), next(check));
    }

    @Test
    public void testNegativeReleasesBeforeTheCheckAreNotReported()
    {
        // as when the watchdog is restarted: those were reported, or logged, already
        negativeReleases = 7;
        ResourceAccountingCheck check = check();
        for (int i = 0; i < 3; i++)
            assertEquals(List.of(), next(check));
        negativeReleases = 8;
        List<Finding> findings = next(check);
        assertEquals(1, findings.size());
        assertTrue(findings.get(0).message, findings.get(0).message.contains(
                   ": 1 negative releases from the memtable pool since the last report, the first seen 0.0s ago; " +
                   "8 since startup;"));
    }

    @Test
    public void testRunLogsDriftAsWarningAndImpossibleStatesAsErrors()
    {
        onHeap = new SubPoolBalance(-1, 0, 100);
        chunkCache = new ChunkCacheBalance(Long.MAX_VALUE / 2, 480 * MiB);
        ResourceAccountingCheck check = new ResourceAccountingCheck(() -> new MemtablePoolBalance(onHeap, offHeap),
                                                                    () -> negativeReleases,
                                                                    List.of(),
                                                                    () -> chunkCache,
                                                                    2, 2, 0);
        try (CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
        {
            check.run();
            check.run();
            onHeap = new SubPoolBalance(1, 0, 100);
            check.run();
            // the impossible state and the drift reported, then the impossible state holding again, and the
            // drift, with no report interval, reported again
            assertEquals(List.of("ERROR " + ResourceAccountingCheck.IMPOSSIBLE_STATE + ResourceAccountingCheck.ON_HEAP_ALLOCATED + ": ",
                                 "WARN " + ResourceAccountingCheck.DRIFT + ResourceAccountingCheck.CHUNK_CACHE_DRIFT + ": ",
                                 "INFO Resource accounting: " + ResourceAccountingCheck.ON_HEAP_ALLOCATED + " holds again",
                                 "WARN " + ResourceAccountingCheck.DRIFT + ResourceAccountingCheck.CHUNK_CACHE_DRIFT + ": "),
                         log.levelsAndPrefixes(List.of(ResourceAccountingCheck.IMPOSSIBLE_STATE + ResourceAccountingCheck.ON_HEAP_ALLOCATED + ": ",
                                                       ResourceAccountingCheck.DRIFT + ResourceAccountingCheck.CHUNK_CACHE_DRIFT + ": ",
                                                       "Resource accounting: " + ResourceAccountingCheck.ON_HEAP_ALLOCATED + " holds again")));
        }
    }

    @Test
    public void testThrowingSourceDoesNotStopTheOthers()
    {
        ResourceAccountingCheck check = check(() -> { throw new IllegalStateException("broken"); });
        networkingPool = new BufferPoolBalance(0, 0, -1);
        assertEquals(List.of(), next(check));
        assertEquals(List.of("buffer-pool.networking.overflow"), invariants(next(check)));
    }

    @Test
    public void testThrowingSourceLeavesItsInvariantsAsTheyWere()
    {
        boolean[] broken = { false };
        ResourceAccountingCheck check = check(() -> {
            if (broken[0])
                throw new IllegalStateException("broken");
            return new MemtablePoolBalance(onHeap, offHeap);
        });
        onHeap = new SubPoolBalance(-1, 0, 100);
        assertEquals(List.of(), next(check));
        broken[0] = true;
        assertEquals(List.of(), next(check));
        assertEquals(1, check.failingCount());
        broken[0] = false;
        assertEquals(List.of(ResourceAccountingCheck.ON_HEAP_ALLOCATED), invariants(next(check)));
    }

    @Test
    public void testMissingChunkCacheIsSkipped()
    {
        chunkCache = null;
        ResourceAccountingCheck check = check();
        for (int i = 0; i < DRIFT_CHECKS + 1; i++)
            assertEquals(List.of(), next(check));
    }

    @Test
    public void testInvalidConfiguration()
    {
        String persist = RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS.getKey();
        String drift = RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS.getKey();
        assertEquals(persist + "=1 is under 2", ResourceAccountingCheck.invalidConfiguration(1, 12));
        assertEquals(persist + "=-1 is under 2", ResourceAccountingCheck.invalidConfiguration(-1, 12));
        assertEquals(drift + "=2 is under " + persist + ", 3", ResourceAccountingCheck.invalidConfiguration(3, 2));
        assertNull(ResourceAccountingCheck.invalidConfiguration(2, 2));
        assertNull(ResourceAccountingCheck.invalidConfiguration(2, 12));
    }

    @Test
    public void testScheduleReadsTheProperties() throws Exception
    {
        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "ResourceAccountingCheckTest");
        try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS, 3)
                                                             .set(RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS, 7))
        {
            ResourceAccountingCheck check = ResourceAccountingCheck.schedule(executor, CHECK_INTERVAL, REPORT_INTERVAL);
            assertNotNull(check);
            assertEquals(3, check.persistChecks);
            assertEquals(7, check.driftChecks);
            assertEquals(REPORT_INTERVAL, check.reportIntervalNanos);
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testScheduleWithDefaults() throws Exception
    {
        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "ResourceAccountingCheckTest");
        try (CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
        {
            ResourceAccountingCheck check = ResourceAccountingCheck.schedule(executor, CHECK_INTERVAL, REPORT_INTERVAL);
            assertNotNull(check);
            assertEquals(2, check.persistChecks);
            assertEquals(12, check.driftChecks);
            assertEquals(List.of("Resource accounting check started: checking every 5000ms; an impossible state is " +
                                 "reported after 2 consecutive failed checks, a drift after 12; report interval 600000ms"),
                         log.messages());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testScheduleDisabledOrInvalid() throws Exception
    {
        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "ResourceAccountingCheckTest");
        try
        {
            try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_ENABLED, false);
                 CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
            {
                assertNull(ResourceAccountingCheck.schedule(executor, CHECK_INTERVAL, REPORT_INTERVAL));
                assertEquals(List.of("Resource accounting check not started, as " +
                                     RESOURCE_ACCOUNTING_CHECK_ENABLED.getKey() + " is false"),
                             log.messages());
            }
            String persist = RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS.getKey();
            String drift = RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS.getKey();
            assertScheduleRefused(executor, "1", "12", persist + "=1 is under 2");
            assertScheduleRefused(executor, "4", "3", drift + "=3 is under " + persist + ", 4");
            assertScheduleRefused(executor, "two", "x", persist + "=two is not an integer; " + drift + "=x is not an integer");
            assertEquals(0, executor.getActiveTaskCount() + executor.getPendingTaskCount());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    private static void assertScheduleRefused(ScheduledExecutorPlus executor, String persist, String drift, String why)
    {
        try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS, persist)
                                                             .set(RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS, drift);
             CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
        {
            assertNull(ResourceAccountingCheck.schedule(executor, CHECK_INTERVAL, REPORT_INTERVAL));
            assertEquals(List.of("Resource accounting check not started, as its configuration is invalid: " + why),
                         log.messages());
        }
    }

    @Test
    public void testNodeSourcesAreRead()
    {
        // the node's own memtable pool, buffer pools and chunk cache: read without error, and a quiet node is healthy
        ResourceAccountingCheck check = ResourceAccountingCheck.forNode(PERSIST_CHECKS, DRIFT_CHECKS, REPORT_INTERVAL);
        try (CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
        {
            for (int i = 0; i < 3; i++)
                assertEquals(List.of(), next(check));
            assertEquals(List.of(), log.messages());
        }
    }

    // captures what a logger logs until closed
    private static final class CapturedLog implements AutoCloseable
    {
        private final Logger logger;
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        CapturedLog(String loggerName)
        {
            logger = (Logger) LoggerFactory.getLogger(loggerName);
            appender.start();
            logger.addAppender(appender);
        }

        List<String> messages()
        {
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        }

        // each event's level and which of the given prefixes its message starts with, or its whole message if none
        List<String> levelsAndPrefixes(List<String> prefixes)
        {
            return appender.list.stream()
                                .map(e -> e.getLevel() + " " + prefixes.stream()
                                                                       .filter(e.getFormattedMessage()::startsWith)
                                                                       .findFirst()
                                                                       .orElse(e.getFormattedMessage()))
                                .collect(Collectors.toList());
        }

        @Override
        public void close()
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
