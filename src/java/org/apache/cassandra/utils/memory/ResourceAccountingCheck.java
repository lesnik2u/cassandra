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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Logs an error when the memtable pool's or the buffer pools' accounting is in a state a correct node cannot be in,
 * and a warning when it has drifted far from what it accounts for, so that a leak or a miscount shows up when it starts, rather than
 * when it fails a flush (HCD-534) or runs the node out of direct memory (CNDB-19222). It only reports: it never
 * corrects a counter, frees memory or fails anything.
 * <p>
 * It runs on {@link org.apache.cassandra.concurrent.ExecutorLivenessWatchdog}'s thread, at its check interval. Each
 * check reads a few counters, with no lock and no iteration over memtables, allocators or cache entries, and tests
 * the invariants below, each named in its report. The counters behind an invariant are updated one after another,
 * not together, so a check can read them between two updates and see an impossible state that a correct node passes
 * through; an invariant is therefore reported only once it has failed at {@link #persistChecks} consecutive checks
 * ({@link #driftChecks} for a drift), and the negative-release count, which only moves on a real violation, at once.
 * An impossible state is logged as an ERROR, a drift as a WARN. Each invariant is reported at most once per report
 * interval, and once more, as an INFO, when it holds again after a report; the negative-release count, which counts
 * events rather than describing a state, has no such recovery.
 * <ul>
 * <li>{@code memtable.<on|off>-heap.allocated}: the memtable sub-pool's {@code allocated} is not negative. It is
 * the sum of its allocators' {@code owns}, as an allocator passes every change to the pool before making it to its
 * {@code owns}, and an allocator's {@code owns} is not negative, as it releases only what it allocated before; it
 * is updated by compare-and-set, so it is never read half way through a change.</li>
 * <li>{@code memtable.<on|off>-heap.reclaiming}: the sub-pool's {@code reclaiming} is not negative. It is the sum of
 * the discarding allocators' {@code reclaiming}, each a past reading of its {@code owns}, so not negative; the pool
 * is told of a change after the allocator makes it, so two concurrent changes can reach the pool in the other order
 * and take it below zero for as long as that takes.</li>
 * <li>{@code memtable.negative-release}: no negative release has been let through since the last report. The
 * memtable pool counts them in {@link AccountingViolations}, which also logs each, with its stack, once a minute at
 * most; this reports how many there were since the last report, and since when, so none goes unseen. Those let
 * through before the check was created are not reported again.</li>
 * <li>{@code buffer-pool.<name>.memory-in-use}: the pool's bytes of pooled buffers in use are not negative. A
 * buffer is counted when handed out and uncounted when it is returned, after being freed, so a check can see the
 * return of a buffer handed out while it was reading, and not the handing out.</li>
 * <li>{@code buffer-pool.<name>.overflow}: the pool's bytes of unpooled buffers are not negative, likewise.</li>
 * <li>{@code buffer-pool.<name>.memory-in-use-within-allocated}: the bytes of pooled buffers in use are not more
 * than the bytes of the slabs they are cut from. Slabs are never freed, and a check reads the buffers in use first,
 * so as for {@code memory-in-use}, only a buffer returned and handed out again within the check can make it fail.
 * What makes it fail for good is a buffer returned to another pool than the one it came from: that pool uncounts it,
 * but its slab's slots are freed in the pool it came from, which hands them out again while still counting the
 * buffer, so that pool's count overtakes its slabs (and the other's can go negative).</li>
 * <li>{@code chunk-cache.drift}: the chunk cache's buffer pool does not hold more than the cache's capacity, plus
 * the larger of {@link #MIN_CHUNK_CACHE_SLACK_BYTES} and the capacity again. The cached chunks weigh at most the
 * capacity, but the pool also serves chunks that are evicted while a reader still holds them, chunks being loaded,
 * and the buffers of hints being read, none of which is bounded by anything but the load; such a load is
 * still unlikely to hold another whole cache's worth for as long as a drift must last to be reported (55 s by
 * default, as the 1st and the 12th check are 55 s apart at 5 s). The pool's own limit is the capacity plus
 * {@link ChunkCache#RESERVED_POOL_SPACE_IN_MiB}; beyond it, buffers are unpooled and counted as overflow. It is
 * compared to the capacity, not to the cache's weighted size, as reading that takes the cache's eviction lock and can
 * run the cache's maintenance, evictions included, on the calling thread. CNDB-19222 grew the pool's overflow for
 * weeks.</li>
 * </ul>
 * Not checked: that a sub-pool's {@code reclaiming} is at most its {@code allocated}. A release by a write still in
 * flight when its memtable is switched out can pass the allocator's check that it is live just before
 * {@code setDiscarding()} marks its {@code owns} as reclaiming, and take its size off {@code owns} and the pool just
 * after, leaving the allocator's {@code reclaiming} above its {@code owns} until the flush releases it, and the pool's
 * {@code reclaiming} above its {@code allocated} for that long if the other allocators own less than that.
 */
public final class ResourceAccountingCheck
{
    private static final Logger logger = LoggerFactory.getLogger(ResourceAccountingCheck.class);

    static final String IMPOSSIBLE_STATE = "Resource accounting: impossible state: ";
    static final String DRIFT = "Resource accounting: drift: ";

    static final String ON_HEAP_ALLOCATED = "memtable.on-heap.allocated";
    static final String ON_HEAP_RECLAIMING = "memtable.on-heap.reclaiming";
    static final String OFF_HEAP_ALLOCATED = "memtable.off-heap.allocated";
    static final String OFF_HEAP_RECLAIMING = "memtable.off-heap.reclaiming";
    static final String NEGATIVE_RELEASE = "memtable.negative-release";
    static final String CHUNK_CACHE_DRIFT = "chunk-cache.drift";

    private static final String NEGATIVE_RELEASES_SOURCE = "the memtable pool's negative releases";

    /** The fewest consecutive failed checks that can report an impossible state; see {@link #persistChecks}. */
    static final int MIN_PERSIST_CHECKS = 2;

    /** The least by which the chunk cache's buffer pool may hold more than the cache's capacity; see the class doc. */
    static final long MIN_CHUNK_CACHE_SLACK_BYTES = 64L << 20;

    private final Supplier<MemtablePoolBalance> memtablePool;
    private final LongSupplier negativeReleases;
    private final List<BufferPoolSource> bufferPools;
    private final Supplier<ChunkCacheBalance> chunkCache;

    /** How many consecutive checks an impossible state must fail before it is reported, at least 2. */
    final int persistChecks;
    /** How many consecutive checks a drift must fail before it is reported, at least {@link #persistChecks}. */
    final int driftChecks;
    final long reportIntervalNanos;

    // touched only by the checking thread: the invariants failing at the last check, when each was last reported,
    // the negative-release count at the last report (or when the check was created), and when a check first saw a
    // count above it, null if none has since that report
    private final Map<String, Failure> failures = new HashMap<>();
    private final Map<String, Long> lastReportNanos = new HashMap<>();
    private long reportedNegativeReleases;
    private Long firstUnreportedNegativeReleaseNanos;

    @VisibleForTesting
    ResourceAccountingCheck(Supplier<MemtablePoolBalance> memtablePool,
                            LongSupplier negativeReleases,
                            List<BufferPoolSource> bufferPools,
                            Supplier<ChunkCacheBalance> chunkCache,
                            int persistChecks,
                            int driftChecks,
                            long reportIntervalNanos)
    {
        this.memtablePool = memtablePool;
        this.negativeReleases = negativeReleases;
        this.bufferPools = bufferPools;
        this.chunkCache = chunkCache;
        this.persistChecks = persistChecks;
        this.driftChecks = driftChecks;
        this.reportIntervalNanos = reportIntervalNanos;
        this.reportedNegativeReleases = Math.max(0, readNegativeReleases());
    }

    /**
     * The check over the node's memtable pool, its two global buffer pools and its chunk cache, if any. They are
     * resolved here, on the calling thread, so that their initialisation, which needs the node's configuration, starts
     * threads and registers metrics, never runs on the checking thread.
     */
    @VisibleForTesting
    static ResourceAccountingCheck forNode(int persistChecks, int driftChecks, long reportIntervalNanos)
    {
        MemtablePool memtablePool = AbstractAllocatorMemtable.MEMORY_POOL;
        BufferPool chunkCachePool = BufferPools.forChunkCache();
        BufferPool networkingPool = BufferPools.forNetworking();
        ChunkCache chunkCache = ChunkCache.instance;
        List<BufferPoolSource> bufferPools =
            List.of(new BufferPoolSource("chunk-cache", () -> BufferPoolBalance.of(chunkCachePool)),
                    new BufferPoolSource("networking", () -> BufferPoolBalance.of(networkingPool)));
        Supplier<ChunkCacheBalance> chunkCacheBalance =
            chunkCache == null ? () -> null
                               : () -> new ChunkCacheBalance(chunkCachePool.usedSizeInBytes(), chunkCache.capacity());
        return new ResourceAccountingCheck(() -> MemtablePoolBalance.of(memtablePool),
                                           AccountingViolations::negativeReleases,
                                           bufferPools,
                                           chunkCacheBalance,
                                           persistChecks, driftChecks, reportIntervalNanos);
    }

    /**
     * Schedules the check on the given executor every check interval, unless it is disabled, its configuration is
     * invalid, or the node's configuration is not loaded; it logs which. The configuration is read here, once, and the
     * sources resolved, which initialises {@link AbstractAllocatorMemtable#MEMORY_POOL}, {@link BufferPools} and
     * {@link ChunkCache} if nothing has yet, and so fixes their configuration: call it only once the memtable and
     * cache configuration is final. Called by {@link org.apache.cassandra.concurrent.ExecutorLivenessWatchdog#start()},
     * with the watchdog's executor and its check and report intervals.
     * <p>
     * The node's configuration is loaded once {@link DatabaseDescriptor#daemonInitialization()} or
     * {@link DatabaseDescriptor#toolInitialization} has run, so a process that applies its configuration through
     * the latter and then starts the watchdog gets the check too, though in this tree only {@code CassandraDaemon}
     * starts the watchdog. Before that, resolving the sources would fail in {@code AbstractAllocatorMemtable}'s static
     * initialiser, and a class whose static initialiser has failed stays unusable for the life of the JVM, so the
     * check is skipped instead. A raw configuration alone, as client mode sets, is not enough, as the memtable and
     * cache settings are applied from it only by those two.
     *
     * @return the check scheduled, or null if none is
     */
    public static ResourceAccountingCheck schedule(ScheduledExecutorPlus executor,
                                                   long checkIntervalNanos,
                                                   long reportIntervalNanos)
    {
        if (!RESOURCE_ACCOUNTING_CHECK_ENABLED.getBoolean())
        {
            logger.info("Resource accounting check not started, as {} is false",
                        RESOURCE_ACCOUNTING_CHECK_ENABLED.getKey());
            return null;
        }

        List<String> invalid = new ArrayList<>();
        int persistChecks = intProperty(RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS, invalid);
        int driftChecks = intProperty(RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS, invalid);
        String invalidBounds = invalid.isEmpty() ? invalidConfiguration(persistChecks, driftChecks) : null;
        if (invalidBounds != null)
            invalid.add(invalidBounds);
        if (!invalid.isEmpty())
        {
            logger.warn("Resource accounting check not started, as its configuration is invalid: {}",
                        String.join("; ", invalid));
            return null;
        }
        if (!nodeConfigurationLoaded())
        {
            logger.info("Resource accounting check not started, as the node's configuration is not loaded: " +
                        "DatabaseDescriptor is neither daemon- nor tool-initialised");
            return null;
        }

        ResourceAccountingCheck check = forNode(persistChecks, driftChecks, reportIntervalNanos);
        executor.scheduleWithFixedDelay(check::run, checkIntervalNanos, checkIntervalNanos, NANOSECONDS);
        logger.info("Resource accounting check started: checking every {}ms; an impossible state is reported after " +
                    "{} consecutive failed checks, a drift after {}; report interval {}ms",
                    NANOSECONDS.toMillis(checkIntervalNanos), persistChecks, driftChecks,
                    NANOSECONDS.toMillis(reportIntervalNanos));
        return check;
    }

    /**
     * Whether the node's configuration is loaded, that is whether {@link DatabaseDescriptor} is daemon- or
     * tool-initialised. Call it from the thread that initialised {@code DatabaseDescriptor}, or after that
     * initialisation has finished, as {@code DatabaseDescriptor} sets these flags before {@code setConfig()}, so
     * another thread could see them set before the configuration is.
     */
    public static boolean nodeConfigurationLoaded()
    {
        return DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized();
    }

    private static int intProperty(CassandraRelevantProperties property, List<String> invalid)
    {
        try
        {
            return property.getInt();
        }
        catch (ConfigurationException e)
        {
            invalid.add(property.getKey() + '=' + property.getString() + " is not an integer");
            return 0;
        }
    }

    /**
     * Why the configuration is invalid, naming each invalid property and its value, or null if it is valid.
     */
    @VisibleForTesting
    static String invalidConfiguration(int persistChecks, int driftChecks)
    {
        if (persistChecks < MIN_PERSIST_CHECKS)
            return RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS.getKey() + '=' + persistChecks + " is under " +
                   MIN_PERSIST_CHECKS;
        if (driftChecks < persistChecks)
            return RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS.getKey() + '=' + driftChecks + " is under " +
                   RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS.getKey() + ", " + persistChecks;
        return null;
    }

    @VisibleForTesting
    void run()
    {
        try
        {
            for (Finding finding : check(preciseTime.now()))
            {
                if (finding.kind == Finding.Kind.RECOVERED)
                    logger.info(finding.message);
                else if (finding.kind == Finding.Kind.DRIFT)
                    logger.warn(finding.message);
                else
                    logger.error(finding.message);
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Resource accounting check failed", t);
        }
    }

    /**
     * One check: the invariants to report now as failing, or as holding again. It logs nothing, but records what it
     * returns as reported, for the rate limits. A source that cannot be read is logged and skipped, its invariants
     * left as they were.
     */
    @VisibleForTesting
    List<Finding> check(long nowNanos)
    {
        List<Finding> findings = new ArrayList<>();

        MemtablePoolBalance memtable = read(memtablePool, "the memtable pool");
        if (memtable != null)
        {
            checkSubPool(memtable.onHeap, ON_HEAP_ALLOCATED, ON_HEAP_RECLAIMING, nowNanos, findings);
            checkSubPool(memtable.offHeap, OFF_HEAP_ALLOCATED, OFF_HEAP_RECLAIMING, nowNanos, findings);
        }

        long negatives = readNegativeReleases();   // -1 if it cannot be read, so never above the last reported
        if (negatives > reportedNegativeReleases)
        {
            // events, not a state: reported with how many and since when, as soon as the report interval allows
            if (firstUnreportedNegativeReleaseNanos == null)
                firstUnreportedNegativeReleaseNanos = nowNanos;
            if (due(NEGATIVE_RELEASE, nowNanos))
            {
                findings.add(new Finding(NEGATIVE_RELEASE, Finding.Kind.IMPOSSIBLE_STATE,
                                         IMPOSSIBLE_STATE + NEGATIVE_RELEASE + ": " +
                                         (negatives - reportedNegativeReleases) + " negative releases from the " +
                                         "memtable pool since the last report, the first seen " +
                                         seconds(nowNanos - firstUnreportedNegativeReleaseNanos) + " ago; " +
                                         negatives + " since startup; each was let through, which keeps the pool's " +
                                         "balance, and logged with its stack by " +
                                         AccountingViolations.class.getName() + " at most once a minute"));
                reportedNegativeReleases = negatives;
                firstUnreportedNegativeReleaseNanos = null;
            }
        }

        for (BufferPoolSource source : bufferPools)
        {
            BufferPoolBalance pool = read(source.balance, source.description);
            if (pool == null)
                continue;
            if (pool.memoryInUse >= 0)
                held(source.memoryInUse, nowNanos, findings);
            else if (fails(source.memoryInUse, persistChecks, nowNanos))
                report(source.memoryInUse, Finding.Kind.IMPOSSIBLE_STATE,
                       "pooled memory in use " + pool.memoryInUse + " is negative" + pool, nowNanos, findings);

            if (pool.overflow >= 0)
                held(source.overflow, nowNanos, findings);
            else if (fails(source.overflow, persistChecks, nowNanos))
                report(source.overflow, Finding.Kind.IMPOSSIBLE_STATE,
                       "overflow memory " + pool.overflow + " is negative" + pool, nowNanos, findings);

            if (pool.memoryInUse <= pool.memoryAllocated)
                held(source.memoryInUseWithinAllocated, nowNanos, findings);
            else if (fails(source.memoryInUseWithinAllocated, persistChecks, nowNanos))
                report(source.memoryInUseWithinAllocated, Finding.Kind.IMPOSSIBLE_STATE,
                       "pooled memory in use " + pool.memoryInUse + " is over the pooled memory allocated " +
                       pool.memoryAllocated + pool, nowNanos, findings);
        }

        ChunkCacheBalance cache = read(chunkCache, "the chunk cache");
        if (cache != null)
        {
            long slack = Math.max(MIN_CHUNK_CACHE_SLACK_BYTES, cache.capacity);
            if (cache.poolInUse - cache.capacity <= slack)
                held(CHUNK_CACHE_DRIFT, nowNanos, findings);
            else if (fails(CHUNK_CACHE_DRIFT, driftChecks, nowNanos))
                report(CHUNK_CACHE_DRIFT, Finding.Kind.DRIFT,
                       "the chunk cache's buffer pool has " + cache.poolInUse + " bytes in use, over the cache's " +
                       "capacity of " + cache.capacity + " by " + (cache.poolInUse - cache.capacity) +
                       ", more than the " + slack + " that chunks held by readers or being loaded, and hints being " +
                       "read, account for", nowNanos, findings);
        }
        return findings;
    }

    private void checkSubPool(SubPoolBalance pool, String allocated, String reclaiming, long nowNanos,
                              List<Finding> findings)
    {
        if (pool.allocated >= 0)
            held(allocated, nowNanos, findings);
        else if (fails(allocated, persistChecks, nowNanos))
            report(allocated, Finding.Kind.IMPOSSIBLE_STATE, "allocated " + pool.allocated + " is negative" + pool,
                   nowNanos, findings);

        if (pool.reclaiming >= 0)
            held(reclaiming, nowNanos, findings);
        else if (fails(reclaiming, persistChecks, nowNanos))
            report(reclaiming, Finding.Kind.IMPOSSIBLE_STATE, "reclaiming " + pool.reclaiming + " is negative" + pool,
                   nowNanos, findings);
    }

    // a source's balance, or null if it has none, as the chunk cache when there is none, or cannot be read
    private <T> T read(Supplier<T> source, String what)
    {
        try
        {
            return source.get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            couldNotRead(what, t);
            return null;
        }
    }

    // keyed by the source, so that one broken source does not hide another
    private void couldNotRead(String what, Throwable t)
    {
        String message = "Resource accounting: could not read {}";
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, message + ' ' + what, reportIntervalNanos, NANOSECONDS,
                         message, what, t);
    }

    // the negative-release count, read without boxing it, or -1 if it cannot be read
    private long readNegativeReleases()
    {
        try
        {
            return negativeReleases.getAsLong();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            couldNotRead(NEGATIVE_RELEASES_SOURCE, t);
            return -1;
        }
    }

    // records that the invariant fails at this check, and whether to report it now: once it has failed at the given
    // number of consecutive checks, and not within the report interval of its last report
    private boolean fails(String invariant, int checksToReport, long nowNanos)
    {
        Failure failure = failures.get(invariant);
        if (failure == null)
        {
            failure = new Failure(nowNanos);
            failures.put(invariant, failure);
        }
        failure.checks++;
        if (failure.checks < checksToReport || !due(invariant, nowNanos))
            return false;
        failure.reported = true;
        return true;
    }

    // whether the invariant may be reported now, not within the report interval of its last report; if so, records
    // that it is
    private boolean due(String invariant, long nowNanos)
    {
        Long last = lastReportNanos.get(invariant);
        if (last != null && nowNanos - last < reportIntervalNanos)
            return false;
        lastReportNanos.put(invariant, nowNanos);
        return true;
    }

    private void report(String invariant, Finding.Kind kind, String state, long nowNanos, List<Finding> findings)
    {
        Failure failure = failures.get(invariant);
        String prefix = kind == Finding.Kind.DRIFT ? DRIFT : IMPOSSIBLE_STATE;
        findings.add(new Finding(invariant, kind, prefix + invariant + ": " + state + "; failed " + failure.checks +
                                                  " consecutive checks, the first " +
                                                  seconds(nowNanos - failure.firstNanos) + " ago"));
    }

    // records that the invariant holds at this check, and reports that it holds again if it was reported failing
    private void held(String invariant, long nowNanos, List<Finding> findings)
    {
        if (failures.isEmpty())
            return;
        Failure failure = failures.remove(invariant);
        if (failure != null && failure.reported)
            findings.add(new Finding(invariant, Finding.Kind.RECOVERED,
                                     "Resource accounting: " + invariant + " holds again, after failing " +
                                     failure.checks + " consecutive checks over " +
                                     seconds(nowNanos - failure.firstNanos)));
    }

    @VisibleForTesting
    int failingCount()
    {
        return failures.size();
    }

    // seconds to one decimal place, independent of the locale
    private static String seconds(long nanos)
    {
        long tenths = NANOSECONDS.toMillis(nanos) / 100;
        return tenths / 10 + "." + tenths % 10 + 's';
    }

    /** An invariant failing at consecutive checks up to the last: since when, at how many, and whether reported. */
    private static final class Failure
    {
        final long firstNanos;
        int checks;
        boolean reported;

        Failure(long firstNanos)
        {
            this.firstNanos = firstNanos;
        }
    }

    /** A memtable sub-pool's balances, read in one go. */
    static final class SubPoolBalance
    {
        final long allocated;
        final long reclaiming;
        final long limit;

        SubPoolBalance(long allocated, long reclaiming, long limit)
        {
            this.allocated = allocated;
            this.reclaiming = reclaiming;
            this.limit = limit;
        }

        static SubPoolBalance of(MemtablePool.SubPool pool)
        {
            return new SubPoolBalance(pool.used(), pool.getReclaiming(), pool.limit);
        }

        @Override
        public String toString()
        {
            return " (allocated " + allocated + ", reclaiming " + reclaiming + ", limit " + limit + ')';
        }
    }

    /** The memtable pool's balances, read in one go. */
    static final class MemtablePoolBalance
    {
        final SubPoolBalance onHeap;
        final SubPoolBalance offHeap;

        MemtablePoolBalance(SubPoolBalance onHeap, SubPoolBalance offHeap)
        {
            this.onHeap = onHeap;
            this.offHeap = offHeap;
        }

        static MemtablePoolBalance of(MemtablePool pool)
        {
            return new MemtablePoolBalance(SubPoolBalance.of(pool.onHeap), SubPoolBalance.of(pool.offHeap));
        }
    }

    /** A buffer pool's balances, read in one go. */
    static final class BufferPoolBalance
    {
        final long memoryInUse;
        final long memoryAllocated;
        final long overflow;

        BufferPoolBalance(long memoryInUse, long memoryAllocated, long overflow)
        {
            this.memoryInUse = memoryInUse;
            this.memoryAllocated = memoryAllocated;
            this.overflow = overflow;
        }

        // the memory in use first: the memory allocated never shrinks (but by unsafeReset(), which only tests call), so,
        // read after it, covers every buffer it counts
        static BufferPoolBalance of(BufferPool pool)
        {
            long memoryInUse = pool.pooledMemoryInUseInBytes();
            long memoryAllocated = pool.pooledMemoryAllocatedInBytes();
            return new BufferPoolBalance(memoryInUse, memoryAllocated, pool.overflowMemoryInBytes());
        }

        @Override
        public String toString()
        {
            return " (pooled memory in use " + memoryInUse + ", pooled memory allocated " + memoryAllocated +
                   ", overflow memory " + overflow + ')';
        }
    }

    /** A buffer pool to check, with the names of its invariants. */
    static final class BufferPoolSource
    {
        final String description;
        final Supplier<BufferPoolBalance> balance;
        final String memoryInUse;
        final String overflow;
        final String memoryInUseWithinAllocated;

        BufferPoolSource(String name, Supplier<BufferPoolBalance> balance)
        {
            this.description = "buffer pool " + name;
            this.balance = balance;
            this.memoryInUse = "buffer-pool." + name + ".memory-in-use";
            this.overflow = "buffer-pool." + name + ".overflow";
            this.memoryInUseWithinAllocated = "buffer-pool." + name + ".memory-in-use-within-allocated";
        }
    }

    /** What the chunk cache's buffer pool has in use, unpooled buffers included, and the cache's capacity. */
    static final class ChunkCacheBalance
    {
        final long poolInUse;
        final long capacity;

        ChunkCacheBalance(long poolInUse, long capacity)
        {
            this.poolInUse = poolInUse;
            this.capacity = capacity;
        }
    }

    /** An invariant to report as failing, or as holding again. */
    static final class Finding
    {
        enum Kind { IMPOSSIBLE_STATE, DRIFT, RECOVERED }

        final String invariant;
        final Kind kind;
        final String message;

        Finding(String invariant, Kind kind, String message)
        {
            this.invariant = invariant;
            this.kind = kind;
            this.message = message;
        }

        @Override
        public String toString()
        {
            return message;
        }
    }
}
