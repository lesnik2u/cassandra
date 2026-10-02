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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * What the memtable pool does when a release finds its accounting impossible, in place of the assert that took down
 * whichever thread happened to release (HCD-534: a flush, in {@code SubAllocator.releaseAll}).
 * <p>
 * With {@link DatabaseDescriptor#strictRuntimeChecks()}, which every test run sets, it throws the assert's
 * {@link AssertionError}, with its message. Otherwise it logs one ERROR a minute, with the sub-pool's balances and the
 * stack, counts the violation for {@link ResourceAccountingCheck}, which reports every one of them, and returns, so the
 * release goes ahead as it would with assertions disabled.
 * <p>
 * Going ahead keeps the pool's balance right. A {@link MemtableAllocator.SubAllocator} passes every change to the pool
 * as it makes it to its own {@code owns}: an allocation adds to both, a release while live subtracts from both, and a
 * release after that changes neither. So the pool's {@code allocated} always includes the allocator's {@code owns},
 * negative or not, and an allocator whose {@code owns} went negative (by releasing more than it allocated) has
 * already taken that much too much off the pool. Its final {@code releaseAll()} releases its {@code owns}, here a
 * negative size, which gives the pool back exactly that, leaving it as if the allocator had never allocated. Its
 * {@code reclaiming}, set from that negative {@code owns} by {@code setDiscarding()}, is likewise given back by the
 * {@code reclaimed()} that follows. The assert, thrown after {@code releaseAll()} had zeroed the allocator's
 * {@code owns} and before that {@code reclaimed()}, left both the pool's {@code allocated} and its {@code reclaiming}
 * wrong for good, as well as failing the flush.
 */
final class AccountingViolations
{
    private static final Logger logger = LoggerFactory.getLogger(AccountingViolations.class);
    private static final String NEGATIVE_RELEASE_MESSAGE =
        "Resource accounting: impossible state: {}: negative release of {} bytes from the memtable {} pool, so a " +
        "memtable allocator released more than it allocated; releasing it anyway, which gives the pool back what " +
        "that allocator took off too much. Pool allocated {}, reclaiming {}, limit {}; logged at most once a " +
        "minute, every one is counted and reported by the resource accounting check";

    private static final LongAdder negativeReleases = new LongAdder();

    // whether a violation throws, as the assert did; volatile so that a test can turn it off, and read only on a
    // violation
    private static volatile boolean strict = DatabaseDescriptor.strictRuntimeChecks();

    private AccountingViolations()
    {
    }

    /**
     * Called by {@link MemtablePool.SubPool#released(long)} with a negative size, before it applies it.
     */
    static void negativeRelease(MemtablePool.SubPool pool, long size)
    {
        if (strict)
            throw new AssertionError("Negative released: " + size);

        negativeReleases.increment();
        // the arguments, the stack among them, are built only if the rate limit lets this one be logged
        NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES, NEGATIVE_RELEASE_MESSAGE,
                         () -> new Object[]{ ResourceAccountingCheck.NEGATIVE_RELEASE, size, pool.name(), pool.used(),
                                             pool.getReclaiming(), pool.limit, new Throwable() });
    }

    /** The negative releases let through since startup. */
    static long negativeReleases()
    {
        return negativeReleases.sum();
    }

    /**
     * Sets whether a violation throws, returning the previous setting.
     */
    @VisibleForTesting
    static boolean setStrictUnsafe(boolean newStrict)
    {
        boolean previous = strict;
        strict = newStrict;
        return previous;
    }
}
