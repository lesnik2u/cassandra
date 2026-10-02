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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The negative release that HCD-534 crashed a flush with: with strict runtime checks, as in every test run, it still
 * throws the assert's error; without them, as in production, it is logged, counted and let through, which leaves the
 * pool as if the offending allocator had never allocated.
 */
public class AccountingViolationsTest
{
    private SlabPool pool;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void createPool()
    {
        pool = new SlabPool(1 << 20, 1 << 20, 1.0f, () -> ImmediateFuture.success(true));
    }

    @After
    public void shutdownPool() throws Exception
    {
        AccountingViolations.setStrictUnsafe(DatabaseDescriptor.strictRuntimeChecks());
        pool.shutdownAndWait(1, TimeUnit.MINUTES);
    }

    @Test
    public void testStrictThrowsAsTheAssertDid()
    {
        assertTrue("test runs set cassandra.strict.runtime.checks", DatabaseDescriptor.strictRuntimeChecks());
        pool.onHeap.allocated(100);
        long violations = AccountingViolations.negativeReleases();
        try
        {
            pool.onHeap.released(-5);
            fail("a negative release did not throw with strict runtime checks");
        }
        catch (AssertionError e)
        {
            assertEquals("Negative released: -5", e.getMessage());
        }
        assertEquals(100, pool.onHeap.used());
        assertEquals(violations, AccountingViolations.negativeReleases());
    }

    @Test
    public void testNotStrictLogsCountsAndReleases()
    {
        AccountingViolations.setStrictUnsafe(false);
        pool.offHeap.allocated(100);
        pool.offHeap.reclaiming(40);
        long violations = AccountingViolations.negativeReleases();
        // an hour on, so that no violation logged by another test holds this one back
        NoSpamLogger.unsafeSetClock(() -> Clock.Global.nanoTime() + TimeUnit.HOURS.toNanos(1));
        try (CapturedLog log = new CapturedLog(AccountingViolations.class.getName()))
        {
            pool.offHeap.released(-5);
            assertEquals(105, pool.offHeap.used());
            assertEquals(violations + 1, AccountingViolations.negativeReleases());

            List<ILoggingEvent> events = log.events();
            assertEquals(1, events.size());
            ILoggingEvent event = events.get(0);
            assertEquals(Level.ERROR, event.getLevel());
            assertEquals("Resource accounting: impossible state: memtable.negative-release: negative release of -5 " +
                         "bytes from the memtable off-heap pool, so a memtable allocator released more than it " +
                         "allocated; releasing it anyway, which gives the pool back what that allocator took off too " +
                         "much. Pool allocated 100, reclaiming 40, limit 1048576; logged at most once a minute, " +
                         "every one is counted and reported by the resource accounting check",
                         event.getFormattedMessage());
            assertNotNull("the stack of the release is logged", event.getThrowableProxy());

            // within the minute, counted but not logged again
            pool.onHeap.allocated(10);
            pool.onHeap.released(-7);
            assertEquals(17, pool.onHeap.used());
            assertEquals(violations + 2, AccountingViolations.negativeReleases());
            assertEquals(1, log.events().size());
        }
        finally
        {
            NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
        }
    }

    /**
     * The HCD-534 shape: an allocator releases more than it allocated, so its owns go negative, and its memtable is
     * then flushed. Its negative release at discard gives the pool back what it took off too much, and its negative
     * reclaiming is given back too, so the pool ends where the other allocators leave it.
     */
    @Test
    public void testNegativeOwnsAtDiscardRestoresThePool()
    {
        AccountingViolations.setStrictUnsafe(false);
        OpOrder order = new OpOrder();
        MemtableAllocator other = pool.newAllocator("other");
        MemtableAllocator overReleasing = pool.newAllocator("over-releasing");
        try (OpOrder.Group group = order.start())
        {
            other.onHeap().allocate(1000, group);
            overReleasing.onHeap().allocate(100, group);
            overReleasing.onHeap().adjust(-150, group);
        }
        assertEquals(-50, overReleasing.onHeap().owns());
        assertEquals(950, pool.onHeap.used());

        overReleasing.setDiscarding();
        assertEquals(-50, pool.onHeap.getReclaiming());
        long violations = AccountingViolations.negativeReleases();
        overReleasing.setDiscarded();

        assertEquals(violations + 1, AccountingViolations.negativeReleases());
        assertEquals(1000, pool.onHeap.used());
        assertEquals(0, pool.onHeap.getReclaiming());
        assertEquals(0, overReleasing.onHeap().owns());
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

        List<ILoggingEvent> events()
        {
            return appender.list;
        }

        @Override
        public void close()
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
