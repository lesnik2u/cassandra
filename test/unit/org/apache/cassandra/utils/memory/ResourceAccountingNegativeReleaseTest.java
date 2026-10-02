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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The HCD-534 shape, end to end, without strict runtime checks, as in production: a memtable's allocator releases
 * more than it owns, through the {@code SubAllocator.adjust} path that HCD-534's shadowed cells took (fixed since, so
 * driven here through {@link Memtable#markExtraOnHeapUsed}, as an index accounts its memory), and the memtable is
 * then flushed. The flush completes, and the watchdog's next accounting check reports the negative release.
 */
@RunWith(Parameterized.class)
public class ResourceAccountingNegativeReleaseTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "memtable={0}")
    public static List<Object> parameters()
    {
        return List.of("skiplist", "trie");
    }

    @Test
    public void testNegativeOwnsAtFlushIsReported() throws Throwable
    {
        boolean strict = AccountingViolations.setStrictUnsafe(false);
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, 100);
             CapturedLog log = new CapturedLog(ResourceAccountingCheck.class.getName()))
        {
            ExecutorLivenessWatchdog.start();
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            for (int i = 0; i < 100; i++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", i % 10, i, "value " + i);

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            Memtable current = cfs.getTracker().getView().getCurrentMemtable();
            MemtableAllocator.SubAllocator onHeap = ((AbstractAllocatorMemtable) current).getAllocator().onHeap();
            try (OpOrder.Group group = Keyspace.writeOrder.start())
            {
                current.markExtraOnHeapUsed(-(onHeap.owns() + 1024), group);
            }
            assertEquals(-1024, onHeap.owns());

            long violations = AccountingViolations.negativeReleases();
            // the flush discards the memtable, so releases its allocator, once it has written it, on the reclaim executor
            flush();

            // the report that counts this one, as an earlier test case may have let one through too
            String reported = ResourceAccountingCheck.IMPOSSIBLE_STATE + ResourceAccountingCheck.NEGATIVE_RELEASE +
                              ": 1 negative releases from the memtable pool since the last report, the first seen ";
            String counted = " ago; " + (violations + 1) + " since startup;";
            ILoggingEvent report = log.await(event -> event.getFormattedMessage().startsWith(reported)
                                                      && event.getFormattedMessage().contains(counted));
            assertEquals(Level.ERROR, report.getLevel());
            assertTrue(report.getThreadName(), report.getThreadName().startsWith("ExecutorLivenessWatchdog"));
            assertEquals(violations + 1, AccountingViolations.negativeReleases());

            // the table goes on taking writes and flushing
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 0, 1000, "after");
            flush();
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck = 1000"), row("after"));
        }
        finally
        {
            ExecutorLivenessWatchdog.stop();
            AccountingViolations.setStrictUnsafe(strict);
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

        // the first event that matches, waiting up to a minute for it
        ILoggingEvent await(Predicate<ILoggingEvent> matches) throws InterruptedException
        {
            long deadline = nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (nanoTime() < deadline)
            {
                List<ILoggingEvent> events;
                synchronized (appender)
                {
                    events = new ArrayList<>(appender.list);
                }
                for (ILoggingEvent event : events)
                {
                    if (matches.test(event))
                        return event;
                }
                Thread.sleep(10);
            }
            throw new AssertionError("not logged within a minute");
        }

        @Override
        public void close()
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
