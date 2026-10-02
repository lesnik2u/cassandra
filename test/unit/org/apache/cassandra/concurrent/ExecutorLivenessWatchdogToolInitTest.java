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

package org.apache.cassandra.concurrent;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A process that loads its configuration through {@link DatabaseDescriptor#toolInitialization(boolean)} and then
 * starts the watchdog gets the resource accounting check, and it reads the node's sources without error. Nothing in
 * this tree but {@link org.apache.cassandra.service.CassandraDaemon} starts the watchdog; this covers an embedder
 * that does. It needs a JVM of its own, in which nothing has called {@link DatabaseDescriptor#daemonInitialization()}.
 */
public class ExecutorLivenessWatchdogToolInitTest
{
    @After
    public void stopWatchdog()
    {
        ExecutorLivenessWatchdog.stop();
    }

    @Test
    public void testToolInitialisedNodeGetsTheAccountingCheck() throws Exception
    {
        DatabaseDescriptor.toolInitialization(false);
        assertTrue(DatabaseDescriptor.isToolInitialized());
        assertFalse(DatabaseDescriptor.isDaemonInitialized());

        Logger logger = (Logger) LoggerFactory.getLogger(ResourceAccountingCheck.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, 20))
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNotNull(ExecutorLivenessWatchdog.accountingCheck());

            // a few checks, on the watchdog's thread, read every source without logging anything
            Thread.sleep(500);
            ExecutorLivenessWatchdog.executor().submit(() -> {}).get(1, MINUTES);
            List<String> messages;
            synchronized (appender)
            {
                messages = appender.list.stream()
                                        .map(event -> event.getLevel() + " " + event.getFormattedMessage())
                                        .collect(Collectors.toList());
            }
            assertEquals(1, messages.size());
            assertTrue(messages.get(0), messages.get(0).startsWith(Level.INFO + " Resource accounting check started"));
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
