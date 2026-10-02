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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The resource accounting check runs with the watchdog, on its executor, and its own configuration never stops the
 * watchdog.
 */
@RunWith(BMUnitRunner.class)
public class ExecutorLivenessWatchdogAccountingTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void stopWatchdog()
    {
        ExecutorLivenessWatchdog.stop();
    }

    @Test
    public void testStartsAndStopsWithTheWatchdog()
    {
        ExecutorLivenessWatchdog.start();
        assertTrue(ExecutorLivenessWatchdog.isRunning());
        assertNotNull(ExecutorLivenessWatchdog.accountingCheck());
        ExecutorLivenessWatchdog.stop();
        assertNull(ExecutorLivenessWatchdog.accountingCheck());
    }

    @Test
    public void testDisabledLeavesTheWatchdogRunning()
    {
        try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_ENABLED, false))
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
        }
    }

    @Test
    public void testInvalidConfigurationLeavesTheWatchdogRunning()
    {
        try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_PERSIST_CHECKS, 1))
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
        }
        ExecutorLivenessWatchdog.stop();
        try (WithProperties properties = new WithProperties().set(RESOURCE_ACCOUNTING_CHECK_DRIFT_CHECKS, "twelve"))
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
        }
    }

    @Test
    @BMRule(name = "fail to create the check over the node's sources",
            targetClass = "org.apache.cassandra.utils.memory.ResourceAccountingCheck",
            targetMethod = "forNode",
            targetLocation = "AT ENTRY",
            action = "throw new java.lang.IllegalStateException(\"injected\")")
    public void testFailedStartLeavesTheWatchdogRunning()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(ExecutorLivenessWatchdog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
            List<ILoggingEvent> warnings = appender.list.stream()
                                                        .filter(event -> event.getLevel() == Level.WARN)
                                                        .collect(Collectors.toList());
            assertEquals(1, warnings.size());
            assertEquals("Resource accounting check failed to start; the executor liveness watchdog runs without it",
                         warnings.get(0).getFormattedMessage());
            assertEquals("injected", warnings.get(0).getThrowableProxy().getMessage());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    @BMRule(name = "fail to create the check over the node's sources",
            targetClass = "org.apache.cassandra.utils.memory.ResourceAccountingCheck",
            targetMethod = "forNode",
            targetLocation = "AT ENTRY",
            action = "throw new java.lang.IllegalStateException(\"injected\")")
    public void testFailedStartIsInspected()
    {
        // the node's configuration is loaded, so the failure is passed to JVMStabilityInspector
        List<Throwable> inspected = new CopyOnWriteArrayList<>();
        BiConsumer<Throwable, Boolean> handler = JVMStabilityInspector.getGlobalErrorHandler();
        JVMStabilityInspector.setGlobalErrorHandler((t, isUncaughtException) -> {
            inspected.add(t);
            handler.accept(t, isUncaughtException);
        });
        try
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
            assertEquals(1, inspected.stream()
                                     .filter(t -> t instanceof IllegalStateException && "injected".equals(t.getMessage()))
                                     .count());
        }
        finally
        {
            JVMStabilityInspector.setGlobalErrorHandler(handler);
        }
    }

    @Test
    public void testNotRunWithoutTheWatchdog()
    {
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_ENABLED, false))
        {
            ExecutorLivenessWatchdog.start();
            assertFalse(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
        }
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, 0))
        {
            ExecutorLivenessWatchdog.start();
            assertFalse(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
        }
    }
}
