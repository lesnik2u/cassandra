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
import org.junit.runner.RunWith;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.memory.ResourceAccountingCheck;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * An embedder may start the watchdog before the node's configuration is loaded. The watchdog then runs, and the
 * resource accounting check, whose sources need that configuration, is not started; nothing initialises them. If
 * starting the check fails, the watchdog still runs, and logs the failure without inspecting it, as
 * {@link org.apache.cassandra.utils.JVMStabilityInspector} reads that configuration and would throw out of
 * {@link ExecutorLivenessWatchdog#start()}; a {@link VirtualMachineError} is still thrown out of it. It needs a JVM of
 * its own, in which nothing has called {@link DatabaseDescriptor#daemonInitialization()}.
 */
@RunWith(BMUnitRunner.class)
public class ExecutorLivenessWatchdogNoDaemonTest
{
    @After
    public void stopWatchdog()
    {
        ExecutorLivenessWatchdog.stop();
    }

    @Test
    public void testWatchdogRunsWithoutTheAccountingCheck() throws Exception
    {
        assertFalse(DatabaseDescriptor.isDaemonInitialized());
        Logger logger = (Logger) LoggerFactory.getLogger(ResourceAccountingCheck.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            assertNull(ExecutorLivenessWatchdog.accountingCheck());
            assertEquals(List.of("Resource accounting check not started, as the node's configuration is not loaded: " +
                                 "DatabaseDescriptor is neither daemon- nor tool-initialised"),
                         appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList()));

            // the watchdog's own checks run, on its thread, and still nothing loads the configuration
            ExecutorLivenessWatchdog watchdog = ExecutorLivenessWatchdog.watchdog();
            ExecutorLivenessWatchdog.executor().submit(watchdog::runCheck).get(1, MINUTES);
            assertFalse(DatabaseDescriptor.isDaemonInitialized());
            assertFalse(DatabaseDescriptor.isToolInitialized());
            assertNull(DatabaseDescriptor.getRawConfig());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    @BMRule(name = "fail to start the accounting check",
            targetClass = "org.apache.cassandra.utils.memory.ResourceAccountingCheck",
            targetMethod = "schedule",
            targetLocation = "AT ENTRY",
            action = "throw new java.lang.IllegalStateException(\"injected\")")
    public void testFailedStartLeavesTheWatchdogRunning()
    {
        assertFalse(DatabaseDescriptor.isDaemonInitialized());
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
            assertFalse(DatabaseDescriptor.isDaemonInitialized());
            assertFalse(DatabaseDescriptor.isToolInitialized());
            assertNull(DatabaseDescriptor.getRawConfig());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    @BMRule(name = "fail to start the accounting check with an OutOfMemoryError",
            targetClass = "org.apache.cassandra.utils.memory.ResourceAccountingCheck",
            targetMethod = "schedule",
            targetLocation = "AT ENTRY",
            action = "throw new java.lang.OutOfMemoryError(\"injected\")")
    public void testVirtualMachineErrorIsRethrown()
    {
        assertFalse(DatabaseDescriptor.isDaemonInitialized());
        Logger logger = (Logger) LoggerFactory.getLogger(ExecutorLivenessWatchdog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            OutOfMemoryError error = assertThrows(OutOfMemoryError.class, ExecutorLivenessWatchdog::start);
            assertEquals("injected", error.getMessage());
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            List<ILoggingEvent> warnings = appender.list.stream()
                                                        .filter(event -> event.getLevel() == Level.WARN)
                                                        .collect(Collectors.toList());
            assertEquals(1, warnings.size());
            assertEquals("injected", warnings.get(0).getThrowableProxy().getMessage());
            assertFalse(DatabaseDescriptor.isDaemonInitialized());
            assertFalse(DatabaseDescriptor.isToolInitialized());
            assertNull(DatabaseDescriptor.getRawConfig());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
