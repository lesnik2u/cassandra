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

package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativePool;

import static org.apache.cassandra.Util.captureLog;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.setCommitLog;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class TrieMemtableValueStorageOptionTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // Create the memtable pool from the test yaml now, before any test temporarily changes the allocation type.
        assertNotNull(AbstractAllocatorMemtable.MEMORY_POOL);
        assertNotNull(TrieMemtable.BUFFER_TYPE);
    }

    /// Nothing turns value location recording off once set, and accepting `commitlog` value storage sets it.
    @Before
    @After
    public void disableValueLocations()
    {
        SerializedValueOffsets.ENABLED = false;
    }

    /// Runs `body` with `memtable_allocation_type` set to `offheap_objects`, as `factory()` checks it. This does not
    /// change the memtable pool, which is created once.
    private static <T> T withOffheapObjects(Supplier<T> body)
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        Config.MemtableAllocationType previous = conf.memtable_allocation_type;
        conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        try
        {
            return body.get();
        }
        finally
        {
            conf.memtable_allocation_type = previous;
        }
    }

    /// Runs `body` with the given resolved commit log disk access mode (null for unresolved, as in offline tools),
    /// compression and encryption overridden in [DatabaseDescriptor], restoring them afterwards. This does not change
    /// the running commit log.
    private static <T> T withCommitLog(Config.DiskAccessMode accessMode,
                                       ParameterizedClass compression,
                                       EncryptionContext encryption,
                                       Supplier<T> body)
    {
        Config.DiskAccessMode previousAccessMode = DatabaseDescriptor.getCommitLogWriteDiskAccessMode();
        // the setter also writes the yaml value, which holds the configured (e.g. legacy), not the resolved, mode
        Config.DiskAccessMode previousConfiguredAccessMode = DatabaseDescriptor.getRawConfig().commitlog_disk_access_mode;
        ParameterizedClass previousCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext previousEncryption = DatabaseDescriptor.getEncryptionContext();
        setCommitLog(accessMode, compression, encryption);
        try
        {
            return body.get();
        }
        finally
        {
            setCommitLog(previousAccessMode, previousCompression, previousEncryption);
            DatabaseDescriptor.getRawConfig().commitlog_disk_access_mode = previousConfiguredAccessMode;
        }
    }

    /// Runs `body` with an uncompressed, unencrypted, memory-mapped commit log, which `commitlog` value storage
    /// requires, whatever the test yaml or the test target (e.g. `test-compression`) configures.
    private static <T> T withMmapCommitLog(Supplier<T> body)
    {
        return withCommitLog(Config.DiskAccessMode.mmap, null, EncryptionContextGenerator.createDisabledContext(), body);
    }

    private static void assumeNotOffheapObjects()
    {
        assumeFalse(DatabaseDescriptor.getMemtableAllocationType() == Config.MemtableAllocationType.offheap_objects);
        assumeFalse(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
    }

    private static void assertRejected(Map<String, String> options, String... messageParts)
    {
        try
        {
            TrieMemtable.factory(options);
            fail("expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            for (String part : messageParts)
                assertTrue(e.getMessage(), e.getMessage().contains(part));
        }
    }

    private static Map<String, String> options(String... kv)
    {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put(kv[i], kv[i + 1]);
        return m;
    }

    private static TableMetadataRef tableMetadataRef()
    {
        return TableMetadataRef.forOfflineTools(TableMetadata.builder("ks", "t")
                                                             .addPartitionKeyColumn("pk", Int32Type.instance)
                                                             .addRegularColumn("v", BytesType.instance)
                                                             .build());
    }

    @Test
    public void defaultsToNative()
    {
        Map<String, String> o = options("shards", "4");
        TrieMemtableFactory f = (TrieMemtableFactory) TrieMemtable.factory(o);
        assertEquals(TrieMemtable.ValueStorage.NATIVE, f.valueStorage);
        assertTrue("options consumed", o.isEmpty());
    }

    @Test
    public void parsesFile()
    {
        Map<String, String> o = options("value_storage", "file");
        TrieMemtableFactory f = withOffheapObjects(() -> (TrieMemtableFactory) TrieMemtable.factory(o));
        assertEquals(TrieMemtable.ValueStorage.FILE, f.valueStorage);
        assertTrue(o.isEmpty());
    }

    @Test
    public void parsesCommitlog()
    {
        Map<String, String> o = options("value_storage", "commitlog");
        TrieMemtableFactory f = withMmapCommitLog(() -> withOffheapObjects(() -> (TrieMemtableFactory) TrieMemtable.factory(o)));
        assertEquals(TrieMemtable.ValueStorage.COMMITLOG, f.valueStorage);
        assertTrue(o.isEmpty());
        assertTrue("value locations are recorded once commitlog value storage is accepted", SerializedValueOffsets.ENABLED);
    }

    @Test
    public void commitlogRejectedWithoutMmapCommitLog()
    {
        for (Config.DiskAccessMode accessMode : new Config.DiskAccessMode[]{ Config.DiskAccessMode.standard, Config.DiskAccessMode.direct })
        {
            withCommitLog(accessMode, null, EncryptionContextGenerator.createDisabledContext(), () -> withOffheapObjects(() -> {
                assertRejected(options("value_storage", "commitlog"), "value_storage", "commitlog_disk_access_mode", accessMode.name());
                // file value storage does not depend on the commit log
                TrieMemtable.factory(options("value_storage", "file"));
                return null;
            }));
        }
    }

    @Test
    public void commitlogRejectedWithCompressedCommitLog()
    {
        ParameterizedClass lz4 = new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap());
        withCommitLog(Config.DiskAccessMode.mmap, lz4, EncryptionContextGenerator.createDisabledContext(), () -> withOffheapObjects(() -> {
            assertRejected(options("value_storage", "commitlog"), "value_storage", "commitlog_compression");
            return null;
        }));
    }

    @Test
    public void commitlogRejectedWithEncryptedCommitLog()
    {
        EncryptionContext encryption = EncryptionContextGenerator.createContext(false);
        assertTrue(encryption.isEnabled());
        withCommitLog(Config.DiskAccessMode.mmap, null, encryption, () -> withOffheapObjects(() -> {
            assertRejected(options("value_storage", "commitlog"), "value_storage", "transparent_data_encryption_options");
            return null;
        }));
    }

    @Test
    public void commitlogRejectedWithoutOffheapObjects()
    {
        assumeNotOffheapObjects();
        withMmapCommitLog(() -> {
            assertRejected(options("value_storage", "commitlog"), "value_storage=commitlog", "offheap_objects");
            return null;
        });
    }

    @Test
    public void rejectedCommitlogLeavesValueLocationsDisabled()
    {
        ParameterizedClass lz4 = new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap());
        withCommitLog(Config.DiskAccessMode.mmap, lz4, EncryptionContextGenerator.createDisabledContext(), () -> withOffheapObjects(() -> {
            assertRejected(options("value_storage", "commitlog"), "commitlog_compression");
            return null;
        }));
        withCommitLog(Config.DiskAccessMode.mmap, null, EncryptionContextGenerator.createContext(false), () -> withOffheapObjects(() -> {
            assertRejected(options("value_storage", "commitlog"), "transparent_data_encryption_options");
            return null;
        }));
        withCommitLog(Config.DiskAccessMode.standard, null, EncryptionContextGenerator.createDisabledContext(), () -> withOffheapObjects(() -> {
            assertRejected(options("value_storage", "commitlog"), "commitlog_disk_access_mode");
            return null;
        }));
        if (DatabaseDescriptor.getMemtableAllocationType() != Config.MemtableAllocationType.offheap_objects)
            withMmapCommitLog(() -> {
                assertRejected(options("value_storage", "commitlog"), "offheap_objects");
                return null;
            });
        assertFalse(SerializedValueOffsets.ENABLED);
    }

    /// Offline tools do not resolve the commit log disk access mode and never write through a memtable: they must be
    /// able to load a schema or yaml that uses `commitlog`, and must not turn on value location recording.
    @Test
    public void commitlogAcceptedWithoutResolvedCommitLogAccessMode()
    {
        TrieMemtableFactory f = withCommitLog(null, null, EncryptionContextGenerator.createDisabledContext(),
                                              () -> withOffheapObjects(() -> (TrieMemtableFactory) TrieMemtable.factory(options("value_storage", "commitlog"))));
        assertEquals(TrieMemtable.ValueStorage.COMMITLOG, f.valueStorage);
        assertFalse(SerializedValueOffsets.ENABLED);
        // the memory pool is still checked
        if (DatabaseDescriptor.getMemtableAllocationType() != Config.MemtableAllocationType.offheap_objects)
            withCommitLog(null, null, EncryptionContextGenerator.createDisabledContext(), () -> {
                assertRejected(options("value_storage", "commitlog"), "offheap_objects");
                return null;
            });
    }

    @Test
    public void rejectsUnknownValueStorage()
    {
        assertRejected(options("value_storage", "tape"), "tape", "native, file, commitlog");
    }

    @Test
    public void factoryWarnsOncePerStorageKindThatValueStorageOutsideMemtableMemoryIsExperimental()
    {
        // file and commitlog each warn, naming themselves, the first time they are accepted, and neither warns again
        List<String> warnings = experimentalWarnings("native", "file", "commitlog", "file", "commitlog");
        assertEquals(warnings.toString(), 2, warnings.size());
        assertExperimentalWarning(warnings.get(0), "file");
        assertExperimentalWarning(warnings.get(1), "commitlog");

        warnings = experimentalWarnings("commitlog", "file", "commitlog");
        assertEquals(warnings.toString(), 2, warnings.size());
        assertExperimentalWarning(warnings.get(0), "commitlog");
        assertExperimentalWarning(warnings.get(1), "file");
    }

    private static void assertExperimentalWarning(String message, String valueStorage)
    {
        assertTrue(message, message.contains(TrieMemtable.VALUE_STORAGE_OPTION + '=' + valueStorage + ':'));
        assertTrue(message, message.contains("experimental"));
        assertTrue(message, message.contains("memtable_offheap_space"));
    }

    /// The warnings logged by creating a factory for each of the `valueStorages` in turn, starting from a state where
    /// no warning has been logged yet.
    private static List<String> experimentalWarnings(String... valueStorages)
    {
        TrieMemtable.ValueStorage.resetWarnings();
        List<ILoggingEvent> events = captureLog(TrieMemtable.class, () -> {
            for (String valueStorage : valueStorages)
            {
                Map<String, String> options = new HashMap<>();
                options.put(TrieMemtable.VALUE_STORAGE_OPTION, valueStorage);
                // commitlog is rejected unless the commit log is uncompressed, unencrypted and memory-mapped, which
                // depends on the test target
                withMmapCommitLog(() -> withOffheapObjects(() -> TrieMemtable.factory(options)));
            }
        });
        List<String> warnings = new ArrayList<>();
        for (ILoggingEvent event : events)
            if (event.getLevel() == Level.WARN)
                warnings.add(event.getFormattedMessage());
        return warnings;
    }

    @Test
    public void factoriesWithDifferentStorageAreNotEqual()
    {
        assertNotEquals(withOffheapObjects(() -> TrieMemtable.factory(options("value_storage", "file"))),
                        TrieMemtable.factory(options()));
        assertEquals(TrieMemtable.factory(options("value_storage", "native")),
                     TrieMemtable.factory(options()));
        assertNotEquals(withOffheapObjects(() -> TrieMemtable.factory(options("value_storage", "file"))),
                        withMmapCommitLog(() -> withOffheapObjects(() -> TrieMemtable.factory(options("value_storage", "commitlog")))));
    }

    @Test
    public void nativeMemtableHasNoValueLog()
    {
        TrieMemtable memtable = (TrieMemtable) TrieMemtable.factory(options())
                                                           .create(new AtomicReference<>(CommitLogPosition.NONE), tableMetadataRef(), OWNER);
        assertEquals(TrieMemtable.ValueStorage.NATIVE, memtable.valueStorage());
        assertEquals(0, memtable.valueLogBytes());
    }

    @Test
    public void fileStorageRejectedByFactoryWithoutOffheapObjects()
    {
        assumeNotOffheapObjects();
        assertRejected(options("value_storage", "file"), "value_storage", "offheap_objects");
    }

    @Test
    public void fileStorageRejectedByMemtableWithoutNativePool()
    {
        assumeNotOffheapObjects();
        // the factory accepts the configuration, but the memtable pool in this JVM is not a NativePool
        TrieMemtableFactory f = withOffheapObjects(() -> (TrieMemtableFactory) TrieMemtable.factory(options("value_storage", "file")));
        try
        {
            f.create(new AtomicReference<>(CommitLogPosition.NONE), tableMetadataRef(), OWNER);
            fail("expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("value_storage"));
            assertTrue(e.getMessage(), e.getMessage().contains("offheap_objects"));
        }
    }

    @Test
    public void valueLogGaugesSumRegisteredSources()
    {
        String bytesName = "org.apache.cassandra.metrics.TrieMemtable.Value log bytes.ks.value_log_gauges";
        String fallbackName = "org.apache.cassandra.metrics.TrieMemtable.Value log fallback bytes.ks.value_log_gauges";
        TrieMemtableMetricsView view = TrieMemtableMetricsView.getOrCreate("ks", "value_log_gauges");
        assertTrue(Metrics.getMetrics().containsKey(bytesName));
        assertTrue(Metrics.getMetrics().containsKey(fallbackName));
        assertEquals(0L, (long) view.valueLogBytes.getValue());
        assertEquals(0L, (long) view.valueLogFallbackBytes.getValue());

        TrieMemtableMetricsView.ValueLogSource a = valueLogSource(5, 3);
        TrieMemtableMetricsView.ValueLogSource b = valueLogSource(7, 40);
        view.addValueLogSource(a);
        view.addValueLogSource(b);
        assertEquals(12L, (long) view.valueLogBytes.getValue());
        assertEquals(43L, (long) view.valueLogFallbackBytes.getValue());
        view.removeValueLogSource(a);
        assertEquals(7L, (long) view.valueLogBytes.getValue());
        assertEquals(40L, (long) view.valueLogFallbackBytes.getValue());

        view.release();
        assertFalse(Metrics.getMetrics().containsKey(bytesName));
        assertFalse(Metrics.getMetrics().containsKey(fallbackName));
    }

    private static TrieMemtableMetricsView.ValueLogSource valueLogSource(long bytes, long fallbackBytes)
    {
        return new TrieMemtableMetricsView.ValueLogSource()
        {
            @Override
            public long valueLogBytes()
            {
                return bytes;
            }

            @Override
            public long valueLogFallbackBytes()
            {
                return fallbackBytes;
            }
        };
    }

    private static final Memtable.Owner OWNER = new Memtable.Owner()
    {
        public Future<CommitLogPosition> signalFlushRequired(Memtable m, ColumnFamilyStore.FlushReason r) { return null; }
        public Memtable getCurrentMemtable() { return null; }
        public Iterable<Memtable> getIndexMemtables() { return Collections.emptyList(); }
        public ShardBoundaries localRangeSplits(int shardCount) { return ShardBoundaries.NONE; }
        public OpOrder readOrdering() { return null; }
        public int getMemtableFlushPeriodInMs() { return Integer.MAX_VALUE; }
    };
}
