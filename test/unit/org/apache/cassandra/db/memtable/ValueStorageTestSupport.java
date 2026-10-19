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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.ILoggingEvent;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.tries.mvcc.CommitLogSegmentMappings;
import org.apache.cassandra.db.tries.mvcc.FileValueStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.Util.randomBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Helpers shared by the tests of trie memtable value storage outside memtable memory (`value_storage: file` and
/// `commitlog`): configuration overrides, value file checks, and the end-to-end scenarios that both storages must pass.
public final class ValueStorageTestSupport
{
    private static final Logger logger = LoggerFactory.getLogger(ValueStorageTestSupport.class);

    static final int TIMEOUT_SECONDS = 30;

    /// The memtable pool's off-heap limit set by [#useSmallMemtablePool]: small enough for a test to fill.
    static final int SMALL_OFFHEAP_SPACE_MIB = 64;

    // logged by AbstractAllocatorMemtable.flushLargestMemtable
    private static final String FLUSHING_LARGEST_MESSAGE = "Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}";

    /// The partition of the 20-byte values written with and without a TTL by [#writeAndVerifyMixedRows].
    private static final int TTL_PK = 100;

    private ValueStorageTestSupport()
    {
    }

    /// Whether `valueStorage` accepts the configuration of this test run: `file` and `commitlog` need
    /// `memtable_allocation_type: offheap_objects`, and `commitlog` an uncompressed, unencrypted, memory-mapped commit
    /// log.
    public static boolean isAccepted(TrieMemtable.ValueStorage valueStorage)
    {
        try
        {
            valueStorage.validate();
            return true;
        }
        catch (ConfigurationException e)
        {
            return false;
        }
    }

    /// Configures an uncompressed, unencrypted, memory-mapped commit log in [DatabaseDescriptor] for the rest of the
    /// run, as `commitlog` value storage requires, whatever the test yaml or the test target (e.g. `test-compression`)
    /// configures. Must be called before the commit log starts.
    public static void forceMmapCommitLog()
    {
        logger.info("Overriding the commit log configuration (compression {}, encryption {}, access mode {}) with an uncompressed, unencrypted mmap commit log",
                    DatabaseDescriptor.getCommitLogCompression(), DatabaseDescriptor.getEncryptionContext(),
                    DatabaseDescriptor.getCommitLogWriteDiskAccessMode());
        setCommitLog(Config.DiskAccessMode.mmap, null, EncryptionContextGenerator.createDisabledContext());
    }

    /// Sets the commit log configuration that `commitlog` value storage checks in [DatabaseDescriptor]: the resolved
    /// disk access mode (null for unresolved, as in offline tools), compression and encryption. This does not change
    /// the running commit log.
    static void setCommitLog(Config.DiskAccessMode accessMode, ParameterizedClass compression, EncryptionContext encryption)
    {
        DatabaseDescriptor.setCommitLogWriteDiskAccessMode(accessMode);
        DatabaseDescriptor.setCommitLogCompression(compression);
        DatabaseDescriptor.setEncryptionContext(encryption);
    }

    /// The current memtable of `cfs`, which must keep its values in `expected`: a value storage configuration that the
    /// memtable factory rejects falls back to the default memtable without failing CREATE TABLE, which would leave the
    /// value storage untested.
    public static TrieMemtable currentMemtable(ColumnFamilyStore cfs, TrieMemtable.ValueStorage expected)
    {
        TrieMemtable memtable = (TrieMemtable) cfs.getCurrentMemtable();
        assertEquals(expected, memtable.valueStorage());
        return memtable;
    }

    /// Checks the value storage of the current memtable of `cfs` as [#currentMemtable] does if `memtableClass` is the
    /// `trie_file` or `trie_commitlog` memtable of the test yaml.
    public static void assertValueStorage(String memtableClass, ColumnFamilyStore cfs)
    {
        for (TrieMemtable.ValueStorage valueStorage : TrieMemtable.ValueStorage.values())
        {
            if (valueStorage != TrieMemtable.ValueStorage.NATIVE && memtableClass.equals(memtableClass(valueStorage)))
                currentMemtable(cfs, valueStorage);
        }
    }

    /// The memtable of the test yaml that keeps its values in `valueStorage`: `trie_file` or `trie_commitlog`.
    private static String memtableClass(TrieMemtable.ValueStorage valueStorage)
    {
        return "trie_" + valueStorage.name().toLowerCase(Locale.ROOT);
    }

    /// The off-heap memory `memtable` owns: its trie, and what it keeps outside memtable memory and charges as such.
    public static long offHeapOwned(Memtable memtable)
    {
        Memtable.MemoryUsage usage = Memtable.newMemoryUsage();
        memtable.addMemoryUsageTo(usage);
        return usage.ownsOffHeap;
    }

    /// Sets the memtable pool's off-heap limit to [#SMALL_OFFHEAP_SPACE_MIB]. Must be called before the pool is
    /// created, i.e. before the server is prepared.
    static void useSmallMemtablePool()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.getRawConfig().memtable_offheap_space = new DataStorageSpec.IntMebibytesBound(SMALL_OFFHEAP_SPACE_MIB);
    }

    /// Writes 70 values of 1 MiB to a new table whose memtable keeps its values in `valueStorage`, in a memtable pool
    /// set up by [#useSmallMemtablePool] with `memtable_cleanup_threshold` 0.8, and checks that the value bytes the
    /// memtable is charged for make the memtable cleaner flush it, once, as it would flush a memtable that kept them.
    public static void valuesTriggerTheMemtableLimitFlush(CQLTester tester, TrieMemtable.ValueStorage valueStorage) throws Throwable
    {
        assertEquals((long) SMALL_OFFHEAP_SPACE_MIB << 20, AbstractAllocatorMemtable.MEMORY_POOL.offHeap.limit);
        tester.createTable("CREATE TABLE %s (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtableClass(valueStorage) + "'");
        ColumnFamilyStore cfs = tester.getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, valueStorage);
        long flushesBefore = cfs.metric.memtableSwitchCount.getCount();
        int valueSize = 1 << 20;   // about 52 of them pass the cleanup threshold
        List<ILoggingEvent> events = Util.captureLog(AbstractAllocatorMemtable.class, appender -> {
            for (int i = 0; i < 70; i++)
                tester.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", i, 0, randomBytes(valueSize, i));
            // flush is asynchronous: the memtable is switched when the flush is scheduled, but memtableSwitchCount is
            // only incremented when the flush task runs, so wait for the latter
            Util.spinAssertEquals(flushesBefore + 1, () -> cfs.metric.memtableSwitchCount.getCount(), TIMEOUT_SECONDS);
        });
        assertNotSame("the memory limit should have switched the memtable", memtable, cfs.getCurrentMemtable());
        boolean flushedByCleaner = false;
        for (ILoggingEvent event : events)
            flushedByCleaner |= FLUSHING_LARGEST_MESSAGE.equals(event.getMessage()) && cfs == event.getArgumentArray()[0];
        assertTrue("the memtable cleaner did not flush " + cfs + ": " + events, flushedByCleaner);

        // data must be intact across memtable and sstable
        for (int i = 0; i < 70; i++)
            assertEquals(randomBytes(valueSize, i), tester.execute("SELECT v FROM %s WHERE pk = ? AND ck = 0", i).one().getBytes("v"));
        assertEquals(flushesBefore + 1, cfs.metric.memtableSwitchCount.getCount());
    }

    /// Writes values to a new table whose memtable keeps them in `valueStorage`, through CQL and with puts that carry
    /// no commit log locations, which `commitlog` storage copies into its fallback, and checks that the memtable's
    /// own allocators are charged every value byte its stores hold, overwritten values included, plus its trie, and
    /// that the discard returns all of it.
    public static void valuesAreChargedUntilTheMemtableIsDiscarded(CQLTester tester, TrieMemtable.ValueStorage valueStorage) throws Throwable
    {
        tester.createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = '" + memtableClass(valueStorage) + "'");
        ColumnFamilyStore cfs = tester.getCurrentColumnFamilyStore();
        TrieMemtable memtable = currentMemtable(cfs, valueStorage);
        int valueSize = 64 << 10;
        for (int i = 0; i < 40; i++)
            tester.execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i % 20, randomBytes(valueSize, i));
        for (int i = 0; i < 10; i++)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfs.metadata(), 100 + i);
            builder.row().add("v", randomBytes(valueSize, 100 + i));
            try (OpOrder.Group group = Keyspace.writeOrder.start())
            {
                memtable.put(builder.build(), UpdateTransaction.NO_OP, group);
            }
        }
        long valueBytes = 50L * valueSize;
        assertEquals(valueBytes, memtable.valueLogBytes());
        long fallbackBytes = valueStorage == TrieMemtable.ValueStorage.COMMITLOG ? 10L * valueSize : 0;
        assertEquals(fallbackBytes, memtable.valueLogFallbackBytes());
        // the tries of 30 partitions take far less than a MiB
        long owned = offHeapOwned(memtable);
        assertTrue(owned + " owned for " + valueBytes + " value bytes", owned >= valueBytes && owned < valueBytes + (1 << 20));

        tester.flush();
        Util.spinAssertEquals("the discarded memtable still owns memory", 0L, () -> offHeapOwned(memtable), TIMEOUT_SECONDS, TimeUnit.SECONDS);
        awaitNoValueFiles(cfs.metadata());
    }

    /// The number of value files of `table` in [FileValueStore#directory].
    public static int valueFileCount(TableMetadata table)
    {
        return FileValueStore.filesOf(table).length;
    }

    /// Waits until every value file of `table` has been removed: a memtable is discarded on the reclaim executor after
    /// the read barrier that follows its flush.
    public static void awaitNoValueFiles(TableMetadata table)
    {
        Util.spinAssertEquals("value files of " + table + " were not removed", 0, () -> valueFileCount(table), TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /// Waits until no commit log segment is mapped: a memtable releases its mappings when it is discarded, on the
    /// reclaim executor after the read barrier that follows its flush.
    public static void awaitNoMappings(String message)
    {
        Util.spinAssertEquals(message, 0, CommitLogSegmentMappings::liveCount, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /// Writes rows mixing values embedded in the trie cell and values that are not (statics, an overwrite with a
    /// bigger value, row, cell and range deletions, short values with and without a TTL) to a new table whose memtable
    /// keeps its values in `valueStorage`, and checks them before and after a flush. Every value is written by CQL, so
    /// none is copied into the fallback of `commitlog` value storage.
    public static void writeAndVerifyMixedRows(CQLTester tester, TrieMemtable.ValueStorage valueStorage) throws Throwable
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, small int, big blob, s blob static, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtableClass(valueStorage) + "'");
        TrieMemtable memtable = currentMemtable(tester.getCurrentColumnFamilyStore(), valueStorage);
        for (int pk = 0; pk < 20; pk++)
        {
            tester.execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, expectedStatic(pk));
            for (int ck = 0; ck < 50; ck++)
                tester.execute("INSERT INTO %s (pk, ck, small, big) VALUES (?, ?, ?, ?)", pk, ck, ck, randomBytes(1024 + 37 * ck, pk * 1000 + ck));
        }
        tester.execute("UPDATE %s SET big = ? WHERE pk = 3 AND ck = 7", randomBytes(70000, 999));   // overwrite with a bigger value
        tester.execute("DELETE FROM %s WHERE pk = 5 AND ck = 9");
        tester.execute("DELETE big FROM %s WHERE pk = 6 AND ck = 1");
        tester.execute("DELETE FROM %s WHERE pk = 7 AND ck > 40");

        // A 17-24 byte value is embedded in the trie cell only if the cell has no TTL (TrieCellData.cellValueCanBeEmbedded).
        long valueLogBytes = memtable.valueLogBytes();
        tester.execute("INSERT INTO %s (pk, ck, big) VALUES (?, 0, ?) USING TTL 86400", TTL_PK, expectedBig(TTL_PK, 0));
        assertEquals("a 20-byte value with a TTL goes to the value log", valueLogBytes + 20, memtable.valueLogBytes());
        tester.execute("INSERT INTO %s (pk, ck, big) VALUES (?, 1, ?)", TTL_PK, expectedBig(TTL_PK, 1));
        assertEquals("a 20-byte value without a TTL is embedded", valueLogBytes + 20, memtable.valueLogBytes());

        assertTrue("values should be in the value log", memtable.valueLogBytes() > 20 * 50 * 1024);
        assertEquals("no value of a CQL write is copied into a fallback", 0, memtable.valueLogFallbackBytes());
        verifyMixedRows(tester);
        tester.flush();
        verifyMixedRows(tester);
    }

    /// The value of `big` written by [#writeAndVerifyMixedRows], or null if the row or the value was deleted.
    private static ByteBuffer expectedBig(int pk, int ck)
    {
        if (pk == TTL_PK)
            return ck == 0 ? randomBytes(20, 4242) : randomBytes(20, 4343);
        if (pk == 3 && ck == 7)
            return randomBytes(70000, 999);
        if (pk == 5 && ck == 9 || pk == 6 && ck == 1 || pk == 7 && ck > 40)
            return null;
        return randomBytes(1024 + 37 * ck, pk * 1000 + ck);
    }

    private static ByteBuffer expectedStatic(int pk)
    {
        return pk == TTL_PK ? null : randomBytes(2048, -1 - pk);
    }

    private static void verifyMixedRows(CQLTester tester) throws Throwable
    {
        assertEquals(randomBytes(70000, 999), tester.execute("SELECT big FROM %s WHERE pk = 3 AND ck = 7").one().getBytes("big"));
        assertTrue(tester.execute("SELECT * FROM %s WHERE pk = 5 AND ck = 9").isEmpty());
        assertNull(tester.execute("SELECT big FROM %s WHERE pk = 6 AND ck = 1").one().getBytes("big"));
        assertEquals(41, tester.execute("SELECT ck FROM %s WHERE pk = 7").size());
        assertEquals(expectedStatic(11), tester.execute("SELECT s FROM %s WHERE pk = 11 LIMIT 1").one().getBytes("s"));
        for (int ck = 0; ck < 50; ck += 7)
            assertEquals(randomBytes(1024 + 37 * ck, 2 * 1000 + ck), tester.execute("SELECT big FROM %s WHERE pk = 2 AND ck = ?", ck).one().getBytes("big"));
        for (int ck = 0; ck < 2; ck++)
            assertEquals(expectedBig(TTL_PK, ck), tester.execute("SELECT big FROM %s WHERE pk = ? AND ck = ?", TTL_PK, ck).one().getBytes("big"));

        // reverse scan of one partition
        UntypedResultSet reversed = tester.execute("SELECT ck, big, s FROM %s WHERE pk = 4 ORDER BY ck DESC");
        assertEquals(50, reversed.size());
        int expectedCk = 49;
        for (UntypedResultSet.Row row : reversed)
        {
            assertEquals(expectedCk, row.getInt("ck"));
            assertEquals(expectedBig(4, expectedCk), row.getBytes("big"));
            assertEquals(expectedStatic(4), row.getBytes("s"));
            --expectedCk;
        }

        // forward full scan
        UntypedResultSet all = tester.execute("SELECT pk, ck, big, s FROM %s");
        assertEquals(20 * 50 - 1 - 9 + 2, all.size());
        Set<String> seen = new HashSet<>();
        for (UntypedResultSet.Row row : all)
        {
            int pk = row.getInt("pk");
            int ck = row.getInt("ck");
            assertTrue("row " + pk + ':' + ck + " seen twice", seen.add(pk + ":" + ck));
            assertEquals("row " + pk + ':' + ck, expectedBig(pk, ck), row.getBytes("big"));
            assertEquals("row " + pk + ':' + ck, expectedStatic(pk), row.getBytes("s"));
        }
    }

    /// Writes 1000 rows of 8-byte values to a new table whose memtable keeps its values in `valueStorage` and checks
    /// that every value is embedded in the trie: nothing reaches the value log.
    public static void writeSmallValues(CQLTester tester, TrieMemtable.ValueStorage valueStorage) throws Throwable
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, v bigint, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtableClass(valueStorage) + "'");
        TrieMemtable memtable = currentMemtable(tester.getCurrentColumnFamilyStore(), valueStorage);
        for (int i = 0; i < 1000; i++)
            tester.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", i, i, (long) i);
        assertEquals("8-byte values are embedded in the trie", 0, memtable.valueLogBytes());
        assertEquals(0, memtable.valueLogFallbackBytes());
    }

    /// Increments 10 counters 100 times in a new table whose memtable keeps its values in `valueStorage`. A counter
    /// cell holds a counter context with one global shard, too large to be embedded in the trie cell, so every update
    /// stores it in the value log, and none is copied into a fallback.
    public static void writeCounters(CQLTester tester, TrieMemtable.ValueStorage valueStorage) throws Throwable
    {
        tester.createTable("CREATE TABLE %s (pk int PRIMARY KEY, c counter) WITH memtable = '" + memtableClass(valueStorage) + "'");
        TrieMemtable memtable = currentMemtable(tester.getCurrentColumnFamilyStore(), valueStorage);
        for (int i = 0; i < 100; i++)
            tester.execute("UPDATE %s SET c = c + 1 WHERE pk = ?", i % 10);
        int counterContextSize = CounterContext.instance().createGlobal(CounterId.getLocalId(), 1, 1).remaining();
        assertEquals(100L * counterContextSize, memtable.valueLogBytes());
        assertEquals(0, memtable.valueLogFallbackBytes());
        assertEquals(10L, tester.execute("SELECT c FROM %s WHERE pk = 3").one().getLong("c"));
    }
}
