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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.Util.randomBytes;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.awaitNoValueFiles;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.useSmallMemtablePool;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.valueFileCount;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeAndVerifyMixedRows;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeCounters;
import static org.apache.cassandra.db.memtable.ValueStorageTestSupport.writeSmallValues;
import static org.junit.Assert.*;

/// End-to-end tests of `value_storage: file` (the `trie_file` memtable of the test yaml): values written through CQL
/// are stored in per-shard value files, read back before and after a flush, and charged to the memtable's memory, so
/// that the memtable memory limit flushes a memtable whose values fill the pool; their files and charges are released
/// once the memtable is discarded.
public class TrieMemtableValueLogTest extends CQLTester
{
    // Overrides CQLTester.setUpClass to run before it
    @BeforeClass
    public static void setUpClass()
    {
        // both before the memtable pool is created
        useSmallMemtablePool();
        // same mechanism MemtableSizeOffheapObjectsTest uses to force the allocation type
        MemtableSizeTestBase.setup(Config.MemtableAllocationType.offheap_objects, Murmur3Partitioner.instance);
    }

    @Test
    public void valuesTriggerTheMemtableLimitFlush() throws Throwable
    {
        ValueStorageTestSupport.valuesTriggerTheMemtableLimitFlush(this, TrieMemtable.ValueStorage.FILE);
    }

    @Test
    public void valuesAreChargedUntilTheMemtableIsDiscarded() throws Throwable
    {
        ValueStorageTestSupport.valuesAreChargedUntilTheMemtableIsDiscarded(this, TrieMemtable.ValueStorage.FILE);
    }

    @Test
    public void mixedRowsReadBackBeforeAndAfterFlush() throws Throwable
    {
        writeAndVerifyMixedRows(this, TrieMemtable.ValueStorage.FILE);
    }

    @Test
    public void smallValuesStayEmbeddedAndCountersUseTheValueLog() throws Throwable
    {
        writeSmallValues(this, TrieMemtable.ValueStorage.FILE);
        writeCounters(this, TrieMemtable.ValueStorage.FILE);
    }

    @Test
    public void valueFilesAreRemovedAfterFlushAndDiscard() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH memtable = 'trie_file'");
        execute("INSERT INTO %s (pk, v) VALUES (0, ?)", randomBytes(4096, 1));
        TableMetadata table = getCurrentColumnFamilyStore().metadata();
        assertTrue(valueFileCount(table) >= 1);
        flush();
        awaitNoValueFiles(table);
        assertEquals(randomBytes(4096, 1), execute("SELECT v FROM %s WHERE pk = 0").one().getBytes("v"));
    }
}
