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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;

import static org.junit.Assert.assertTrue;

/**
 * {@link ResourceAccountingNoFalsePositiveTest} with {@code offheap_objects}, so that the memtable pool's off-heap
 * sub-pool, which the tests' {@code heap_buffers} leaves unused, is checked too. It needs a JVM of its own, as the
 * memtable pool is created once per JVM, from the allocation type configured when it is first used.
 */
public class ResourceAccountingNoFalsePositiveOffHeapObjectsTest extends ResourceAccountingNoFalsePositiveTest
{
    // hides CQLTester.setUpClass, to set the allocation type before anything creates the memtable pool
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        prepareServer();
    }

    @Test
    public void testTheMemtablePoolIsOffHeap()
    {
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        assertTrue(pool.getClass().getName(), pool instanceof NativePool);
        assertTrue(pool.offHeap.limit > 0);
    }
}
