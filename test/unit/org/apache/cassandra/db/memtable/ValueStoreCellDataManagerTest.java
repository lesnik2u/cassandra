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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.db.tries.mvcc.FileValueStore;
import org.apache.cassandra.db.tries.mvcc.ValueStore;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.Util.randomBytes;
import static org.junit.Assert.*;

public class ValueStoreCellDataManagerTest
{
    private File dir;
    private FileValueStore store;
    private ValueStoreCellDataManager manager;

    @Before
    public void setUp() throws Exception
    {
        dir = new File(Files.createTempDirectory("value-store-cdm-test"));
        store = new FileValueStore(new File(dir, "ks-t-1-0.values"), 1 << 20);
        manager = new ValueStoreCellDataManager(store);
    }

    @After
    public void tearDown()
    {
        manager.close();
        dir.deleteRecursive();
    }

    @Test
    public void storeAndLoad()
    {
        ByteBuffer a = randomBytes(300, 1), b = randomBytes(17, 2);
        long ha = manager.store(a.duplicate(), 300);
        long hb = manager.store(b.duplicate(), 17);
        assertNotEquals(ha, hb);
        assertEquals(a, manager.load(ha, 300));
        assertEquals(b, manager.load(hb, 17));
        assertEquals(317, manager.valueLogBytes());
        assertEquals(317, store.bytesWritten());
    }

    @Test
    public void noRecyclingNoReleaseNoOnHeapData()
    {
        ByteBuffer a = randomBytes(100, 3);
        long ha = manager.store(a.duplicate(), 100);
        assertFalse(manager.releaseNeeded());
        manager.release(ha, 100);              // must be a no-op
        manager.completeMutation();
        manager.abortMutation();
        manager.releaseReferencesUnsafe();
        assertEquals(a, manager.load(ha, 100));
        assertEquals(0, manager.unusedReservedOnHeapMemory());
        assertEquals(store.onHeapSize(), manager.onHeapSize());
    }

    @Test
    public void closeWithoutStoreCreatesNothingAndIsIdempotent()
    {
        manager.close();
        assertFalse(new File(dir, "ks-t-1-0.values").exists());
        manager.close();
    }

    @Test
    public void closeAfterStoreDeletesFile()
    {
        manager.store(randomBytes(10, 3), 10);
        assertTrue(new File(dir, "ks-t-1-0.values").exists());
        manager.close();
        assertFalse(new File(dir, "ks-t-1-0.values").exists());
        assertEquals(10, manager.valueLogBytes());   // still readable after close: the metrics gauge may read it after the discard
    }

    @Test
    public void storePassesTheCurrentValueLocationsToTheStore()
    {
        RecordingStore recordingStore = new RecordingStore();
        ValueStoreCellDataManager recording = new ValueStoreCellDataManager(recordingStore);
        CommitLogValueLocations valueLocations = new CommitLogValueLocations(1, new File(dir, "CommitLog-7-1.log"), 1 << 20, 100,
                                                                             new SerializedValueOffsets());

        recording.store(randomBytes(20, 1), 20);
        recording.valueLocations = valueLocations;
        recording.store(randomBytes(20, 2), 20);
        recording.store(randomBytes(20, 3), 20);
        recording.valueLocations = null;
        recording.store(randomBytes(20, 4), 20);

        assertEquals(Arrays.asList(null, valueLocations, valueLocations, null), recordingStore.appendedValueLocations);
    }

    @Test
    public void fallbackBytesComeFromTheStore()
    {
        assertEquals(0, manager.fallbackBytes());   // a store that never references values
        RecordingStore recordingStore = new RecordingStore();
        recordingStore.fallbackBytes = 42;
        assertEquals(42, new ValueStoreCellDataManager(recordingStore).fallbackBytes());
    }

    /// Records the value locations of every append; keeps no values.
    private static class RecordingStore implements ValueStore
    {
        final List<CommitLogValueLocations> appendedValueLocations = new ArrayList<>();
        long fallbackBytes;

        @Override
        public long append(ByteBuffer value, CommitLogValueLocations valueLocations)
        {
            appendedValueLocations.add(valueLocations);
            return appendedValueLocations.size();
        }

        @Override
        public ByteBuffer read(long handle, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long bytesWritten()
        {
            return 0;
        }

        @Override
        public long fallbackBytes()
        {
            return fallbackBytes;
        }

        @Override
        public long onHeapSize()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    }
}
