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

package org.apache.cassandra.db.tries.mvcc;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.Util.captureLog;
import static org.apache.cassandra.Util.randomBytes;
import static org.junit.Assert.*;

public class FileValueStoreTest
{
    private static final long REGION_SIZE = 1 << 20;   // rather than FileValueStore.REGION_SIZE, to keep the files small

    private File dir;
    private File file;
    private FileValueStore store;

    @Before
    public void setUp() throws Exception
    {
        dir = new File(Files.createTempDirectory("value-log-test"));
        file = new File(dir, "ks-tbl-1-0.values");
        store = new FileValueStore(file, REGION_SIZE);
    }

    @After
    public void tearDown()
    {
        store.close();
        dir.deleteRecursive();
    }

    @Test
    public void appendThenReadRoundTrips()
    {
        ByteBuffer v1 = randomBytes(100, 1), v2 = randomBytes(5000, 2);
        long h1 = store.append(v1.duplicate(), null);
        long h2 = store.append(v2.duplicate(), null);
        assertEquals(0, h1);
        assertEquals(100, h2);
        assertEquals(5100, store.bytesWritten());
        assertEquals(v1, store.read(h1, 100));
        assertEquals(v2, store.read(h2, 5000));
        assertEquals(1, store.regionCount());
        assertTrue(store.exists());
    }

    @Test
    public void appendDoesNotMoveSourcePosition()
    {
        ByteBuffer v = randomBytes(64, 3);
        store.append(v, null);
        assertEquals(0, v.position());
    }

    @Test
    public void readReturnsBigEndianViewNotCopy()
    {
        ByteBuffer v = ByteBuffer.allocate(8).putLong(0, 0x0102030405060708L);
        long h = store.append(v.duplicate(), null);
        ByteBuffer r = store.read(h, 8);
        assertEquals(ByteOrder.BIG_ENDIAN, r.order());
        assertEquals(0x0102030405060708L, r.getLong(0));
        assertTrue(r.isDirect());
    }

    @Test
    public void closeUnmapsAndDeletesAndIsIdempotent()
    {
        store.append(randomBytes(10, 4), null);
        assertTrue(file.exists());
        store.close();
        assertFalse(file.exists());
        assertEquals(0, store.regionCount());
        assertFalse(store.exists());
        store.close(); // second close must not throw
    }

    @Test
    public void closeToleratesFileAlreadyDeleted()
    {
        store.append(randomBytes(10, 4), null);
        assertTrue(file.tryDelete());
        store.close();
        assertFalse(store.exists());
    }

    @Test
    public void secondStoreOnSameFileFailsAndLeavesFirstIntact()
    {
        ByteBuffer v = randomBytes(100, 9);
        long h = store.append(v.duplicate(), null);
        FileValueStore other = new FileValueStore(file, REGION_SIZE);
        try
        {
            other.append(randomBytes(100, 10), null);
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException e)
        {
            // expected: the name collides with a live store
        }
        other.close();
        assertTrue(file.exists());
        assertEquals(v, store.read(h, 100));
    }

    @Test
    public void closeWithoutAppendDoesNotCreateFile()
    {
        assertFalse(store.exists());
        store.close();
        assertFalse(file.exists());
    }

    @Test
    public void valueExactlyFillingRegionStaysInRegion()
    {
        int first = (int) (REGION_SIZE - 4096);
        long h1 = store.append(ByteBuffer.allocate(first), null);
        long h2 = store.append(randomBytes(4096, 5), null);           // fills the region exactly
        assertEquals(0, h1);
        assertEquals(first, h2);
        assertEquals(1, store.regionCount());
        assertEquals(randomBytes(4096, 5), store.read(h2, 4096));
    }

    @Test
    public void valueNotFittingRegionTailStartsNextRegion()
    {
        int first = (int) (REGION_SIZE - 100);
        store.append(ByteBuffer.allocate(first), null);
        long h2 = store.append(randomBytes(200, 6), null);
        assertEquals(REGION_SIZE, h2);        // tail of 100 bytes wasted
        assertEquals(2, store.regionCount());
        assertEquals(randomBytes(200, 6), store.read(h2, 200));
    }

    @Test
    public void valueLargerThanRegionGetsDedicatedRegion()
    {
        int big = (int) (REGION_SIZE + 12345);
        long h = store.append(randomBytes(big, 7), null);
        assertEquals(0, h);
        assertEquals(1, store.regionCount());
        assertEquals(randomBytes(big, 7), store.read(h, big));
        long h2 = store.append(randomBytes(8, 8), null);
        assertEquals(2, store.regionCount());
        assertEquals(randomBytes(8, 8), store.read(h2, 8));
    }

    @Test
    public void readersSeeOldRegionsWhileWriterGrows() throws Exception
    {
        int n = 4000;
        int len = 1024;                                        // 4000 × 1 KiB ≈ 4 regions of 1 MiB
        ByteBuffer[] expected = new ByteBuffer[97];            // value i is expected[i % 97]
        for (int k = 0; k < expected.length; k++)
            expected[k] = randomBytes(len, k);
        long[] handles = new long[n];
        AtomicInteger published = new AtomicInteger();         // handles[0, published) are readable
        for (int i = 0; i < 100; i++)
            handles[i] = store.append(expected[i % expected.length].duplicate(), null);
        published.set(100);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        Thread reader = new Thread(() -> {
            try
            {
                Random random = new Random(11);
                while (true)
                {
                    boolean last = stop.get();
                    int p = published.get();
                    // the latest value, often in a region added after this thread started, and an older one
                    for (int i : new int[]{ p - 1, random.nextInt(p) })
                        if (!expected[i % expected.length].equals(store.read(handles[i], len)))
                            throw new AssertionError("corrupt read of " + i);
                    if (last)
                        break;
                }
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        reader.start();
        for (int i = 100; i < n; i++)
        {
            handles[i] = store.append(expected[i % expected.length].duplicate(), null);
            published.set(i + 1);
        }
        stop.set(true);
        reader.join();
        assertNull(failure.get());
        assertTrue(store.regionCount() >= 3);
        for (int i = 0; i < n; i += 97)
            assertEquals(expected[i % expected.length], store.read(handles[i], len));
    }

    @Test
    public void bytesWrittenIsTheSumOfAppendedLengths()
    {
        int[] lengths = { 70000, 100, (int) REGION_SIZE - 50, 200, (int) REGION_SIZE + 12345, 8 };
        long[] handles = new long[lengths.length];
        long sum = 0;
        for (int i = 0; i < lengths.length; i++)
        {
            handles[i] = store.append(randomBytes(lengths[i], i), null);
            sum += lengths[i];
            assertEquals(sum, store.bytesWritten());
        }
        assertTrue(store.regionCount() >= 4);
        for (int i = 0; i < lengths.length; i++)
            assertEquals(randomBytes(lengths[i], i), store.read(handles[i], lengths[i]));
    }

    @Test
    public void appendRecreatesDeletedDirectory()
    {
        DatabaseDescriptor.daemonInitialization();
        File d = FileValueStore.directory();
        d.deleteRecursive();
        TableId tableId = TableId.fromUUID(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
        FileValueStore recreated = new FileValueStore(FileValueStore.fileFor("ks", "tbl", tableId, 8, 0), REGION_SIZE);
        try
        {
            long h = recreated.append(randomBytes(100, 1), null);
            assertEquals(randomBytes(100, 1), recreated.read(h, 100));
            assertTrue(d.isDirectory());
        }
        finally
        {
            recreated.close();
        }
    }

    @Test
    public void onHeapSizeGrowsWithRegions()
    {
        long before = store.onHeapSize();
        int big = (int) REGION_SIZE;
        store.append(ByteBuffer.allocate(big), null);
        store.append(ByteBuffer.allocate(big), null);
        assertTrue(store.onHeapSize() > before);
    }

    @Test
    public void deleteStaleFilesRemovesOnlyValueFiles() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        File d = FileValueStore.directory();
        assertTrue(d.isDirectory());
        assertSame("the directory is created once and then cached", d, FileValueStore.directory());
        File stale = new File(d, "ks-tbl-99-0.values");
        File foreign = new File(d, "keep.txt");
        File undeletable = new File(d, "ks-tbl-99-1.values");   // a non-empty directory, which cannot be deleted
        Files.write(stale.toPath(), new byte[10]);
        Files.write(foreign.toPath(), new byte[10]);
        undeletable.createDirectoriesIfNotExists();
        Files.write(new File(undeletable, "x").toPath(), new byte[10]);

        List<ILoggingEvent> events = captureLog(FileValueStore.class, () -> assertEquals(1, FileValueStore.deleteStaleFiles()));
        assertFalse(stale.exists());
        assertTrue(foreign.exists());
        assertTrue(undeletable.exists());
        assertEquals(1, events.size());
        assertEquals(Level.WARN, events.get(0).getLevel());
        assertEquals(undeletable, events.get(0).getArgumentArray()[0]);
        foreign.delete();
        undeletable.deleteRecursive();

        TableId tableId = TableId.fromUUID(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
        long jvmStart = ManagementFactory.getRuntimeMXBean().getStartTime();
        assertEquals("ks-tbl-0123456789abcdef0123456789abcdef-" + jvmStart + "-7-3.values",
                     FileValueStore.fileFor("ks", "tbl", tableId, 7, 3).name());
    }
}
