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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.db.commitlog.SerializedValueOffsets;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.Util.randomBytes;
import static org.apache.cassandra.db.tries.mvcc.CommitLogValueStore.FALLBACK_TAG;
import static org.apache.cassandra.db.tries.mvcc.CommitLogValueStore.OFFSET_BITS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitLogValueStoreTest
{
    private static final int SEGMENT_SIZE = 1 << 20;
    private static final long REGION_SIZE = 1 << 20;
    private static final long OFFSET_MASK = (1L << OFFSET_BITS) - 1;

    private File dir;
    private File fallbackFile;
    private FileValueStore fallback;
    private CommitLogValueStore store;
    private int baseline;

    @Before
    public void setUp() throws Exception
    {
        dir = new File(Files.createTempDirectory("commitlog-value-store-test"));
        fallbackFile = new File(dir, "ks-tbl-1-0.values");
        fallback = new FileValueStore(fallbackFile, REGION_SIZE);
        store = new CommitLogValueStore(fallback);
        baseline = CommitLogSegmentMappings.liveCount();
    }

    @After
    public void tearDown()
    {
        store.close();
        dir.deleteRecursive();
    }

    /// A segment file of [#SEGMENT_SIZE] zero bytes with each `values[i]` written at `offsets[i]`.
    private File segment(String name, ByteBuffer[] values, int[] offsets) throws Exception
    {
        byte[] content = new byte[SEGMENT_SIZE];
        for (int i = 0; i < values.length; ++i)
            values[i].duplicate().get(content, offsets[i], values[i].remaining());
        File file = new File(dir, name);
        Files.write(file.toPath(), content);
        return file;
    }

    /// Value locations saying `value` sits at `absoluteOffset` of the segment, as an entry starting 50 bytes before it.
    private static CommitLogValueLocations valueLocations(long segmentId, File file, int segmentSize, ByteBuffer value, long absoluteOffset)
    {
        SerializedValueOffsets relative = new SerializedValueOffsets();
        relative.record(value.array(), value.arrayOffset() + value.position(), value.remaining(), 50);
        // the serialized entry the offsets are relative to, which the value locations check the value against
        byte[] entry = new byte[50 + value.remaining()];
        value.duplicate().get(entry, 50, value.remaining());
        relative.finish(entry);
        return new CommitLogValueLocations(segmentId, file, segmentSize, absoluteOffset - 50, relative);
    }

    @Test
    public void appendWithValueLocationsReturnsSegmentHandle() throws Exception
    {
        ByteBuffer value = randomBytes(100, 1);
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ value }, new int[]{ 4096 });

        long handle = store.append(value.duplicate(), valueLocations(id, file, SEGMENT_SIZE, value, 4096));
        assertEquals(0, handle & FALLBACK_TAG);
        assertEquals(0, handle >>> OFFSET_BITS);
        assertEquals(4096, handle & OFFSET_MASK);
        assertEquals(0, value.position());

        ByteBuffer read = store.read(handle, 100);
        assertEquals(value, read);
        assertEquals(ByteOrder.BIG_ENDIAN, read.order());
        assertTrue(read.isDirect());

        assertEquals(100, store.referencedBytes());
        assertEquals(0, store.fallbackBytes());
        assertEquals(100, store.bytesWritten());
        assertEquals(1, store.segmentCount());
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertFalse("nothing was copied, so the fallback file was never created", fallback.exists());
    }

    @Test
    public void valueNotInValueLocationsFallsBack() throws Exception
    {
        ByteBuffer value = randomBytes(100, 1);
        ByteBuffer copy = ByteBuffer.wrap(value.array().clone());   // same bytes, different storage: not recorded
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ value }, new int[]{ 4096 });

        long handle = store.append(copy.duplicate(), valueLocations(id, file, SEGMENT_SIZE, value, 4096));
        assertNotEquals(0, handle & FALLBACK_TAG);
        assertEquals(value, store.read(handle, 100));
        assertEquals(0, store.referencedBytes());
        assertEquals(100, store.fallbackBytes());
        assertEquals(100, store.bytesWritten());
        assertEquals(0, store.segmentCount());
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
        assertTrue(fallbackFile.exists());
    }

    @Test
    public void appendWithoutValueLocationsFallsBack()
    {
        ByteBuffer v1 = randomBytes(100, 1), v2 = randomBytes(200, 2);
        long h1 = store.append(v1.duplicate(), null);
        long h2 = store.append(v2.duplicate(), null);
        assertNotEquals(0, h1 & FALLBACK_TAG);
        assertNotEquals(0, h2 & FALLBACK_TAG);
        assertEquals(v1, store.read(h1, 100));
        assertEquals(v2, store.read(h2, 200));
        assertEquals(300, store.fallbackBytes());
        assertEquals(0, store.referencedBytes());
        assertEquals(300, store.bytesWritten());
        assertEquals(0, store.segmentCount());
    }

    @Test
    public void valueLocationsWithoutRelativeOffsetsFallsBack()
    {
        ByteBuffer value = randomBytes(100, 1);
        long id = TestSegmentIds.next();
        CommitLogValueLocations valueLocations = new CommitLogValueLocations(id, new File(dir, "CommitLog-7-1.log"), SEGMENT_SIZE, 0, null);
        long handle = store.append(value.duplicate(), valueLocations);
        assertNotEquals(0, handle & FALLBACK_TAG);
        assertEquals(value, store.read(handle, 100));
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void valueLocationsBeyondMappedSizeFallsBack() throws Exception
    {
        ByteBuffer value = randomBytes(100, 1);
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ value }, new int[]{ 4096 });

        // the segment is mapped with the size the value locations state; the value would end 50 bytes past it
        long handle = store.append(value.duplicate(), valueLocations(id, file, 4096 + 50, value, 4096));
        assertNotEquals(0, handle & FALLBACK_TAG);
        assertEquals(value, store.read(handle, 100));
        assertEquals(0, store.referencedBytes());
        assertEquals(100, store.fallbackBytes());
    }

    @Test
    public void valueLocationsForMissingSegmentFileFallsBack()
    {
        ByteBuffer value = randomBytes(100, 1);
        long id = TestSegmentIds.next();
        File missing = new File(dir, "CommitLog-7-404.log");

        long handle = store.append(value.duplicate(), valueLocations(id, missing, SEGMENT_SIZE, value, 4096));
        assertNotEquals(0, handle & FALLBACK_TAG);
        assertEquals(value, store.read(handle, 100));
        assertEquals(0, store.segmentCount());
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void verifyRejectsValueLocationsPointingAtOtherBytes() throws Exception
    {
        assertTrue("the test JVM sets cassandra.memtable_value_log_verify", CommitLogValueStore.VERIFY);
        ByteBuffer inSegment = randomBytes(100, 1);
        ByteBuffer value = randomBytes(100, 99);
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ inSegment }, new int[]{ 4096 });

        try
        {
            store.append(value.duplicate(), valueLocations(id, file, SEGMENT_SIZE, value, 4096));
            fail("value locations pointing at different bytes must be rejected");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains(file.toString()));
            assertTrue(e.getMessage(), e.getMessage().contains("4096"));
        }
        assertEquals(0, store.referencedBytes());
        assertEquals(0, store.bytesWritten());
        // the segment was mapped before the comparison; it stays referenced until close
        assertEquals(1, store.segmentCount());
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void segmentThatCannotBeMappedIsNotRetried() throws Exception
    {
        ByteBuffer v1 = randomBytes(100, 1), v2 = randomBytes(100, 2), other = randomBytes(100, 3);
        long id = TestSegmentIds.next(), otherId = TestSegmentIds.next();
        File missing = new File(dir, "CommitLog-7-1.log");

        long h1 = store.append(v1.duplicate(), valueLocations(id, missing, SEGMENT_SIZE, v1, 4096));
        assertNotEquals(0, h1 & FALLBACK_TAG);

        // the file appears after the failure; CommitLogSegmentMappings remembers the failure and the store does not map
        // the segment again
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ v2 }, new int[]{ 4096 });
        long h2 = store.append(v2.duplicate(), valueLocations(id, file, SEGMENT_SIZE, v2, 4096));
        assertNotEquals(0, h2 & FALLBACK_TAG);
        assertEquals(0, store.segmentCount());
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
        assertEquals(v1, store.read(h1, 100));
        assertEquals(v2, store.read(h2, 100));

        // other segments are still referenced
        File otherFile = segment("CommitLog-7-2.log", new ByteBuffer[]{ other }, new int[]{ 4096 });
        long h3 = store.append(other.duplicate(), valueLocations(otherId, otherFile, SEGMENT_SIZE, other, 4096));
        assertEquals(0, h3 & FALLBACK_TAG);
        assertEquals(other, store.read(h3, 100));
        assertEquals(1, store.segmentCount());

        // nor after another segment has been mapped
        long h4 = store.append(v2.duplicate(), valueLocations(id, file, SEGMENT_SIZE, v2, 4096));
        assertNotEquals(0, h4 & FALLBACK_TAG);
        assertEquals(v2, store.read(h4, 100));
        assertEquals(1, store.segmentCount());
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertEquals(300, store.fallbackBytes());
        assertEquals(100, store.referencedBytes());
    }

    @Test
    public void segmentsAreIndexedInFirstUseOrder() throws Exception
    {
        ByteBuffer a1 = randomBytes(100, 1), a2 = randomBytes(300, 2), b1 = randomBytes(200, 3);
        long idA = TestSegmentIds.next(), idB = TestSegmentIds.next();
        File fileA = segment("CommitLog-7-1.log", new ByteBuffer[]{ a1, a2 }, new int[]{ 4096, 70_000 });
        File fileB = segment("CommitLog-7-2.log", new ByteBuffer[]{ b1 }, new int[]{ SEGMENT_SIZE - 200 });

        long hA1 = store.append(a1.duplicate(), valueLocations(idA, fileA, SEGMENT_SIZE, a1, 4096));
        long hB1 = store.append(b1.duplicate(), valueLocations(idB, fileB, SEGMENT_SIZE, b1, SEGMENT_SIZE - 200));
        long hA2 = store.append(a2.duplicate(), valueLocations(idA, fileA, SEGMENT_SIZE, a2, 70_000));

        assertEquals(4096L, hA1);
        assertEquals((1L << OFFSET_BITS) | (SEGMENT_SIZE - 200), hB1);
        assertEquals(70_000L, hA2);
        assertEquals(a1, store.read(hA1, 100));
        assertEquals(b1, store.read(hB1, 200));
        assertEquals(a2, store.read(hA2, 300));
        assertEquals(2, store.segmentCount());
        assertEquals(baseline + 2, CommitLogSegmentMappings.liveCount());
        assertEquals(600, store.referencedBytes());
        assertEquals(600, store.bytesWritten());
    }

    @Test
    public void mixedAppendsCountBothKinds() throws Exception
    {
        ByteBuffer referenced = randomBytes(100, 1), copied = randomBytes(250, 2);
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ referenced }, new int[]{ 4096 });

        long h1 = store.append(referenced.duplicate(), valueLocations(id, file, SEGMENT_SIZE, referenced, 4096));
        long h2 = store.append(copied.duplicate(), valueLocations(id, file, SEGMENT_SIZE, referenced, 4096));
        assertEquals(0, h1 & FALLBACK_TAG);
        assertNotEquals(0, h2 & FALLBACK_TAG);
        assertEquals(referenced, store.read(h1, 100));
        assertEquals(copied, store.read(h2, 250));
        assertEquals(100, store.referencedBytes());
        assertEquals(250, store.fallbackBytes());
        assertEquals(350, store.bytesWritten());
    }

    @Test
    public void storesShareSegmentMappings() throws Exception
    {
        ByteBuffer value = randomBytes(100, 1);
        long id = TestSegmentIds.next();
        File file = segment("CommitLog-7-1.log", new ByteBuffer[]{ value }, new int[]{ 4096 });
        CommitLogValueStore other = new CommitLogValueStore(new FileValueStore(new File(dir, "ks-tbl-1-1.values"), REGION_SIZE));
        try
        {
            long h1 = store.append(value.duplicate(), valueLocations(id, file, SEGMENT_SIZE, value, 4096));
            long h2 = other.append(value.duplicate(), valueLocations(id, file, SEGMENT_SIZE, value, 4096));
            assertEquals(0, h1 & FALLBACK_TAG);
            assertEquals(0, h2 & FALLBACK_TAG);
            assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());

            store.close();
            assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
            assertTrue(file.tryDelete());
            assertEquals(value, other.read(h2, 100));
        }
        finally
        {
            other.close();
        }
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void closeReleasesMappingsDeletesFallbackAndIsIdempotent() throws Exception
    {
        ByteBuffer referenced = randomBytes(100, 1), copied = randomBytes(250, 2);
        long idA = TestSegmentIds.next(), idB = TestSegmentIds.next();
        File fileA = segment("CommitLog-7-1.log", new ByteBuffer[]{ referenced }, new int[]{ 4096 });
        File fileB = segment("CommitLog-7-2.log", new ByteBuffer[]{ referenced }, new int[]{ 8192 });
        store.append(referenced.duplicate(), valueLocations(idA, fileA, SEGMENT_SIZE, referenced, 4096));
        store.append(referenced.duplicate(), valueLocations(idA, fileA, SEGMENT_SIZE, referenced, 4096));
        store.append(referenced.duplicate(), valueLocations(idB, fileB, SEGMENT_SIZE, referenced, 8192));
        store.append(copied.duplicate(), null);
        assertEquals(baseline + 2, CommitLogSegmentMappings.liveCount());
        assertTrue(fallbackFile.exists());

        store.close();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
        assertFalse(fallbackFile.exists());
        assertEquals(0, store.segmentCount());
        // the metrics gauge may still read these
        assertEquals(550, store.bytesWritten());
        assertEquals(250, store.fallbackBytes());
        assertEquals(300, store.referencedBytes());

        store.close(); // second close must not throw or release again
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }
}
