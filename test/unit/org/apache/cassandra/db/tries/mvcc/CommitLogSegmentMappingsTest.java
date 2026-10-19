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

import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitLogSegmentMappingsTest
{
    private static final int SIZE = 1 << 20;

    private File dir;
    private File file;
    private byte[] content;
    private int baseline;

    @Before
    public void setUp() throws Exception
    {
        dir = new File(Files.createTempDirectory("segment-mappings-test"));
        file = new File(dir, "CommitLog-7-1.log");
        content = new byte[SIZE];
        new Random(42).nextBytes(content);
        Files.write(file.toPath(), content);
        baseline = CommitLogSegmentMappings.liveCount();
    }

    @After
    public void tearDown()
    {
        dir.deleteRecursive();
    }

    private void assertContent(CommitLogSegmentMappings.Mapping mapping)
    {
        assertEquals(SIZE, mapping.size());
        for (int off : new int[]{ 0, 1, 4095, 4096, 123_457, SIZE - 1 })
            assertEquals("offset " + off, content[off], MemoryUtil.getByte(mapping.address() + off));
    }

    @Test
    public void acquireTwiceSharesOneMapping()
    {
        long id = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping m1 = CommitLogSegmentMappings.acquire(id, file, SIZE);
        CommitLogSegmentMappings.Mapping m2 = CommitLogSegmentMappings.acquire(id, file, SIZE);
        assertNotNull(m1);
        assertSame(m1, m2);
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertContent(m1);

        m1.release();
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertContent(m2);
        m2.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void acquireWithOtherSizeReturnsLiveMapping()
    {
        long id = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping m1 = CommitLogSegmentMappings.acquire(id, file, SIZE);
        CommitLogSegmentMappings.Mapping m2 = CommitLogSegmentMappings.acquire(id, file, 4096);
        assertSame(m1, m2);
        assertEquals(SIZE, m2.size());
        m1.release();
        m2.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void mappingStaysReadableAfterFileDeleted()
    {
        long id = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping m = CommitLogSegmentMappings.acquire(id, file, SIZE);
        assertNotNull(m);
        assertTrue(file.tryDelete());
        assertContent(m);
        m.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void acquireMissingFileReturnsNull()
    {
        long id = TestSegmentIds.next();
        assertNull(CommitLogSegmentMappings.acquire(id, new File(dir, "CommitLog-7-404.log"), SIZE));
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    /// A segment that could not be mapped is not tried again by any store, also after other segments have been mapped,
    /// and a newer failure extends this to every older segment that is not mapped already.
    @Test
    public void failedSegmentsAreNotRetried() throws Exception
    {
        long id = TestSegmentIds.next();
        File late = new File(dir, "CommitLog-7-2.log");
        assertNull(CommitLogSegmentMappings.acquire(id, late, SIZE));

        Files.write(late.toPath(), content);
        assertNull("the failed segment is not mapped again", CommitLogSegmentMappings.acquire(id, late, SIZE));
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());

        long otherId = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping other = CommitLogSegmentMappings.acquire(otherId, file, SIZE);
        assertNotNull("a newer segment is mapped", other);
        assertNull("mapping another segment does not clear the failure", CommitLogSegmentMappings.acquire(id, late, SIZE));
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());

        long untriedId = TestSegmentIds.next();
        assertNull(CommitLogSegmentMappings.acquire(TestSegmentIds.next(), new File(dir, "CommitLog-7-404.log"), SIZE));
        assertNull("the newer failure does not clear the older one", CommitLogSegmentMappings.acquire(id, late, SIZE));
        assertNull("a segment older than the newest failure is not mapped", CommitLogSegmentMappings.acquire(untriedId, file, SIZE));
        assertSame("but its live mapping is still shared", other, CommitLogSegmentMappings.acquire(otherId, file, SIZE));
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());

        other.release();
        other.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void acquireBeyondFileLengthReturnsNull()
    {
        long id = TestSegmentIds.next();
        assertNull(CommitLogSegmentMappings.acquire(id, file, SIZE + 4096));
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
        assertEquals("a read-only mapping must not extend the file", SIZE, file.length());
    }

    @Test
    public void acquireAfterReleaseGivesFreshMapping()
    {
        long id = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping dead = CommitLogSegmentMappings.acquire(id, file, SIZE);
        dead.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());

        CommitLogSegmentMappings.Mapping fresh = CommitLogSegmentMappings.acquire(id, file, SIZE);
        assertNotNull(fresh);
        assertNotSame(dead, fresh);
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertContent(fresh);
        fresh.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    @Test
    public void releaseOfDeadMappingThrowsAndLeavesFreshMappingIntact()
    {
        long id = TestSegmentIds.next();
        CommitLogSegmentMappings.Mapping dead = CommitLogSegmentMappings.acquire(id, file, SIZE);
        dead.release();
        try
        {
            dead.release();
            fail("released more often than acquired");
        }
        catch (IllegalStateException e)
        {
            // expected
        }

        CommitLogSegmentMappings.Mapping fresh = CommitLogSegmentMappings.acquire(id, file, SIZE);
        try
        {
            dead.release();
            fail("released more often than acquired");
        }
        catch (IllegalStateException e)
        {
            // expected
        }
        assertEquals(baseline + 1, CommitLogSegmentMappings.liveCount());
        assertContent(fresh);
        fresh.release();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }

    /// Threads acquire, read and release the same segment concurrently, so the reference count repeatedly drops to
    /// zero while other threads acquire. A thread handed a dead (unmapped) mapping would read wrong bytes or
    /// crash the JVM.
    @Test
    public void concurrentAcquireAndReleaseNeverHandsOutDeadMapping() throws Throwable
    {
        long id = TestSegmentIds.next();
        int threads = 4;
        int iterations = 2000;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; ++t)
        {
            int seed = t;
            workers[t] = new Thread(() -> {
                Random random = new Random(seed);
                try
                {
                    for (int i = 0; i < iterations; ++i)
                    {
                        CommitLogSegmentMappings.Mapping m = CommitLogSegmentMappings.acquire(id, file, SIZE);
                        assertNotNull(m);
                        int off = random.nextInt(SIZE);
                        assertEquals(content[off], MemoryUtil.getByte(m.address() + off));
                        m.release();
                    }
                }
                catch (Throwable e)
                {
                    failure.compareAndSet(null, e);
                }
            });
            workers[t].start();
        }
        for (Thread w : workers)
            w.join();
        if (failure.get() != null)
            throw failure.get();
        assertEquals(baseline, CommitLogSegmentMappings.liveCount());
    }
}
