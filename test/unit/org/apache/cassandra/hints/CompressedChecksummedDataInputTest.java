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

package org.apache.cassandra.hints;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Checks that {@link CompressedChecksummedDataInput} returns everything it takes from the chunk cache buffer pool.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "Reset the readBuffer get counter for each failed growth test",
        targetClass = "org.apache.cassandra.hints.CompressedChecksummedDataInputTest",
        targetMethod = "testFailedBufferGrowth",
        targetLocation = "AT ENTRY",
        action = "deleteCounter(\"readBufferGets\")")
public class CompressedChecksummedDataInputTest
{
    // Over the reader's initial buffer, so the reader must replace it, and small enough to be served from the pool
    private static final int POOLED_BLOCK_SIZE = 16 << 10;
    // Over BufferPool.NORMAL_CHUNK_SIZE, so the pool has to allocate the reader's buffer outside its chunks
    private static final int UNPOOLED_BLOCK_SIZE = 256 << 10;

    // Reading the first block gets the compressed buffer, then grows the uncompressed one. Reading a bigger second
    // block grows the compressed buffer (third get), then the uncompressed one (fourth get).
    private static final String NTH_READ_BUFFER_GET = "callerEquals(\"CompressedChecksummedDataInput.readBuffer\", true) && incrementCounter(\"readBufferGets\") == ";
    private static final String FAIL_GET = "throw new java.lang.OutOfMemoryError(\"Direct buffer memory\")";

    private File dir;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void createDir()
    {
        dir = new File(Files.createTempDir());
    }

    @After
    public void deleteDir()
    {
        dir.deleteRecursive();
    }

    @Test
    public void testPooledBlocksLZ4() throws Exception
    {
        testReadReturnsBuffersToPool(LZ4Compressor.class, POOLED_BLOCK_SIZE);
    }

    @Test
    public void testUnpooledBlocksLZ4() throws Exception
    {
        testReadReturnsBuffersToPool(LZ4Compressor.class, UNPOOLED_BLOCK_SIZE);
    }

    @Test
    public void testPooledBlocksDeflate() throws Exception
    {
        testReadReturnsBuffersToPool(DeflateCompressor.class, POOLED_BLOCK_SIZE);
    }

    @Test
    public void testUnpooledBlocksDeflate() throws Exception
    {
        testReadReturnsBuffersToPool(DeflateCompressor.class, UNPOOLED_BLOCK_SIZE);
    }

    private void testReadReturnsBuffersToPool(Class<? extends ICompressor> compressorClass, int blockSize) throws Exception
    {
        // Each hint is exactly as big as the session buffer, so the session buffers it rather than writing it as a
        // block of its own, and the next append (or closing the session) writes it as one block of blockSize.
        byte[][] hints = { randomBytes(blockSize, 1), randomBytes(blockSize, 2), randomBytes(blockSize, 3) };
        File file = writeHintsFile(compressorClass, blockSize, hints);

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        try (HintsReader reader = HintsReader.open(file))
        {
            ChecksummedDataInput input = reader.getInput();
            assertTrue(input instanceof CompressedChecksummedDataInput);
            for (byte[] hint : hints)
            {
                byte[] read = new byte[hint.length];
                input.readFully(read);
                assertArrayEquals(hint, read);
            }
            assertTrue(input.isEOF());
        }

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    /**
     * Fails growing the compressed buffer for the second block, as an exhausted direct memory would, and checks that
     * the buffers the reader still holds are returned to the pool once, on close.
     */
    @Test
    @BMRule(name = "Fail growing the unpooled compressed buffer",
            targetClass = "org.apache.cassandra.utils.memory.BufferPool",
            targetMethod = "get(int, org.apache.cassandra.io.compress.BufferType)",
            targetLocation = "AT ENTRY",
            condition = NTH_READ_BUFFER_GET + "3",
            action = FAIL_GET)
    public void testFailedCompressedBufferGrowth() throws Exception
    {
        testFailedBufferGrowth(UNPOOLED_BLOCK_SIZE, true);
    }

    /**
     * As {@link #testFailedCompressedBufferGrowth()}, but fails growing the uncompressed buffer.
     */
    @Test
    @BMRule(name = "Fail growing the unpooled uncompressed buffer",
            targetClass = "org.apache.cassandra.utils.memory.BufferPool",
            targetMethod = "get(int, org.apache.cassandra.io.compress.BufferType)",
            targetLocation = "AT ENTRY",
            condition = NTH_READ_BUFFER_GET + "4",
            action = FAIL_GET)
    public void testFailedUncompressedBufferGrowth() throws Exception
    {
        testFailedBufferGrowth(UNPOOLED_BLOCK_SIZE, false);
    }

    /**
     * As {@link #testFailedUncompressedBufferGrowth()}, but with buffers served from the pool.
     */
    @Test
    @BMRule(name = "Fail growing the pooled uncompressed buffer",
            targetClass = "org.apache.cassandra.utils.memory.BufferPool",
            targetMethod = "get(int, org.apache.cassandra.io.compress.BufferType)",
            targetLocation = "AT ENTRY",
            condition = NTH_READ_BUFFER_GET + "4",
            action = FAIL_GET)
    public void testFailedPooledUncompressedBufferGrowth() throws Exception
    {
        testFailedBufferGrowth(POOLED_BLOCK_SIZE, false);
    }

    private void testFailedBufferGrowth(int blockSize, boolean failCompressedBuffer) throws Exception
    {
        // A first block of blockSize, then a bigger second one, so both buffers have to grow
        byte[] first = randomBytes(blockSize, 1);
        byte[] second = randomBytes(blockSize * 5 / 4, 2);
        File file = writeHintsFile(LZ4Compressor.class, blockSize, first, second);

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        try (HintsReader reader = HintsReader.open(file))
        {
            CompressedChecksummedDataInput input = (CompressedChecksummedDataInput) reader.getInput();
            input.readFully(new byte[first.length]);
            int compressedBufferCapacity = input.compressedBufferCapacity();
            int bufferCapacity = input.bufferCapacity();

            try
            {
                input.readFully(new byte[second.length]);
                fail("Expected a buffer allocation for the second block to fail");
            }
            catch (OutOfMemoryError e)
            {
                // expected
            }

            // the buffer that failed to grow is still the one the first block was read into
            if (failCompressedBuffer)
                assertEquals(compressedBufferCapacity, input.compressedBufferCapacity());
            else
                assertTrue(input.compressedBufferCapacity() > compressedBufferCapacity);
            assertEquals(bufferCapacity, input.bufferCapacity());
        }

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    /**
     * Fails the reader's constructor after its first buffer has been taken from the pool.
     */
    @Test
    @BMRule(name = "Fail constructing the reader",
            targetClass = "org.apache.cassandra.hints.CompressedChecksummedDataInput",
            targetMethod = "<init>",
            targetLocation = "AT EXIT",
            action = "throw new java.lang.OutOfMemoryError(\"Java heap space\")")
    public void testFailedConstruction() throws Exception
    {
        File file = writeHintsFile(LZ4Compressor.class, POOLED_BLOCK_SIZE, randomBytes(POOLED_BLOCK_SIZE, 1));

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        Throwable e = assertThrows(Throwable.class, () -> HintsReader.open(file));
        Throwables.assertAnyCause(e, OutOfMemoryError.class);

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    /**
     * Fails returning the buffers to the pool on close, and checks that the file channel is closed anyway. The
     * buffers whose return failed stay out of the pool, so this test does not check the pool's counters.
     */
    @Test
    @BMRule(name = "Fail returning the reader's buffers",
            targetClass = "org.apache.cassandra.utils.memory.BufferPool",
            targetMethod = "put(java.nio.ByteBuffer)",
            targetLocation = "AT ENTRY",
            condition = "callerEquals(\"CompressedChecksummedDataInput.releaseBuffer\", true)",
            action = "throw new java.lang.IllegalStateException(\"put failed\")")
    public void testFailedReleaseClosesChannel() throws Exception
    {
        File file = writeHintsFile(LZ4Compressor.class, POOLED_BLOCK_SIZE, randomBytes(POOLED_BLOCK_SIZE, 1));

        HintsReader reader = HintsReader.open(file);
        ChecksummedDataInput input = reader.getInput();
        input.readFully(new byte[POOLED_BLOCK_SIZE]);
        ChannelProxy channel = input.getChannel();

        IllegalStateException e = assertThrows(IllegalStateException.class, reader::close);
        assertEquals("put failed", e.getMessage());
        assertTrue(channel.isCleanedUp());
    }

    /**
     * Closing the reader again must not return its buffers to the pool a second time: by then the pool may have
     * handed the same buffer objects to someone else.
     */
    @Test
    public void testDoubleClose() throws Throwable
    {
        File file = writeHintsFile(LZ4Compressor.class, POOLED_BLOCK_SIZE, randomBytes(POOLED_BLOCK_SIZE, 1));

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        // A new thread has its own local pool, so the buffer objects it reuses are exactly the reader's three: the
        // initial buffer, returned when the reader grew it, then the compressed and the uncompressed buffer.
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try
            {
                HintsReader reader = HintsReader.open(file);
                reader.getInput().readFully(new byte[POOLED_BLOCK_SIZE]);
                reader.close();

                ByteBuffer[] others = new ByteBuffer[3];
                for (int i = 0; i < others.length; i++)
                    others[i] = pool.get(POOLED_BLOCK_SIZE, BufferType.OFF_HEAP);
                try
                {
                    reader.close();
                    for (ByteBuffer other : others)
                        assertEquals(POOLED_BLOCK_SIZE, other.capacity());
                    assertEquals(usedBefore + others.length * POOLED_BLOCK_SIZE, pool.usedSizeInBytes());
                }
                finally
                {
                    pool.putMultiple(others);
                }
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        }, "double-close");
        thread.start();
        thread.join();
        if (failure.get() != null)
            throw failure.get();

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    /**
     * A file the writer closed without writing a hint holds only the descriptor, so the reader never reads a block.
     */
    @Test
    public void testDescriptorOnlyFile() throws Exception
    {
        File file = writeHintsFile(LZ4Compressor.class, POOLED_BLOCK_SIZE);

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        try (HintsReader reader = HintsReader.open(file))
        {
            assertTrue(reader.getInput() instanceof CompressedChecksummedDataInput);
            assertTrue(reader.getInput().isEOF());
            assertFalse(reader.iterator().hasNext());
        }

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    @Test
    public void testCloseBeforeFirstBlock() throws Exception
    {
        File file = writeHintsFile(LZ4Compressor.class, POOLED_BLOCK_SIZE, randomBytes(POOLED_BLOCK_SIZE, 1));

        BufferPool pool = BufferPools.forChunkCache();
        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        try (HintsReader reader = HintsReader.open(file))
        {
            assertTrue(reader.getInput() instanceof CompressedChecksummedDataInput);
            assertFalse(reader.getInput().isEOF());
        }

        assertPoolUnchanged(pool, usedBefore, overflowBefore);
    }

    /**
     * Writes the given hints through a session whose buffer is {@code blockSize}, so that the writer compresses and
     * writes a block each time the buffer fills up, and writes a hint bigger than the buffer as a block of its own.
     */
    private File writeHintsFile(Class<? extends ICompressor> compressorClass, int blockSize, byte[]... hints) throws Exception
    {
        HintsDescriptor descriptor = new HintsDescriptor(UUID.randomUUID(), System.currentTimeMillis(), params(compressorClass));
        try (HintsWriter writer = HintsWriter.create(dir, descriptor);
             HintsWriter.Session session = writer.newSession(ByteBuffer.allocateDirect(blockSize)))
        {
            for (byte[] hint : hints)
                session.append(ByteBuffer.wrap(hint));
        }
        return descriptor.file(dir);
    }

    private static void assertPoolUnchanged(BufferPool pool, long usedBefore, long overflowBefore)
    {
        long overflowAfter = pool.overflowMemoryInBytes();
        long usedAfter = pool.usedSizeInBytes();
        String state = String.format("overflow %d -> %d, used %d -> %d", overflowBefore, overflowAfter, usedBefore, usedAfter);
        // usedSizeInBytes is memoryInUse + overflowMemoryInBytes
        assertEquals("memoryInUse: " + state, usedBefore - overflowBefore, usedAfter - overflowAfter);
        assertEquals("overflowMemoryInBytes: " + state, overflowBefore, overflowAfter);
        assertEquals("usedSizeInBytes: " + state, usedBefore, usedAfter);
    }

    private static byte[] randomBytes(int size, long seed)
    {
        // random, so that compression keeps the blocks at about their uncompressed size
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static ImmutableMap<String, Object> params(Class<? extends ICompressor> compressorClass)
    {
        ImmutableMap<String, Object> compressionParams = ImmutableMap.of(ParameterizedClass.CLASS_NAME, compressorClass.getSimpleName());
        return ImmutableMap.of(HintsDescriptor.COMPRESSION, compressionParams);
    }
}
