/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.memory.BufferPools;

public final class CompressedChecksummedDataInput extends ChecksummedDataInput
{
    private static final BufferPool bufferPool = BufferPools.forChunkCache();

    private final ICompressor compressor;
    private volatile long filePosition = 0;     // Current position in file, advanced when reading chunk.
    private volatile long sourcePosition = 0;   // Current position in file to report, advanced after consuming chunk.
    private volatile ByteBuffer compressedBuffer = null;
    private final ByteBuffer metadataBuffer = ByteBuffer.allocate(CompressedHintsWriter.METADATA_SIZE);

    // Takes ownership of buffer, which upgradeInput took from bufferPool. buffer and compressedBuffer always come from
    // bufferPool, so they must always be returned to it.
    private CompressedChecksummedDataInput(ChannelProxy channel, ByteBuffer buffer, ICompressor compressor, long filePosition)
    {
        super(channel, buffer);
        this.compressor = compressor;
        this.sourcePosition = this.filePosition = filePosition;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return filePosition == channel.size() && buffer.remaining() == 0;
    }

    public long getSourcePosition()
    {
        return sourcePosition;
    }

    static class Position extends ChecksummedDataInput.Position
    {
        final long bufferStart;
        final int bufferPosition;

        public Position(long sourcePosition, long bufferStart, int bufferPosition)
        {
            super(sourcePosition);
            this.bufferStart = bufferStart;
            this.bufferPosition = bufferPosition;
        }

        @Override
        public long subtract(InputPosition o)
        {
            Position other = (Position) o;
            return bufferStart - other.bufferStart + bufferPosition - other.bufferPosition;
        }
    }

    public InputPosition getSeekPosition()
    {
        return new Position(sourcePosition, bufferOffset, buffer.position());
    }

    public void seek(InputPosition p)
    {
        Position pos = (Position) p;
        bufferOffset = pos.bufferStart;
        filePosition = pos.sourcePosition;
        buffer.position(0).limit(0);
        resetCrc();
        reBuffer();
        buffer.position(pos.bufferPosition);
        assert sourcePosition == pos.sourcePosition;
        assert bufferOffset == pos.bufferStart;
        assert buffer.position() == pos.bufferPosition;
    }

    @Override
    protected void readBuffer()
    {
        sourcePosition = filePosition;
        if (isEOF())
            return;

        metadataBuffer.clear();
        channel.read(metadataBuffer, filePosition);
        filePosition += CompressedHintsWriter.METADATA_SIZE;
        metadataBuffer.rewind();

        int uncompressedSize = metadataBuffer.getInt();
        int compressedSize = metadataBuffer.getInt();

        if (compressedBuffer == null || compressedSize > compressedBuffer.capacity())
        {
            int bufferSize = compressedSize + (compressedSize / 20);  // allocate +5% to cover variability in compressed size
            // take the new buffer before returning the old one, so that if get() throws we still own the old one and
            // close() returns it exactly once
            ByteBuffer oldCompressedBuffer = compressedBuffer;
            compressedBuffer = bufferPool.get(bufferSize, compressor.preferredBufferType());
            if (oldCompressedBuffer != null)
            {
                bufferPool.put(oldCompressedBuffer);
            }
        }

        compressedBuffer.clear();
        compressedBuffer.limit(compressedSize);
        channel.read(compressedBuffer, filePosition);
        compressedBuffer.rewind();
        filePosition += compressedSize;

        if (buffer.capacity() < uncompressedSize)
        {
            int bufferSize = uncompressedSize + (uncompressedSize / 20);
            // as for compressedBuffer, take the new buffer before returning the old one
            ByteBuffer oldBuffer = buffer;
            buffer = bufferPool.get(bufferSize, compressor.preferredBufferType());
            bufferPool.put(oldBuffer);
        }

        buffer.clear();
        buffer.limit(uncompressedSize);
        try
        {
            compressor.uncompress(compressedBuffer, buffer);
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getFile());
        }
    }

    /**
     * Returns {@link #buffer} and {@link #compressedBuffer} to {@link #bufferPool}, and nulls them, so that calling this
     * again cannot return a buffer the pool may have handed to someone else since. compressedBuffer is null until the
     * first block is read.
     */
    @Override
    protected void releaseBuffer()
    {
        ByteBuffer released = buffer;
        ByteBuffer releasedCompressed = compressedBuffer;
        buffer = null;
        compressedBuffer = null;
        try
        {
            if (released != null)
                bufferPool.put(released);
        }
        finally
        {
            if (releasedCompressed != null)
                bufferPool.put(releasedCompressed);
        }
    }

    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, ICompressor compressor)
    {
        long position = input.getPosition();
        input.close();

        ChannelProxy channel = new ChannelProxy(input.getFile());
        try
        {
            ByteBuffer buffer = bufferPool.get(RandomAccessReader.DEFAULT_BUFFER_SIZE, compressor.preferredBufferType());
            try
            {
                return new CompressedChecksummedDataInput(channel, buffer, compressor, position);
            }
            catch (Throwable t)
            {
                try
                {
                    bufferPool.put(buffer);
                }
                catch (Throwable t2)
                {
                    t.addSuppressed(t2);
                }
                throw t;
            }
        }
        catch (Throwable t)
        {
            throw Throwables.cleaned(channel.close(t));
        }
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }

    @VisibleForTesting
    int bufferCapacity()
    {
        return Objects.requireNonNull(buffer, "the reader is closed").capacity();
    }

    @VisibleForTesting
    int compressedBufferCapacity()
    {
        return Objects.requireNonNull(compressedBuffer, "no block has been read, or the reader is closed").capacity();
    }
}
