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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.memory.MemoryUtil;

/// Append-only, memory-mapped store for the values of trie memtable cells that do not fit in the 32-byte trie cell.
/// One instance per memtable shard; a handle is the absolute file offset of the value.
///
/// Threading: as [ValueStore]. The region table is an immutable snapshot in a volatile field; [#append] publishes the
/// snapshot containing a new region before it returns the first handle into it.
public final class FileValueStore implements ValueStore
{
    private static final Logger logger = LoggerFactory.getLogger(FileValueStore.class);

    /// The size of the regions a file grows by in production. A region is mapped as one `MappedByteBuffer`, so it
    /// must be smaller than 2 GiB.
    static final long REGION_SIZE = 64L << 20;

    private static final long PAGE = MemoryUtil.pageSize();

    private static final String FILE_SUFFIX = ".values";

    /// Distinguishes the file names of this process from those of earlier processes, whose `memtableSeq` also started
    /// at 1. In-JVM dtest instances share it, so it does not separate the runs of an instance restarted in the same
    /// JVM.
    private static final long JVM_START_MILLIS = ManagementFactory.getRuntimeMXBean().getStartTime();

    /// The value log directory, set once it has been created and its stale files deleted; see [#directory].
    private static volatile File directory;

    /// Immutable snapshot of the mapped regions, replaced on growth and read once per [#read].
    private static final class Regions
    {
        static final Regions EMPTY = new Regions(new long[0], new long[0], new MappedByteBuffer[0], new long[0]);
        final long[] starts;              // ascending file offsets
        final long[] ends;                // starts[i] + buffers[i].capacity()
        final MappedByteBuffer[] buffers; // same order
        final long[] addresses;           // MemoryUtil.getAddress(buffers[i])

        Regions(long[] starts, long[] ends, MappedByteBuffer[] buffers, long[] addresses)
        {
            this.starts = starts;
            this.ends = ends;
            this.buffers = buffers;
            this.addresses = addresses;
        }

        /// Adds `buffer`, mapped at file offset `start`, as the region that follows the last one: regions are mapped at
        /// the end of the file.
        Regions with(long start, MappedByteBuffer buffer)
        {
            int n = starts.length;
            assert start == (n == 0 ? 0 : ends[n - 1]) : "region at " + start + " after regions ending at " + Arrays.toString(ends);
            long[] s = Arrays.copyOf(starts, n + 1);
            long[] e = Arrays.copyOf(ends, n + 1);
            MappedByteBuffer[] b = Arrays.copyOf(buffers, n + 1);
            long[] a = Arrays.copyOf(addresses, n + 1);
            s[n] = start;
            e[n] = start + buffer.capacity();
            b[n] = buffer;
            a[n] = MemoryUtil.getAddress(buffer);
            return new Regions(s, e, b, a);
        }
    }

    private final File file;
    private final long regionSize;
    private FileChannel channel;         // writer only
    private volatile Regions regions = Regions.EMPTY;
    private long currentBase = 0;        // writer only: address of the current region minus its start offset
    private long regionEnd = 0;          // writer only: end offset of the current region, and of the file
    private long appendPosition = 0;     // writer only
    private volatile long bytesWritten = 0; // published for metrics and memory accounting
    private boolean closed = false;

    /// Does not create the file; that happens on the first [#append]. Values are appended to regions of
    /// `regionSize` bytes; a larger value gets a region of its own.
    @VisibleForTesting
    public FileValueStore(File file, long regionSize)
    {
        assert regionSize > 0 && regionSize % PAGE == 0 && regionSize <= Integer.MAX_VALUE : regionSize;
        this.file = file;
        this.regionSize = regionSize;
    }

    /// The store of shard `shard` of the memtable `memtableSeq` of the table `metadata`: a file in [#directory]
    /// growing in regions of [#REGION_SIZE].
    public static FileValueStore forShard(TableMetadata metadata, long memtableSeq, int shard)
    {
        return new FileValueStore(fileFor(metadata.keyspace, metadata.name, metadata.id, memtableSeq, shard), REGION_SIZE);
    }

    /// Copies `value` and returns the handle; `valueLocations` is ignored.
    @Override
    public long append(ByteBuffer value, CommitLogValueLocations valueLocations)
    {
        assert !closed : "append after close of " + file;
        int length = value.remaining();
        if (appendPosition + length > regionEnd)
            addRegion(length);
        long handle = appendPosition;
        MemoryUtil.setBytes(currentBase + handle, value);
        // a value larger than the region size has a dedicated region: its page-rounding tail takes no further values
        appendPosition = length > regionSize ? regionEnd : handle + length;
        bytesWritten += length;   // single writer
        return handle;
    }

    /// Returns a big-endian view over the mapped bytes. Valid until [#close].
    @Override
    public ByteBuffer read(long handle, int length)
    {
        Regions r = regions;
        int i = Arrays.binarySearch(r.starts, handle);
        if (i < 0)
            i = -i - 2;
        assert i >= 0 && handle + length <= r.ends[i] : "handle " + handle + " outside " + file;
        return MemoryUtil.getByteBuffer(r.addresses[i] + (handle - r.starts[i]), length, ByteOrder.BIG_ENDIAN);
    }

    /// Maps a region at the end of the file: of `regionSize` bytes, or of its own for a larger value.
    private void addRegion(int valueLength)
    {
        long start = regionEnd;
        long size = Math.max(regionSize, roundUpToPage(valueLength));
        MappedByteBuffer buffer;
        try
        {
            if (channel == null)
                channel = open();
            // mapping past the end of the file extends it
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, start, size);
        }
        catch (FileAlreadyExistsException e)
        {
            // two live stores must never share a file; a collision is a naming bug, not a disk failure
            throw new IllegalStateException("Memtable value log " + file + " already exists", e);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        Regions r = regions.with(start, buffer);
        regions = r;   // publish before the writer's state points into the new region
        currentBase = r.addresses[r.addresses.length - 1] - start;
        appendPosition = start;  // a value that does not fit the tail of the old region starts the new one
        regionEnd = start + size;
    }

    /// Creates the file. A value log directory deleted since [#directory] created it is recreated, without deleting
    /// stale files, which is only safe before any store exists.
    private FileChannel open() throws IOException
    {
        try
        {
            return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
        }
        catch (NoSuchFileException e)
        {
            file.parent().createDirectoriesIfNotExists();
            return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
        }
    }

    private static long roundUpToPage(long n)
    {
        return (n + PAGE - 1) / PAGE * PAGE;
    }

    /// The sum of the appended lengths. Excludes the unused tails of regions a value did not fit into and the page
    /// rounding of dedicated regions.
    @Override
    public long bytesWritten()
    {
        return bytesWritten;
    }

    /// On-heap footprint of the region table (the mapped buffers themselves are off-heap).
    @Override
    public long onHeapSize()
    {
        Regions r = regions;
        return ObjectSizes.sizeOfArray(r.starts) + ObjectSizes.sizeOfArray(r.ends) + ObjectSizes.sizeOfArray(r.addresses)
               + ObjectSizes.sizeOfReferenceArray(r.buffers.length)
               + (long) r.buffers.length * ObjectSizes.DIRECT_BUFFER_SHALLOW_SIZE;
    }

    /// Unmaps every region, closes and deletes the file. Idempotent. Every step is attempted even if an earlier one
    /// fails; the failures are rethrown together at the end.
    @Override
    public void close()
    {
        if (closed)
            return;
        closed = true;
        Regions r = regions;
        regions = Regions.EMPTY;
        Throwable accumulate = null;
        for (MappedByteBuffer b : r.buffers)
            accumulate = Throwables.perform(accumulate, () -> FileUtils.clean(b));
        if (channel != null)
        {
            accumulate = Throwables.perform(accumulate, file.path(), Throwables.FileOpType.WRITE, channel::close);
            channel = null;
            if (!file.tryDelete() && file.exists())
                logger.warn("Could not delete memtable value log {}", file);
        }
        Throwables.maybeFail(accumulate);
    }

    /// The directory holding value logs: a sibling of the commit log segments. Created on first use, when the stale
    /// files of a previous run are also deleted, because they are meaningless after a restart; later calls return
    /// the cached directory.
    public static File directory()
    {
        File dir = directory;
        if (dir != null)
            return dir;
        synchronized (FileValueStore.class)
        {
            if (directory == null)
            {
                dir = valueLogDirectory();
                dir.createDirectoriesIfNotExists();
                int n = deleteStaleFiles();
                if (n > 0)
                    logger.info("Deleted {} stale memtable value log files from {}", n, dir);
                directory = dir;
            }
            return directory;
        }
    }

    /// Deletes every `*.values` file in the value log directory and returns how many were deleted. Only safe before
    /// any store exists, which is why it runs only from the first [#directory] call.
    @VisibleForTesting
    static int deleteStaleFiles()
    {
        File[] files = valueLogDirectory().tryList((f, name) -> name.endsWith(FILE_SUFFIX));
        if (files == null)
            return 0;
        int deleted = 0;
        for (File f : files)
        {
            if (f.tryDelete())
                ++deleted;
            else
                logger.warn("Could not delete stale memtable value log {}", f);
        }
        return deleted;
    }

    /// `<keyspace>-<table>-<tableId>-<jvmStartMillis>-<memtableSeq>-<shard>.values`; the table id, in the hex form
    /// table directories use, keeps a dropped and recreated table with the same name from reusing a live file name,
    /// and the JVM start time keeps a stale file of an earlier process that could not be deleted from colliding with a
    /// file of this one (it does not separate in-JVM dtest instance restarts, which share the JVM).
    static File fileFor(String keyspace, String table, TableId tableId, long memtableSeq, int shard)
    {
        return new File(directory(), filePrefix(keyspace, table, tableId) + JVM_START_MILLIS + '-' + memtableSeq + '-' + shard + FILE_SUFFIX);
    }

    /// The value files of the table `metadata` in [#directory], of any memtable and process.
    @VisibleForTesting
    public static File[] filesOf(TableMetadata metadata)
    {
        String prefix = filePrefix(metadata.keyspace, metadata.name, metadata.id);
        return directory().tryList((f, name) -> name.startsWith(prefix) && name.endsWith(FILE_SUFFIX));
    }

    private static String filePrefix(String keyspace, String table, TableId tableId)
    {
        return keyspace + '-' + table + '-' + tableId.toHexString() + '-';
    }

    private static File valueLogDirectory()
    {
        return new File(DatabaseDescriptor.getCommitLogLocation(), "memtable_values");
    }

    @Override
    public String toString()
    {
        return "FileValueStore(" + file + ", " + bytesWritten + " bytes, " + regionCount() + " regions)";
    }

    /// Whether this store has created its file and not yet closed. Reads writer-only state.
    @VisibleForTesting
    boolean exists()
    {
        return channel != null;
    }

    @VisibleForTesting
    int regionCount()
    {
        return regions.starts.length;
    }
}
