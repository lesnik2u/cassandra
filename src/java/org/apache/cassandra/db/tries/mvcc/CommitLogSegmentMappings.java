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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.memory.MemoryUtil;

/// Node-wide, reference-counted read-only mappings of commit log segment files, shared by every memtable value store
/// that points into a segment, so that there is at most one mapping per segment.
///
/// A segment is mapped when the first store acquires it, while the file is certainly on disk, and unmapped when the
/// last store releases it. The commit log deletes or archives the file only once every memtable with data in it has
/// flushed, but that can be before the memtables are discarded: the mapping stays readable for the reads in between.
///
/// Threading: [#acquire] and [Mapping#release] may be called by any thread. Every reference count change runs inside
/// the map's per-key `compute`, so a reference count reaching zero and the removal of its entry are one atomic step,
/// and [#acquire] never returns a mapping that has been (or is about to be) unmapped. The file is mapped outside the
/// map's lock.
public final class CommitLogSegmentMappings
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentMappings.class);

    private static final ConcurrentHashMap<Long, Mapping> MAPPINGS = new ConcurrentHashMap<>();

    /// The id of the newest segment that could not be mapped. [#acquire] refuses, without trying to map it, every
    /// segment with this id or a lower one that has no live mapping; a live mapping is still shared. Segment ids
    /// increase, so the segment that failed is normally the one being written, and the stores of every shard and
    /// memtable writing into it fall back at once instead of each trying again. Only grows, but refuses no newer
    /// segment: once the commit log has moved on, a transient failure has cost at most the values of one or two
    /// already-old segments (the failed one, and one still written to by writes that raced the switch to it). A
    /// racing thread that does not see a failure yet only makes one more mapping attempt.
    private static final AtomicLong newestFailedSegmentId = new AtomicLong(Long.MIN_VALUE);

    private CommitLogSegmentMappings()
    {
    }

    /// A read-only mapping of a whole segment file. Valid until the holder calls [#release].
    public static final class Mapping
    {
        private final long segmentId;
        private final MappedByteBuffer buffer;
        private final long address;
        /// Guarded by the `compute` on [#segmentId]; zero once the mapping is removed and unmapped.
        private int refs = 1;

        private Mapping(long segmentId, MappedByteBuffer buffer)
        {
            this.segmentId = segmentId;
            this.buffer = buffer;
            // getAddress accepts only writable direct buffers; a duplicate of the read-only one shares its address
            this.address = MemoryUtil.getAddress(MemoryUtil.duplicateDirectByteBuffer(buffer, MemoryUtil.getHollowDirectByteBuffer()));
        }

        /// The address of the first byte of the segment.
        public long address()
        {
            return address;
        }

        /// The number of mapped bytes.
        public int size()
        {
            return buffer.capacity();
        }

        /// Drops one reference; the last one unmaps the segment.
        /// @throws IllegalStateException if this mapping has already been released by every holder
        public void release()
        {
            if (MAPPINGS.compute(segmentId, this::decrement) == null)
                FileUtils.clean(buffer);
        }

        private Mapping decrement(Long id, Mapping current)
        {
            if (current != this)
                throw new IllegalStateException("Mapping of commit log segment " + segmentId + " released more often than acquired");
            return --refs == 0 ? null : this;
        }

        @Override
        public String toString()
        {
            return "Mapping(segment " + segmentId + ", " + size() + " bytes)";
        }
    }

    /// The mapping of segment `segmentId`, mapping the first `size` bytes of `file` read-only if no live mapping of it
    /// exists. A live mapping is returned whatever its size; callers bound their reads by [Mapping#size]. Every
    /// non-null result must be released exactly once.
    /// @return null if the file cannot be opened or mapped, e.g. because it has already been deleted or is shorter
    ///         than `size`, or if no live mapping exists and mapping this segment or a newer one failed before
    public static Mapping acquire(long segmentId, File file, int size)
    {
        Mapping existing = MAPPINGS.computeIfPresent(segmentId, CommitLogSegmentMappings::addRef);
        if (existing != null || segmentId <= newestFailedSegmentId.get())
            return existing;

        // not under the map's lock: when mmap fails, the JDK collects garbage and sleeps before retrying
        Mapping mapped = map(segmentId, file, size);
        if (mapped == null)
            return MAPPINGS.computeIfPresent(segmentId, CommitLogSegmentMappings::addRef);   // mapped by another thread meanwhile
        Mapping acquired = MAPPINGS.compute(segmentId, (id, current) -> current == null ? mapped : addRef(id, current));
        if (acquired != mapped)
        {
            // another thread mapped the segment first; ours is unused, and failing to unmap it must not lose the
            // reference just taken on theirs
            try
            {
                FileUtils.clean(mapped.buffer);
            }
            catch (RuntimeException e)
            {
                logger.warn("Could not unmap a duplicate mapping of commit log segment {} ({})", file, segmentId, e);
            }
        }
        return acquired;
    }

    private static Mapping addRef(Long id, Mapping current)
    {
        assert current.refs > 0 : current;
        ++current.refs;
        return current;
    }

    private static Mapping map(long segmentId, File file, int size)
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ))
        {
            // the mapping stays valid after the channel is closed
            return new Mapping(segmentId, channel.map(FileChannel.MapMode.READ_ONLY, 0, size));
        }
        catch (IOException e)
        {
            newestFailedSegmentId.accumulateAndGet(segmentId, Math::max);
            logger.debug("Could not map commit log segment {} ({}); its values will be copied", file, segmentId, e);
            return null;
        }
    }

    /// The number of mapped segments.
    @VisibleForTesting
    public static int liveCount()
    {
        return MAPPINGS.size();
    }
}
