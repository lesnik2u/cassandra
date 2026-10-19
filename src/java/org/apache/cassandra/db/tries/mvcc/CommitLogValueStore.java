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
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.commitlog.CommitLogValueLocations;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.memory.MemoryUtil;

/// A [ValueStore] that does not copy a value it can find in the commit log segment its mutation was just written to,
/// but points at the bytes there; any other value is copied into a [FileValueStore] fallback.
///
/// Handle layout:
/// - a fallback handle is `FALLBACK_TAG | h`, where `h` is the fallback's own handle;
/// - a segment handle is `(segmentIndex << OFFSET_BITS) | absoluteOffset`, where `segmentIndex` is the position of the
///   segment in this store's segment table (segments are numbered in order of first use) and `absoluteOffset` is the
///   offset of the value in the segment file. A segment is smaller than 2 GiB, so its offsets fit in `OFFSET_BITS`
///   bits; the index has the `63 - OFFSET_BITS` bits below the tag.
///
/// Each segment in the table holds a [CommitLogSegmentMappings.Mapping] until [#close]. The commit log keeps a segment
/// until every memtable with data in it has flushed; the mapping keeps it readable also between the flush and the
/// discard of the memtable, when the commit log may already have deleted the file.
///
/// Threading: as [ValueStore]. The segment table is an immutable snapshot in a volatile field. [#append] publishes the
/// snapshot containing a segment before it returns the first handle into it, and the trie publishes a handle only
/// after `append` has returned, so a reader holding a handle always finds its segment in the snapshot it reads.
public final class CommitLogValueStore implements ValueStore
{
    static final long FALLBACK_TAG = 1L << 63;
    static final int OFFSET_BITS = 40;
    private static final long OFFSET_MASK = (1L << OFFSET_BITS) - 1;
    private static final int MAX_SEGMENTS = 1 << (63 - OFFSET_BITS);

    /// Whether [#append] compares the segment bytes with the value before returning a segment handle. Not tied to
    /// assertions, which are enabled in production: the check costs a comparison per referenced value.
    @VisibleForTesting
    static final boolean VERIFY = CassandraRelevantProperties.MEMTABLE_VALUE_LOG_VERIFY.getBoolean();

    /// Immutable snapshot of the referenced segments, replaced when a segment is added and read once per [#read].
    private static final class Segments
    {
        static final Segments EMPTY = new Segments(new long[0], new long[0], new long[0], new CommitLogSegmentMappings.Mapping[0]);
        final long[] ids;                                // commit log segment ids, in order of first use
        final long[] addresses;                          // mappings[i].address()
        final long[] sizes;                              // mappings[i].size()
        final CommitLogSegmentMappings.Mapping[] mappings;

        Segments(long[] ids, long[] addresses, long[] sizes, CommitLogSegmentMappings.Mapping[] mappings)
        {
            this.ids = ids;
            this.addresses = addresses;
            this.sizes = sizes;
            this.mappings = mappings;
        }

        Segments with(long id, CommitLogSegmentMappings.Mapping mapping)
        {
            int n = ids.length;
            long[] i = Arrays.copyOf(ids, n + 1);
            long[] a = Arrays.copyOf(addresses, n + 1);
            long[] s = Arrays.copyOf(sizes, n + 1);
            CommitLogSegmentMappings.Mapping[] m = Arrays.copyOf(mappings, n + 1);
            i[n] = id;
            a[n] = mapping.address();
            s[n] = mapping.size();
            m[n] = mapping;
            return new Segments(i, a, s, m);
        }
    }

    private final FileValueStore fallback;
    private volatile Segments segments = Segments.EMPTY;
    private volatile long referencedBytes = 0;  // value bytes left in commit log segments
    /// Writer only: the last segment [CommitLogSegmentMappings#acquire] refused, not asked again for the values that
    /// follow, which saves a map lookup per value while the commit log is still writing into it.
    private long unmappableSegmentId = Long.MIN_VALUE;
    private boolean closed = false;

    /// Values that cannot be found in a commit log segment are copied into `fallback`, which this store closes.
    public CommitLogValueStore(FileValueStore fallback)
    {
        this.fallback = fallback;
    }

    /// Returns a handle to the value's bytes in the commit log segment if `valueLocations` locates them there and the
    /// segment can be mapped; otherwise copies the value into the fallback.
    /// @throws IllegalStateException in verify mode, if the segment bytes at the located offset differ from `value`
    @Override
    public long append(ByteBuffer value, CommitLogValueLocations valueLocations)
    {
        assert !closed : "append after close of " + this;
        long offset = valueLocations == null ? -1 : valueLocations.absoluteOffsetOf(value);
        if (offset < 0)
            return copy(value);

        int index = segmentIndex(valueLocations);
        if (index < 0)
            return copy(value);

        int length = value.remaining();
        Segments s = segments;
        if (offset + length > s.sizes[index])
            return copy(value);

        if (VERIFY)
            verify(s.addresses[index] + offset, value, length, valueLocations.segmentFile(), offset);
        referencedBytes += length;
        return ((long) index << OFFSET_BITS) | offset;
    }

    private long copy(ByteBuffer value)
    {
        return FALLBACK_TAG | fallback.append(value, null);
    }

    /// The index of the segment of `valueLocations` in the segment table, acquiring its mapping and publishing a new
    /// table if it is not there yet; -1 if [CommitLogSegmentMappings#acquire] returns null, or returned null when last
    /// asked for this segment, or if the table already holds [#MAX_SEGMENTS] segments.
    private int segmentIndex(CommitLogValueLocations valueLocations)
    {
        Segments s = segments;
        long segmentId = valueLocations.segmentId();
        // the segment being written is usually the last one added
        for (int i = s.ids.length - 1; i >= 0; --i)
        {
            if (s.ids[i] == segmentId)
                return i;
        }
        if (s.ids.length == MAX_SEGMENTS || segmentId == unmappableSegmentId)
            return -1;
        CommitLogSegmentMappings.Mapping mapping = CommitLogSegmentMappings.acquire(segmentId,
                                                                                    valueLocations.segmentFile(),
                                                                                    valueLocations.segmentSize());
        if (mapping == null)
        {
            unmappableSegmentId = segmentId;
            return -1;
        }
        segments = s.with(segmentId, mapping);   // publish before any handle into the segment is returned
        return s.ids.length;
    }

    private static void verify(long address, ByteBuffer value, int length, File segmentFile, long offset)
    {
        if (!MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN).equals(value))
            throw new IllegalStateException("Value of " + length + " bytes does not match the bytes of commit log segment "
                                            + segmentFile + " at offset " + offset);
    }

    /// Returns a big-endian view over the segment or fallback bytes. Valid until [#close]. A segment is mapped
    /// read-only, so a stray write through a view of it faults instead of corrupting the commit log.
    @Override
    public ByteBuffer read(long handle, int length)
    {
        if ((handle & FALLBACK_TAG) != 0)
            return fallback.read(handle & ~FALLBACK_TAG, length);
        Segments s = segments;
        int index = (int) (handle >>> OFFSET_BITS);
        long offset = handle & OFFSET_MASK;
        assert index < s.ids.length && offset + length <= s.sizes[index] : "handle " + handle + " outside " + this;
        return MemoryUtil.getByteBuffer(s.addresses[index] + offset, length, ByteOrder.BIG_ENDIAN);
    }

    /// Value bytes referenced in commit log segments plus the fallback's [FileValueStore#bytesWritten]. Both are
    /// non-decreasing, so the sum is too.
    @Override
    public long bytesWritten()
    {
        return referencedBytes + fallback.bytesWritten();
    }

    /// The fallback's [FileValueStore#bytesWritten].
    @Override
    public long fallbackBytes()
    {
        return fallback.bytesWritten();
    }

    /// The number of commit log segments this store refers to.
    int segmentCount()
    {
        return segments.ids.length;
    }

    /// On-heap footprint of the segment table and of the fallback; the mappings are shared with other stores and not
    /// counted.
    @Override
    public long onHeapSize()
    {
        Segments s = segments;
        return ObjectSizes.sizeOfArray(s.ids) + ObjectSizes.sizeOfArray(s.addresses) + ObjectSizes.sizeOfArray(s.sizes)
               + ObjectSizes.sizeOfReferenceArray(s.mappings.length)
               + fallback.onHeapSize();
    }

    /// Releases the mapping of every referenced segment, then closes the fallback. Idempotent. Every step is attempted
    /// even if an earlier one fails; the failures are rethrown together at the end.
    @Override
    public void close()
    {
        if (closed)
            return;
        closed = true;
        Segments s = segments;
        segments = Segments.EMPTY;
        Throwable accumulate = null;
        for (CommitLogSegmentMappings.Mapping mapping : s.mappings)
            accumulate = Throwables.perform(accumulate, mapping::release);
        accumulate = Throwables.perform(accumulate, fallback::close);
        Throwables.maybeFail(accumulate);
    }

    @Override
    public String toString()
    {
        return "CommitLogValueStore(" + segmentCount() + " segments, " + referencedBytes + " referenced bytes, "
               + fallbackBytes() + " fallback bytes)";
    }

    /// Value bytes referenced in commit log segments.
    @VisibleForTesting
    long referencedBytes()
    {
        return referencedBytes;
    }
}
