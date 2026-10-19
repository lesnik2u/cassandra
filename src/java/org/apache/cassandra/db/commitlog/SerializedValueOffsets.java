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

package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.NoSpamLogger;

/// Offsets of the values inside a cached mutation serialization, of a local write and of a deserialized remote
/// mutation alike, keyed by the identity of their backing storage.
///
/// A heap value is keyed by its backing array and the array offset of its first byte; a value without an accessible
/// array (direct or read-only) is keyed by the buffer itself and its position. [#find] matches on the same key, offset
/// and length, scanning forward from the record after its previous hit.
///
/// A recorder is carried by the stream whose values it records, in
/// [org.apache.cassandra.io.util.DataOutputBuffer#recorder] or
/// [org.apache.cassandra.io.util.TeeDataInputPlus#recorder], and records only the values written to or read from that
/// stream.
///
/// Once the serialization is complete, [#finish] attaches its bytes, and every hit of [#find] is spot-checked against
/// them: a record whose bytes do not match the value is not returned, and is logged at most once a minute.
public final class SerializedValueOffsets
{
    private static final Logger logger = LoggerFactory.getLogger(SerializedValueOffsets.class);
    private static final NoSpamLogger.NoSpamLogStatement mismatchLog =
        NoSpamLogger.getStatement(logger,
                                  "A value of {} bytes recorded at offset {} of a serialized mutation does not match the " +
                                  "bytes there; it is copied instead of referenced in the commit log, and counted in the " +
                                  "memtable's value log fallback metric. This message is logged at most once a minute.",
                                  1, TimeUnit.MINUTES);

    /// The shortest value worth recording. Values of up to [org.apache.cassandra.db.memtable.TrieCellData#OFFSET_TTL]
    /// (16) bytes, or 24 without a TTL, are embedded in the 32-byte trie cell and never reach a value store, so only
    /// longer values are recorded.
    public static final int MIN_RECORDED_LENGTH = 17;

    /// Enabled once any table uses commitlog value storage; recorders are only set on streams while true. Not
    /// volatile: a thread that sees the change late only records nothing, and values not recorded are copied.
    public static boolean ENABLED = false;

    private static final int INITIAL_CAPACITY = 8;

    // allocated by the first record: most mutations have no value long enough to be recorded
    private Object[] keys;
    private int[] offsets;
    private int[] lengths;
    private int[] relOffsets;
    private int count;
    /// The serialization the relative offsets point into, attached by [#finish]; null until then.
    private byte[] serialized;
    /// Where [#find] starts scanning: the index after its previous hit. Values are usually looked up in the order they
    /// were recorded, which makes each lookup one comparison instead of a scan. Only a hint: any value is a valid
    /// start, so a racy update is harmless.
    private int cursor;

    /// Attaches the bytes of the completed serialization these offsets are relative to, against which [#find]
    /// checks every hit. Returns these offsets, or null if nothing was recorded.
    public SerializedValueOffsets finish(byte[] serialized)
    {
        if (count == 0)
            return null;
        this.serialized = serialized;
        return this;
    }

    /// Records that the value identified by `key` (a `byte[]`, or a [ByteBuffer] without an accessible array), `offset`
    /// and `length` was written at `relativeOffset`.
    @VisibleForTesting
    public void record(Object key, int offset, int length, int relativeOffset)
    {
        if (keys == null)
        {
            keys = new Object[INITIAL_CAPACITY];
            offsets = new int[INITIAL_CAPACITY];
            lengths = new int[INITIAL_CAPACITY];
            relOffsets = new int[INITIAL_CAPACITY];
        }
        else if (count == keys.length)
        {
            int capacity = count * 2;
            keys = Arrays.copyOf(keys, capacity);
            offsets = Arrays.copyOf(offsets, capacity);
            lengths = Arrays.copyOf(lengths, capacity);
            relOffsets = Arrays.copyOf(relOffsets, capacity);
        }
        keys[count] = key;
        offsets[count] = offset;
        lengths[count] = length;
        relOffsets[count] = relativeOffset;
        ++count;
    }

    /// Records that a cell value, a `byte[]` or a [ByteBuffer], of `length` bytes was written at `relativeOffset`,
    /// keyed as [#find] keys the buffer of a cell holding it. Values of any other type are not recorded.
    public void recordValue(Object value, int length, int relativeOffset)
    {
        if (value instanceof byte[])
            record(value, 0, length, relativeOffset);
        else if (value instanceof ByteBuffer)
        {
            ByteBuffer buffer = (ByteBuffer) value;
            if (buffer.hasArray())
                record(buffer.array(), buffer.arrayOffset() + buffer.position(), length, relativeOffset);
            else
                record(buffer, buffer.position(), length, relativeOffset);
        }
    }

    /// The relative offset recorded for `value`, or -1 if absent or shorter than [#MIN_RECORDED_LENGTH]. The recorded
    /// length must equal `value.remaining()`.
    /// When the same value was recorded more than once, any of its records may be returned: they hold equal bytes.
    ///
    /// A record is only returned if the first and last bytes of `value` equal the bytes at its offset in the
    /// serialization attached by [#finish], and never before one is attached. A record that fails this check means the
    /// value was recorded at the wrong offset, or changed since it was serialized; it is logged.
    public int find(ByteBuffer value)
    {
        if (value.remaining() < MIN_RECORDED_LENGTH)
            return -1;
        Object key;
        int offset;
        if (value.hasArray())
        {
            key = value.array();
            offset = value.arrayOffset() + value.position();
        }
        else
        {
            key = value;
            offset = value.position();
        }
        int length = value.remaining();
        // read once: a racing update between the bounds check and the use could start the scan at count
        int i = cursor;
        if (i >= count)
            i = 0;
        for (int probes = 0; probes < count; ++probes)
        {
            if (keys[i] == key && offsets[i] == offset && lengths[i] == length)
                return hit(i, value);
            if (++i == count)
                i = 0;
        }
        return -1;
    }

    private int hit(int index, ByteBuffer value)
    {
        if (serialized == null)
            return -1;
        int relativeOffset = relOffsets[index];
        if (!endsMatch(value, serialized, relativeOffset))
        {
            mismatchLog.error(new Object[]{ value.remaining(), relativeOffset });
            return -1;
        }
        cursor = index + 1;
        return relativeOffset;
    }

    /// Whether the first and the last 8 bytes of `value`, which is at least [#MIN_RECORDED_LENGTH] bytes long, equal
    /// the bytes at `at` in `serialized`.
    private static boolean endsMatch(ByteBuffer value, byte[] serialized, int at)
    {
        int length = value.remaining();
        assert length >= Long.BYTES : length;
        if (at < 0 || at > serialized.length - length)
            return false;
        int last = length - Long.BYTES;
        long first = value.getLong(value.position());
        long end = value.getLong(value.position() + last);
        // ByteArrayUtil reads big-endian
        if (value.order() != ByteOrder.BIG_ENDIAN)
        {
            first = Long.reverseBytes(first);
            end = Long.reverseBytes(end);
        }
        return first == ByteArrayUtil.getLong(serialized, at) && end == ByteArrayUtil.getLong(serialized, at + last);
    }

    /// The number of records.
    @VisibleForTesting
    public int size()
    {
        return count;
    }
}
