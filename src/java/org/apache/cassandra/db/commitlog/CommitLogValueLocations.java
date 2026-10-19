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

import org.apache.cassandra.io.util.File;

/// The locations of values written into one commit-log entry, resolvable to absolute offsets in its segment, so that
/// a memtable value store may refer to the values there instead of copying them.
public final class CommitLogValueLocations
{
    private final long segmentId;
    private final File segmentFile;
    private final int segmentSize;
    private final long entryStart;
    /// Offsets relative to [#entryStart]; may be null.
    private final SerializedValueOffsets relative;

    public CommitLogValueLocations(long segmentId, File segmentFile, int segmentSize, long entryStart, SerializedValueOffsets relative)
    {
        this.segmentId = segmentId;
        this.segmentFile = segmentFile;
        this.segmentSize = segmentSize;
        this.entryStart = entryStart;
        this.relative = relative;
    }

    /// The id of the segment, unique for the life of the process.
    public long segmentId()
    {
        return segmentId;
    }

    /// The segment file, which exists while the write that produced these locations is in progress.
    public File segmentFile()
    {
        return segmentFile;
    }

    /// The size of the segment file in bytes.
    public int segmentSize()
    {
        return segmentSize;
    }

    /// The absolute segment offset of `value` (its position..limit), or -1 if [#relative] is null or does not contain
    /// it.
    public long absoluteOffsetOf(ByteBuffer value)
    {
        if (relative == null)
            return -1;
        int relativeOffset = relative.find(value);
        return relativeOffset < 0 ? -1 : entryStart + relativeOffset;
    }
}
