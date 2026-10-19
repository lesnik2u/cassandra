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

import org.apache.cassandra.db.commitlog.CommitLogValueLocations;

/// Append-only store for values kept outside memtable memory.
///
/// A handle is opaque: only the store that returned it may interpret it. The file-backed [FileValueStore] uses the
/// absolute file offset of the value; [CommitLogValueStore] packs an index into its own segment table and an offset.
///
/// Threading, for every implementation:
/// - [#append] has a single writer, the owning shard's writer (under the shard's write lock).
/// - [#read] may be called by any thread holding a handle that has been published to it.
/// - [#close] is called only after the memtable's read barrier (see `ColumnFamilyStore.Flush.reclaim`), so no reader
///   is still using a view it returned.
/// - [#bytesWritten] and [#fallbackBytes] may be read by any thread, also after [#close].
public interface ValueStore extends AutoCloseable
{
    /// Stores the bytes of `value` from its position to its limit, which are not modified, and returns an opaque
    /// handle that only this store interprets. `valueLocations` is where the write put its values in the commit log,
    /// or null; the store may refer to the value there instead of copying it, or ignore it.
    long append(ByteBuffer value, CommitLogValueLocations valueLocations);

    /// A big-endian view over the stored bytes; writing through it is undefined. Valid until [#close].
    ByteBuffer read(long handle, int length);

    /// Bytes appended so far (written, not live): a non-decreasing counter. The memtable charges them to its memory,
    /// as the bytes it owns outside it.
    long bytesWritten();

    /// The part of [#bytesWritten] copied because the value could not be referenced where it already was; 0 for a
    /// store that never references.
    default long fallbackBytes()
    {
        return 0;
    }

    /// On-heap footprint of the store's bookkeeping.
    long onHeapSize();

    /// Releases all storage; idempotent.
    @Override
    void close();
}
