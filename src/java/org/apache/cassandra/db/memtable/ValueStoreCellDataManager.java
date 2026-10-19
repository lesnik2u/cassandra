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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.tries.mvcc.ValueStore;

/// [TrieMemtable.CellDataBufferManager] that keeps the values that do not fit a trie cell in a [ValueStore] instead
/// of memtable memory. The trie cell holds the handle the store returned; the handle is opaque to everything but the
/// store, which alone interprets it.
///
/// Append-only: every write appends a new version of the value and the trie cell is repointed to it. Superseded
/// versions are not reclaimed individually; the store releases all of them at once when the memtable is discarded.
/// Hence [#valueLogBytes] counts written, not live, bytes.
final class ValueStoreCellDataManager extends TrieMemtable.CellDataBufferManager
{
    private final ValueStore store;

    ValueStoreCellDataManager(ValueStore store)
    {
        this.store = store;
    }

    @Override
    public long store(ByteBuffer buffer, int length)
    {
        assert buffer.remaining() == length : "length " + length + " != remaining " + buffer.remaining();
        return store.append(buffer, valueLocations);
    }

    @Override
    public ByteBuffer load(long handle, int length)
    {
        return store.read(handle, length);
    }

    @Override
    public void release(long handle, int length)
    {
        // Nothing to do: the store is append-only and released as a whole when the memtable is discarded.
    }

    @Override
    boolean releaseNeeded()
    {
        return false;
    }

    @Override
    long onHeapSize()
    {
        return store.onHeapSize();
    }

    @Override
    long valueLogBytes()
    {
        return store.bytesWritten();
    }

    @Override
    long fallbackBytes()
    {
        return store.fallbackBytes();
    }

    @Override
    void completeMutation()
    {
        // Nothing needed as the store does not recycle
    }

    @Override
    void abortMutation()
    {
        // Nothing needed as the store does not recycle
    }

    @Override
    long unusedReservedOnHeapMemory()
    {
        return 0;
    }

    @Override
    void releaseReferencesUnsafe()
    {
        // no references held
    }

    @Override
    void close()
    {
        store.close();
    }

    @Override
    public String toString()
    {
        return "ValueStoreCellDataManager(" + store + ')';
    }
}
