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

package org.apache.cassandra.db.tries;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryReadTrie.NONE;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.PREFIX_ALTERNATE_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.PREFIX_CONTENT_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.PREFIX_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.isLeaf;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.isNull;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.offset;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Tests that [InMemoryRangeTrie.InMemoryRangeCursor#reset] leaves the cursor in the same state as a newly constructed
/// one, for any root the cursor is reset to and any state the cursor was left in by its previous use.
public class InMemoryRangeCursorResetTest
{
    enum RootShape
    {
        PLAIN_NODE,
        PREFIX_WITH_DESCENT_CONTENT,
        PREFIX_WITH_RETURN_CONTENT,
        LEAF
    }

    /// The state in which the previous use of the cursor leaves it.
    enum PreviousUse
    {
        FRESH,
        HALF_WALKED,
        ON_OPENING_BOUNDARY,
        AFTER_SKIP,
        EXHAUSTED
    }

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test
    public void testResetMatchesFreshCursor() throws TrieSpaceExhaustedException
    {
        InMemoryRangeTrie<DeletionMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        // A covered range whose root has content on the descent path, and a child.
        addRange(trie, key(0x20), true, key(0x20, 0x80), false, 10);
        // A covered range ending on the return path of a node with a child.
        addRange(trie, key(0x30, 0x10), true, key(0x30), true, 10);
        // Ranges ending and starting in leaves.
        addRange(trie, key(0x38), true, key(0x40), true, 10);
        // A partition deletion with a newer row range inside it: content on both paths of the branch root.
        addRange(trie, key(0x50), true, key(0x50), true, 10);
        addRange(trie, key(0x50, 0x10), true, key(0x50, 0x20), false, 20);

        Set<Integer> roots = collectRoots(trie);
        Set<RootShape> shapes = EnumSet.noneOf(RootShape.class);
        for (int root : roots)
            shapes.addAll(shapes(trie, root));
        assertEquals(EnumSet.allOf(RootShape.class), shapes);

        Set<PreviousUse> reachedCheckedState = EnumSet.noneOf(PreviousUse.class);
        for (Direction direction : Direction.values())
            for (int previousRoot : roots)
                for (PreviousUse previousUse : PreviousUse.values())
                    for (int root : roots)
                    {
                        InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> reused = new InMemoryRangeTrie.InMemoryRangeCursor<>(trie, direction, previousRoot);
                        if (prepare(trie, reused, previousRoot, previousUse))
                            reachedCheckedState.add(previousUse);
                        reused.reset(root);

                        InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> fresh = new InMemoryRangeTrie.InMemoryRangeCursor<>(trie, direction, root);
                        assertSameWalk(String.format("%s from root %d after %s, reset to root %d %s", direction, previousRoot, previousUse, root, shapes(trie, root)),
                                       fresh,
                                       reused);
                    }
        // Make sure the previous uses do leave the state that the reset must discard.
        assertEquals(EnumSet.allOf(PreviousUse.class), reachedCheckedState);
    }

    /// Use the cursor as described by `previousUse`.
    /// @return true if the cursor is left in the state that `previousUse` is meant to test
    private static boolean prepare(InMemoryRangeTrie<DeletionMarker> trie,
                                   InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> cursor,
                                   int root,
                                   PreviousUse previousUse)
    {
        switch (previousUse)
        {
            case FRESH:
                return true;
            case HALF_WALKED:
            {
                int steps = walkLength(new InMemoryRangeTrie.InMemoryRangeCursor<>(trie, cursor.direction, root));
                for (int i = 0; i < steps / 2; ++i)
                    cursor.advance();
                return cursor.depth > 0;
            }
            case ON_OPENING_BOUNDARY:
                // Leaves the opening boundary as the cursor's active and previous content.
                while (!Cursor.isExhausted(cursor.encodedPosition()))
                {
                    DeletionMarker content = cursor.content();
                    if (content != null && content.succedingState(cursor.direction) != null)
                        return true;
                    cursor.advance();
                }
                return false;
            case AFTER_SKIP:
            {
                // Skip to a position without content, which leaves the active state to be determined on request.
                InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> ahead = new InMemoryRangeTrie.InMemoryRangeCursor<>(trie, cursor.direction, root);
                long next = ahead.advance();
                while (!Cursor.isExhausted(next) && ahead.content() != null)
                {
                    cursor.advance();
                    next = ahead.advance();
                }
                if (Cursor.isExhausted(next))
                    return false;
                cursor.skipTo(next);
                return !cursor.activeIsSet;
            }
            case EXHAUSTED:
                walkLength(cursor);
                return true;
            default:
                throw new AssertionError();
        }
    }

    private static void assertSameWalk(String message,
                                       InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> expected,
                                       InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> actual)
    {
        while (true)
        {
            assertEquals(message, expected.encodedPosition(), actual.encodedPosition());
            assertEquals(message, expected.depth, actual.depth);
            assertEquals(message, expected.content(), actual.content());
            // Marker equality does not check positions.
            assertEquals(message, String.valueOf(expected.state()), String.valueOf(actual.state()));
            if (Cursor.isExhausted(expected.encodedPosition()))
                return;
            expected.advance();
            actual.advance();
        }
    }

    private static int walkLength(InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> cursor)
    {
        int steps = 0;
        while (!Cursor.isExhausted(cursor.advance()))
            ++steps;
        return steps;
    }

    /// All nodes a cursor visits in the trie, each of which can be the root of a cursor.
    private static Set<Integer> collectRoots(InMemoryRangeTrie<DeletionMarker> trie)
    {
        Set<Integer> roots = new LinkedHashSet<>();
        InMemoryRangeTrie.InMemoryRangeCursor<DeletionMarker> cursor = trie.makeCursor(Direction.FORWARD);
        while (!Cursor.isExhausted(cursor.encodedPosition()))
        {
            if (cursor.currentFullNode != NONE)
                roots.add(cursor.currentFullNode);
            cursor.advance();
        }
        return roots;
    }

    private static Set<RootShape> shapes(InMemoryRangeTrie<DeletionMarker> trie, int node)
    {
        Set<RootShape> shapes = EnumSet.noneOf(RootShape.class);
        if (isLeaf(node))
            shapes.add(RootShape.LEAF);
        else if (offset(node) == PREFIX_OFFSET)
        {
            for (int contentOffset : new int[]{ PREFIX_CONTENT_OFFSET, PREFIX_ALTERNATE_OFFSET })
            {
                int contentId = trie.getIntVolatile(node + contentOffset);
                if (isNull(contentId))
                    continue;
                assertTrue(isLeaf(contentId));
                shapes.add(trie.shouldPresentAfterBranch(contentId) ? RootShape.PREFIX_WITH_RETURN_CONTENT
                                                                    : RootShape.PREFIX_WITH_DESCENT_CONTENT);
            }
        }
        else
            shapes.add(RootShape.PLAIN_NODE);
        return shapes;
    }

    private static void addRange(InMemoryRangeTrie<DeletionMarker> trie,
                                 ByteComparable left, boolean leftInclusive,
                                 ByteComparable right, boolean rightInclusive,
                                 int timestamp) throws TrieSpaceExhaustedException
    {
        trie.apply(RangeTrie.range(left, leftInclusive, right, rightInclusive, VERSION, new DeletionMarker(left, timestamp, timestamp)),
                   DataPoint::combineDeletion,
                   v -> false);
    }

    private static ByteComparable key(int... bytes)
    {
        byte[] array = new byte[bytes.length];
        for (int i = 0; i < bytes.length; ++i)
            array[i] = (byte) bytes[i];
        return ByteComparable.preencoded(VERSION, array);
    }
}
