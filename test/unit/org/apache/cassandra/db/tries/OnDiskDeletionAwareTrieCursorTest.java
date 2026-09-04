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

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.LIVE;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.MARKER;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// Checks that the on-disk deletion-aware cursor presents exactly the same walk as the in-memory one it was
/// written from, for both [Cursor#advance] and [Cursor#advanceMultiple], and that each cursor keeps the
/// invariant that [Cursor#encodedPosition] returns what the advancing call just returned.
///
/// The invariant matters because consumers do not always advance every source: [FlexibleMergeCursor], while it
/// is walking a deletion branch, reads the live cursor's [Cursor#encodedPosition] without advancing it. A
/// cursor that reports a stale position there makes the merge ask for a deletion branch that was already
/// consumed, which repeats the deletion branch in the output.
public class OnDiskDeletionAwareTrieCursorTest
{
    @BeforeClass
    public static void setUp()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void testEmpty() throws IOException
    {
        assertWalksMatch(points());
    }

    @Test
    public void testOneLivePoint() throws IOException
    {
        assertWalksMatch(points(live("abc", 1)));
    }

    @Test
    public void testTenLivePoints() throws IOException
    {
        List<DataPoint> points = new ArrayList<>();
        for (int i = 0; i < 10; ++i)
            points.add(live(String.format("ab%d", i), i + 1));
        assertWalksMatch(points);
    }

    @Test
    public void testRangeWithLivePointsInside() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), live("abd", 1), live("abe", 2), marker("abf", 5, -1)));
    }

    /// Live content after the deletion branch keeps the walk going past it, which is why this shape worked
    /// while the two below did not.
    @Test
    public void testRangeWithLivePointAfter() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3)));
    }

    /// A range tombstone with nothing live in the trie at all: the deletion branch is the last thing the walk
    /// sees, so the cursor is exhausted immediately after it.
    @Test
    public void testRangeWithNoLiveContent() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), marker("abd", 5, -1)));
    }

    /// Live content before the deletion branch but none after it, which exhausts the cursor at the same point
    /// as the case above while leaving the trie non-trivial.
    @Test
    public void testRangeWithLivePointBeforeOnly() throws IOException
    {
        assertWalksMatch(points(live("aaa", 1), marker("abc", -1, 5), marker("abd", 5, -1)));
    }

    /// The narrowest range: two adjacent keys, and no live content. This is the trie-level shape of a row
    /// deletion that carries no cells.
    @Test
    public void testAdjacentKeyRangeWithNoLiveContent() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), marker("abc0", 5, -1)));
    }

    @Test
    public void testDeletionBranchAtRootWithNoLiveContent() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), marker("abd", 5, -1)), true);
    }

    @Test
    public void testDeletionBranchAtRootWithLiveContent() throws IOException
    {
        assertWalksMatch(points(marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3)), true);
    }

    private void assertWalksMatch(List<DataPoint> points) throws IOException
    {
        assertWalksMatch(points, false);
    }

    private void assertWalksMatch(List<DataPoint> points, boolean deletionsAtRoot) throws IOException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points, false, deletionsAtRoot);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(source, LIVE, MARKER, out);
            ByteBuffer buffer = out.asNewBuffer();
            try (OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                     OnDiskDeletionAwareTrie.open(buffer, LIVE, MARKER, VERSION, -1))
            {
                for (Direction direction : Direction.values())
                {
                    for (boolean multiple : new boolean[]{ false, true })
                    {
                        assertPositionsMatch(source.cursor(direction), read.cursor(direction), multiple);
                        assertPositionIsCurrent(read.cursor(direction), multiple);
                        assertPositionIsCurrent(source.cursor(direction), multiple);
                    }
                }

                // The end-to-end symptom: an intersection asks for the deletion branch through the position
                // flags of the merge's live source, so a stale position surfaces here as a repeated branch.
                assertEquals(DataPoint.toList(source.intersect(TrieSet.full(VERSION))),
                             DataPoint.toList(read.intersect(TrieSet.full(VERSION))));
            }
        }
    }

    /// Walks the two cursors in lockstep, comparing the position, the content and the deletion branch at every
    /// step. `advanceMultiple` is allowed to skip positions, but only ones without content or a deletion
    /// branch, so the two implementations must agree on what they present even if they differ on what they
    /// skip; the comparison below tolerates a skipped position on either side.
    private static void assertPositionsMatch(DeletionAwareCursor<LivePoint, DeletionMarker> expected,
                                             DeletionAwareCursor<LivePoint, DeletionMarker> actual,
                                             boolean multiple)
    {
        long expectedPosition = expected.encodedPosition();
        long actualPosition = actual.encodedPosition();
        while (true)
        {
            // Skipped positions carry neither content nor a deletion branch, so only advance the cursor that
            // is behind, and compare only where the two meet.
            long cmp = Cursor.compare(expectedPosition, actualPosition);
            if (cmp == 0)
            {
                assertEquals("Position", Cursor.toString(expectedPosition), Cursor.toString(actualPosition));
                if (Cursor.isExhausted(expectedPosition))
                    return;
                assertEquals("Content", Cursor.content(expected, expectedPosition), Cursor.content(actual, actualPosition));
                RangeCursor<DeletionMarker> expectedBranch = DeletionAwareCursor.deletionBranchCursor(expected, expectedPosition);
                RangeCursor<DeletionMarker> actualBranch = DeletionAwareCursor.deletionBranchCursor(actual, actualPosition);
                assertEquals("Deletion branch present", expectedBranch != null, actualBranch != null);
                if (expectedBranch != null)
                    TrieUtil.assertCursorWalksEqual(expectedBranch, actualBranch);
                expectedPosition = advance(expected, multiple);
                actualPosition = advance(actual, multiple);
            }
            else if (cmp < 0)
            {
                assertSkippable(expected, expectedPosition);
                expectedPosition = advance(expected, multiple);
            }
            else
            {
                assertSkippable(actual, actualPosition);
                actualPosition = advance(actual, multiple);
            }
        }
    }

    /// A position only one of the two cursors stopped on must have been skippable, i.e. carry no content and
    /// no deletion branch.
    private static void assertSkippable(DeletionAwareCursor<LivePoint, DeletionMarker> cursor, long position)
    {
        assertEquals("Content at position " + Cursor.toString(position) + " skipped by the other cursor",
                     null, Cursor.content(cursor, position));
        assertEquals("Deletion branch at position " + Cursor.toString(position) + " skipped by the other cursor",
                     null, DeletionAwareCursor.deletionBranchCursor(cursor, position));
    }

    /// Verifies that the cursor's own [Cursor#encodedPosition] agrees with what the advancing call returned,
    /// including at the end of the walk.
    private static void assertPositionIsCurrent(DeletionAwareCursor<LivePoint, DeletionMarker> cursor, boolean multiple)
    {
        long position = cursor.encodedPosition();
        while (!Cursor.isExhausted(position))
        {
            position = advance(cursor, multiple);
            assertEquals("Reported position after advance", Cursor.toString(position), Cursor.toString(cursor.encodedPosition()));
        }
    }

    private static long advance(DeletionAwareCursor<LivePoint, DeletionMarker> cursor, boolean multiple)
    {
        return multiple ? cursor.advanceMultiple(null) : cursor.advance();
    }

    private static List<DataPoint> points(DataPoint... p)
    {
        List<DataPoint> list = new ArrayList<>();
        for (DataPoint x : p)
            list.add(x);
        return list;
    }

    private static LivePoint live(String key, int timestamp)
    {
        return new LivePoint(TrieUtil.comparable(key), timestamp);
    }

    private static DeletionMarker marker(String key, int left, int right)
    {
        return new DeletionMarker(TrieUtil.comparable(key), left, right);
    }
}
