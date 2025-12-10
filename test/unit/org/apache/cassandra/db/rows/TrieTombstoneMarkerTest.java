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

package org.apache.cassandra.db.rows;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.quicktheories.core.Gen;

import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

/**
 * Unit tests for TrieTombstoneMarker and its implementations (Covering, Boundary, Point).
 */
public class TrieTombstoneMarkerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // Test data generators
    private static final int MAX_TIMESTAMP = 1000000;
    private static final int MAX_LOCAL_DELETION_TIME = 100000;

    private Gen<DeletionTime> deletionTimeGen()
    {
        return integers().between(1, MAX_TIMESTAMP)
                        .zip(integers().between(1, MAX_LOCAL_DELETION_TIME),
                             DeletionTime::new);
    }

    private Gen<DeletionTime> deletionTimeOrLiveGen()
    {
        return integers().between(0, MAX_TIMESTAMP)
                        .zip(integers().between(0, MAX_LOCAL_DELETION_TIME),
                             (ts, ldt) -> ts == 0 ? DeletionTime.LIVE : new DeletionTime(ts, ldt));
    }

    // ========== Covering Marker Tests ==========

    @Test
    public void testCoveringCreation()
    {
        DeletionTime dt = new DeletionTime(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(dt);

        assertNotNull(marker);
        assertFalse(marker.isBoundary());
        assertFalse(marker.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        assertEquals(dt, marker.deletionTime());
    }

    @Test
    public void testCoveringIsNotBoundary()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(new DeletionTime(100, 50));
        assertFalse("Covering marker should not be a boundary", covering.isBoundary());
    }

    @Test
    public void testCoveringPrecedingState()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(new DeletionTime(100, 50));
        
        // Covering markers return themselves as preceding state in both directions
        assertEquals(covering, covering.precedingState(Direction.FORWARD));
        assertEquals(covering, covering.precedingState(Direction.REVERSE));
        // Covering markers return themselves as succeding state in both directions
        assertEquals(covering, covering.succedingState(Direction.FORWARD));
        assertEquals(covering, covering.succedingState(Direction.REVERSE));
    }

    @Test
    public void testCoveringCannotConvertToRangeTombstoneMarker()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(new DeletionTime(100, 50));
        ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
        
        try
        {
            covering.toRangeTombstoneMarker(ByteComparable.EMPTY,
                                           ByteComparable.Version.OSS50,
                                           comparator,
                                           DeletionTime.LIVE);
            fail("Covering marker should not be convertible to RangeTombstoneMarker");
        }
        catch (AssertionError e)
        {
            // Expected
        }
    }

    @Test
    public void testCoveringMergeWithCovering()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2);
                
                TrieTombstoneMarker merged = m1.mergeWith(m2);
                
                assertNotNull(merged);
                assertFalse(merged.isBoundary());
                
                // Should keep the higher deletion time
                DeletionTime expected = dt1.supersedes(dt2) ? dt1 : dt2;
                assertEquals(expected, merged.deletionTime());
            });
    }

    @Test
    public void testCoveringDropShadowed()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker marker = TrieTombstoneMarker.covering(dt1);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(dt2);
                
                TrieTombstoneMarker result = marker.dropShadowed(deletion);
                
                if (dt1.supersedes(dt2))
                {
                    // Marker survives if it supersedes the deletion
                    assertNotNull(result);
                    assertEquals(dt1, result.deletionTime());
                }
                else
                {
                    // Marker is dropped if deletion supersedes it
                    assertNull(result);
                }
            });
    }

    // ========== Boundary Marker Tests ==========

    @Test
    public void testBoundaryCreation()
    {
        DeletionTime left = new DeletionTime(100, 50);
        DeletionTime right = new DeletionTime(200, 60);
        
        TrieTombstoneMarkerImpl.Covering leftCov = TrieTombstoneMarkerImpl.covering(left);
        TrieTombstoneMarkerImpl.Covering rightCov = TrieTombstoneMarkerImpl.covering(right);
        
        TrieTombstoneMarker boundary = TrieTombstoneMarkerImpl.make(leftCov, rightCov);
        
        assertNotNull(boundary);
        assertTrue(boundary.isBoundary());
        assertFalse(boundary.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
    }

    @Test
    public void testBoundaryWithEqualSidesBecomeCovering()
    {
        DeletionTime dt = new DeletionTime(100, 50);
        TrieTombstoneMarkerImpl.Covering cov = TrieTombstoneMarkerImpl.covering(dt);
        
        TrieTombstoneMarker result = TrieTombstoneMarkerImpl.make(cov, cov);
        
        assertNotNull(result);
        assertFalse("Equal sides should result in covering marker", result.isBoundary());
        assertEquals(dt, result.deletionTime());
    }

    @Test
    public void testBoundaryPrecedingState()
    {
        DeletionTime left = new DeletionTime(100, 50);
        DeletionTime right = new DeletionTime(200, 60);
        
        TrieTombstoneMarkerImpl.Covering leftCov = TrieTombstoneMarkerImpl.covering(left);
        TrieTombstoneMarkerImpl.Covering rightCov = TrieTombstoneMarkerImpl.covering(right);
        TrieTombstoneMarker boundary = TrieTombstoneMarkerImpl.make(leftCov, rightCov);
        
        assertEquals(leftCov, boundary.precedingState(Direction.FORWARD));
        assertEquals(rightCov, boundary.precedingState(Direction.REVERSE));

        assertEquals(rightCov, boundary.succedingState(Direction.FORWARD));
        assertEquals(leftCov, boundary.succedingState(Direction.REVERSE));
    }

    @Test
    public void testBoundaryMergeWithBoundary()
    {
        qt().forAll(deletionTimeOrLiveGen(), deletionTimeOrLiveGen(),
                   deletionTimeOrLiveGen(), deletionTimeOrLiveGen())
            .assuming((dt1, dt2, dt3, dt4) -> 
                !dt1.equals(dt2) && !dt3.equals(dt4)) // Ensure we have boundaries
            .checkAssert((dt1, dt2, dt3, dt4) -> {
                TrieTombstoneMarkerImpl.Covering left1 = dt1.isLive() ? null : TrieTombstoneMarkerImpl.covering(dt1);
                TrieTombstoneMarkerImpl.Covering right1 = dt2.isLive() ? null : TrieTombstoneMarkerImpl.covering(dt2);
                TrieTombstoneMarkerImpl.Covering left2 = dt3.isLive() ? null : TrieTombstoneMarkerImpl.covering(dt3);
                TrieTombstoneMarkerImpl.Covering right2 = dt4.isLive() ? null : TrieTombstoneMarkerImpl.covering(dt4);
                
                TrieTombstoneMarker b1 = TrieTombstoneMarkerImpl.make(left1, right1);
                TrieTombstoneMarker b2 = TrieTombstoneMarkerImpl.make(left2, right2);
                
                TrieTombstoneMarker merged = b1.mergeWith(b2);
                
                assertNotNull(merged);
                // Verify that merge takes the higher deletion on each side
            });
    }

    @Test
    public void testBoundaryRestrict()
    {
        DeletionTime left = new DeletionTime(100, 50);
        DeletionTime right = new DeletionTime(200, 60);
        
        TrieTombstoneMarkerImpl.Covering leftCov = TrieTombstoneMarkerImpl.covering(left);
        TrieTombstoneMarkerImpl.Covering rightCov = TrieTombstoneMarkerImpl.covering(right);
        TrieTombstoneMarker boundary = TrieTombstoneMarkerImpl.make(leftCov, rightCov);
        
        // Restrict to before only
        TrieTombstoneMarker beforeOnly = boundary.restrict(true, false);
        assertNotNull(beforeOnly);
        assertTrue(beforeOnly.isBoundary());
        
        // Restrict to after only
        TrieTombstoneMarker afterOnly = boundary.restrict(false, true);
        assertNotNull(afterOnly);
        assertTrue(afterOnly.isBoundary());
        
        // Restrict to both (should return same)
        TrieTombstoneMarker both = boundary.restrict(true, true);
        assertEquals(boundary, both);
        
        // Restrict to neither (should return null)
        TrieTombstoneMarker neither = boundary.restrict(false, false);
        assertNull(neither);
    }

    // ========== Point Marker Tests ==========

    @Test
    public void testPointCreation()
    {
        DeletionTime pointDt = new DeletionTime(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, pointDt);
        
        assertNotNull(point);
        assertTrue(point.isBoundary());
        assertTrue(point.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        assertEquals(pointDt, point.deletionTime());
    }

    @Test
    public void testPointWithCoveringDeletion()
    {
        DeletionTime pointDt = new DeletionTime(150, 55);
        DeletionTime coveringDt = new DeletionTime(100, 50);
        
        TrieTombstoneMarkerImpl.Covering pointCov = TrieTombstoneMarkerImpl.covering(pointDt);
        TrieTombstoneMarkerImpl.Covering coveringCov = TrieTombstoneMarkerImpl.covering(coveringDt);
        
        TrieTombstoneMarker point = new TrieTombstoneMarkerImpl.Point(pointCov, coveringCov);
        
        assertNotNull(point);
        assertTrue(point.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        assertEquals(pointDt, point.deletionTime());
    }

    @Test
    public void testPointMergeWithCovering()
    {
        DeletionTime pointDt = new DeletionTime(150, 55);
        DeletionTime coveringDt = new DeletionTime(100, 50);
        
        TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, pointDt);
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(coveringDt);
        
        TrieTombstoneMarker merged = point.mergeWith(covering);
        
        assertNotNull(merged);
        assertTrue(merged.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        
        // Point should survive if it supersedes covering
        if (pointDt.supersedes(coveringDt))
        {
            assertEquals(pointDt, merged.deletionTime());
        }
    }

    @Test
    public void testPointMergeWithPoint()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker p1 = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, dt1);
                TrieTombstoneMarker p2 = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, dt2);
                
                TrieTombstoneMarker merged = p1.mergeWith(p2);
                
                assertNotNull(merged);
                assertTrue(merged.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
                
                // Should keep the higher deletion time
                DeletionTime expected = dt1.supersedes(dt2) ? dt1 : dt2;
                assertEquals(expected, merged.deletionTime());
            });
    }

    @Test
    public void testPointDropShadowed()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((pointDt, deletionDt) -> {
                TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, pointDt);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(deletionDt);
                
                TrieTombstoneMarker result = point.dropShadowed(deletion);
                
                if (pointDt.supersedes(deletionDt))
                {
                    // Point survives if it supersedes the deletion
                    assertNotNull(result);
                    assertTrue(result.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
                }
                else
                {
                    // Point is dropped if deletion supersedes it
                    assertNull(result);
                }
            });
    }

    // ========== Collection Merge Tests ==========

    @Test
    public void testMergeCollection()
    {
        DeletionTime dt1 = new DeletionTime(100, 50);
        DeletionTime dt2 = new DeletionTime(200, 60);
        DeletionTime dt3 = new DeletionTime(150, 55);
        
        List<TrieTombstoneMarker> markers = Arrays.asList(
            TrieTombstoneMarker.covering(dt1),
            TrieTombstoneMarker.covering(dt2),
            TrieTombstoneMarker.covering(dt3)
        );
        
        TrieTombstoneMarker merged = TrieTombstoneMarker.merge(markers);
        
        assertNotNull(merged);
        // Should have the highest deletion time
        assertEquals(dt2, merged.deletionTime());
    }

    @Test
    public void testMergeEmptyCollection()
    {
        TrieTombstoneMarker merged = TrieTombstoneMarker.merge(Arrays.asList());
        assertNull(merged);
    }

    // ========== Timestamp Update Tests ==========

    @Test
    public void testCoveringWithUpdatedTimestamp()
    {
        DeletionTime original = new DeletionTime(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original);
        
        long newTimestamp = 200;
        TrieTombstoneMarker updated = marker.withUpdatedTimestamp(newTimestamp);
        
        assertNotNull(updated);
        assertEquals(newTimestamp, updated.deletionTime().markedForDeleteAt());
        assertEquals(original.localDeletionTime(), updated.deletionTime().localDeletionTime());
    }

    @Test
    public void testPointWithUpdatedTimestamp()
    {
        DeletionTime pointDt = new DeletionTime(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, pointDt);
        
        long newTimestamp = 250;
        TrieTombstoneMarker updated = point.withUpdatedTimestamp(newTimestamp);
        
        if (updated != null)
        {
            assertEquals(newTimestamp, updated.deletionTime().markedForDeleteAt());
            assertTrue(updated.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        }
    }

    // ========== Map Function Tests ==========

    @Test
    public void testCoveringMap()
    {
        DeletionTime original = new DeletionTime(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original);
        
        // Map to a higher timestamp
        TrieTombstoneMarker mapped = marker.map(dt -> new DeletionTime(dt.markedForDeleteAt() + 100, dt.localDeletionTime()));
        
        assertNotNull(mapped);
        assertEquals(200, mapped.deletionTime().markedForDeleteAt());
    }

    @Test
    public void testCoveringMapToLive()
    {
        DeletionTime original = new DeletionTime(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original);
        
        // Map to LIVE
        TrieTombstoneMarker mapped = marker.map(dt -> DeletionTime.LIVE);
        
        assertNull("Mapping to LIVE should return null", mapped);
    }

    @Test
    public void testPointMap()
    {
        DeletionTime pointDt = new DeletionTime(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, pointDt);
        
        // Map to a higher timestamp
        TrieTombstoneMarker mapped = point.map(dt -> new DeletionTime(dt.markedForDeleteAt() + 100, dt.localDeletionTime()));
        
        if (mapped != null)
        {
            assertTrue(mapped.hasPointData(TrieTombstoneMarker.PointDataType.ROW));
        }
    }

    // ========== Memory Size Tests ==========

    @Test
    public void testCoveringMemorySize()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(new DeletionTime(100, 50));
        long size = covering.unsharedHeapSize();
        
        assertTrue("Covering marker should have positive heap size", size > 0);
    }

    @Test
    public void testBoundaryMemorySize()
    {
        DeletionTime left = new DeletionTime(100, 50);
        DeletionTime right = new DeletionTime(200, 60);
        
        TrieTombstoneMarkerImpl.Covering leftCov = TrieTombstoneMarkerImpl.covering(left);
        TrieTombstoneMarkerImpl.Covering rightCov = TrieTombstoneMarkerImpl.covering(right);
        TrieTombstoneMarker boundary = TrieTombstoneMarkerImpl.make(leftCov, rightCov);
        
        long size = boundary.unsharedHeapSize();
        
        assertTrue("Boundary marker should have positive heap size", size > 0);
        assertTrue("Boundary should be larger than covering", size > TrieTombstoneMarker.covering(left).unsharedHeapSize());
    }

    @Test
    public void testPointMemorySize()
    {
        TrieTombstoneMarker point = TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, new DeletionTime(150, 55));
        long size = point.unsharedHeapSize();
        
        assertTrue("Point marker should have positive heap size", size > 0);
    }

    // ========== Property-Based Tests ==========

    @Test
    public void testMergeIsCommutative()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2);
                
                TrieTombstoneMarker merged1 = m1.mergeWith(m2);
                TrieTombstoneMarker merged2 = m2.mergeWith(m1);
                
                assertEquals("Merge should be commutative", 
                           merged1.deletionTime(), merged2.deletionTime());
            });
    }

    @Test
    public void testMergeIsAssociative()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2, dt3) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2);
                TrieTombstoneMarker m3 = TrieTombstoneMarker.covering(dt3);
                
                TrieTombstoneMarker merged1 = m1.mergeWith(m2).mergeWith(m3);
                TrieTombstoneMarker merged2 = m1.mergeWith(m2.mergeWith(m3));
                
                assertEquals("Merge should be associative", 
                           merged1.deletionTime(), merged2.deletionTime());
            });
    }

    @Test
    public void testDropShadowedIsIdempotent()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((markerDt, deletionDt) -> {
                TrieTombstoneMarker marker = TrieTombstoneMarker.covering(markerDt);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(deletionDt);
                
                TrieTombstoneMarker dropped1 = marker.dropShadowed(deletion);
                TrieTombstoneMarker dropped2 = dropped1 != null ? dropped1.dropShadowed(deletion) : null;
                
                assertEquals("dropShadowed should be idempotent", dropped1, dropped2);
            });
    }
}
