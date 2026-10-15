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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_RANDOM_SEED;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/// Tests that a deletion-aware mutator used for many mutations, as memtables do, gives the same result as merging
/// all the mutations. In particular, the existing deletions must be applied correctly to the incoming data at every
/// deletion branch, also when an earlier mutation left the cursor over a deletion branch in the middle of that branch.
public class InMemoryDeletionAwareTrieMutatorTest
{
    static final int PARTITIONS = 8;

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test
    public void testExistingDeletionsAppliedToIncomingData() throws TrieSpaceExhaustedException
    {
        testExistingDeletionsAppliedToIncomingData(true);
        testExistingDeletionsAppliedToIncomingData(false);
    }

    private void testExistingDeletionsAppliedToIncomingData(boolean deletionsAtFixedPoints) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = mutator(trie, deletionsAtFixedPoints);

        // Two deleted ranges in partition 40 and one in 50, all newer than the data that follows.
        mutator.apply(deletion(0x40, 0x2000, 0x3000, 10));
        mutator.apply(deletion(0x40, 0x5000, 0x6000, 10));
        mutator.apply(deletion(0x50, 0x2000, 0x3000, 10));

        // Data ends between the two ranges, leaving the deletion branch half-walked.
        mutator.apply(rows(0x40, 5, 0x2500, 0x4000));
        assertNull(trie.get(key(0x40, 0x2500)));
        assertNotNull(trie.get(key(0x40, 0x4000)));

        // A different branch next must not see anything of the previous walk.
        mutator.apply(rows(0x50, 5, 0x1000, 0x2500, 0x4000));
        assertNotNull(trie.get(key(0x50, 0x1000)));
        assertNull(trie.get(key(0x50, 0x2500)));
        assertNotNull(trie.get(key(0x50, 0x4000)));

        // Both branches in a single mutation.
        mutator.apply(rows(0x40, 5, 0x1000, 0x5500).mergeWith(rows(0x50, 5, 0x2fff, 0x3000),
                                                               LivePoint::combine,
                                                               DeletionMarker::combine,
                                                               DeletionMarker::applyTo,
                                                               deletionsAtFixedPoints));
        assertNotNull(trie.get(key(0x40, 0x1000)));
        assertNull(trie.get(key(0x40, 0x5500)));
        assertNull(trie.get(key(0x50, 0x2fff)));
        assertNotNull(trie.get(key(0x50, 0x3000)));
    }

    @Test
    public void testPartitionDeletionsAppliedToIncomingData() throws TrieSpaceExhaustedException
    {
        testPartitionDeletionsAppliedToIncomingData(true);
        testPartitionDeletionsAppliedToIncomingData(false);
    }

    /// Deletion branches whose root carries a covering deletion (as partition deletions are written), so that a
    /// cursor at the branch root has the closing boundary waiting on its return path.
    private void testPartitionDeletionsAppliedToIncomingData(boolean deletionsAtFixedPoints) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = mutator(trie, deletionsAtFixedPoints);

        mutator.apply(partitionDeletion(0x40, 10));
        mutator.apply(partitionDeletion(0x50, 10));

        // The first creates the mutator's cursor over our deletion branch, the second reuses it.
        mutator.apply(rows(0x40, 5, 0x1000));
        mutator.apply(rows(0x50, 5, 0x1000));
        assertNull(trie.get(key(0x40, 0x1000)));
        assertNull(trie.get(key(0x50, 0x1000)));
    }

    @Test
    public void testRandomMutationsMatchMergedView() throws TrieSpaceExhaustedException
    {
        long seed = TEST_RANDOM_SEED.getLong(new Random().nextLong());
        System.out.println("Seed: " + seed + " (rerun with -D" + TEST_RANDOM_SEED.getKey() + '=' + seed + ')');
        testRandomMutationsMatchMergedView(new Random(seed), true);
        testRandomMutationsMatchMergedView(new Random(seed), false);
    }

    private void testRandomMutationsMatchMergedView(Random rand, boolean deletionsAtFixedPoints) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = mutator(trie, deletionsAtFixedPoints);
        List<DeletionAwareTrie<LivePoint, DeletionMarker>> mutations = new ArrayList<>();

        for (int i = 0; i < 1000; ++i)
        {
            DeletionAwareTrie<LivePoint, DeletionMarker> mutation = randomMutation(rand);
            mutator.apply(mutation);
            mutations.add(mutation);
        }

        // The mutator's result must match a merged view of all mutations, which does not involve the mutator.
        List<DataPoint> expected = toList(DeletionAwareTrie.merge(mutations,
                                                                  LivePoint::combineCollection,
                                                                  DeletionMarker::combineCollection,
                                                                  DeletionMarker::applyTo,
                                                                  deletionsAtFixedPoints));
        List<DataPoint> actual = toList(trie);
        assertEquals(expected, actual);
        assertEquals(expected.toString(), actual.toString()); // marker equality does not check positions
    }

    private static InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>.Mutator<LivePoint, DeletionMarker>
    mutator(InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie, boolean deletionsAtFixedPoints)
    {
        return trie.mutator(DataPoint::combineLive,
                            DataPoint::combineDeletion,
                            DataPoint::deleteLive,
                            DataPoint::deleteLive,
                            deletionsAtFixedPoints,
                            v -> false);
    }

    /// A mutation touching a few partitions, each with a few rows and possibly a deleted range. The deletions are
    /// rooted at the partition.
    private static DeletionAwareTrie<LivePoint, DeletionMarker> randomMutation(Random rand) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> mutation = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = mutator(mutation, true);
        int partitionCount = 1 + rand.nextInt(3);
        for (int i = 0; i < partitionCount; ++i)
        {
            int partition = 0x10 * (1 + rand.nextInt(PARTITIONS));
            int rowCount = rand.nextInt(4);
            for (int j = 0; j < rowCount; ++j)
            {
                ByteComparable key = key(partition, rand.nextInt(0x10000));
                mutator.apply(DeletionAwareTrie.singleton(key, VERSION, new LivePoint(key, rand.nextInt(20))));
            }
            if (rand.nextBoolean())
            {
                int left = rand.nextInt(0xFFFF);
                int right = left + 1 + rand.nextInt(0xFFFF - left);
                mutator.apply(deletion(partition, left, right, rand.nextInt(20)));
            }
        }
        return mutation;
    }

    private static DeletionAwareTrie<LivePoint, DeletionMarker> rows(int partition, int timestamp, int... clusterings) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> mutation = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = mutator(mutation, true);
        for (int clustering : clusterings)
        {
            ByteComparable key = key(partition, clustering);
            mutator.apply(DeletionAwareTrie.singleton(key, VERSION, new LivePoint(key, timestamp)));
        }
        return mutation;
    }

    private static DeletionAwareTrie<LivePoint, DeletionMarker> deletion(int partition, int left, int right, int timestamp)
    {
        return DeletionAwareTrie.deletedRange(bytes(partition),
                                              clustering(left),
                                              clustering(right),
                                              VERSION,
                                              new DeletionMarker(key(partition, left), timestamp, timestamp));
    }

    private static DeletionAwareTrie<LivePoint, DeletionMarker> partitionDeletion(int partition, int timestamp)
    {
        return DeletionAwareTrie.deletionBranch(bytes(partition),
                                                VERSION,
                                                RangeTrie.branch(ByteComparable.EMPTY,
                                                                 VERSION,
                                                                 new DeletionMarker(bytes(partition), timestamp, timestamp)));
    }

    private static ByteComparable key(int partition, int clustering)
    {
        return ByteComparable.preencoded(VERSION, new byte[]{ (byte) partition, (byte) (clustering >> 8), (byte) clustering });
    }

    private static ByteComparable clustering(int clustering)
    {
        return ByteComparable.preencoded(VERSION, new byte[]{ (byte) (clustering >> 8), (byte) clustering });
    }

    private static ByteComparable bytes(int partition)
    {
        return ByteComparable.preencoded(VERSION, new byte[]{ (byte) partition });
    }
}
