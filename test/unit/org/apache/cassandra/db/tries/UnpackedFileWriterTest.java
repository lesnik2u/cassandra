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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.RANGE_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.STRING_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertUnpackedWriterMatches;

/// Covers [UnpackedFileWriter]: that it writes byte-for-byte what [FileWriter] writes when the destination does no
/// page packing, and that what it writes reads back.
///
/// The shapes here are the ones the fold in [FileWriter.Node#make] turns on, which is the only place the two
/// writers can disagree. Wider coverage comes from the corpora of the other on-disk tests, which run through
/// [TrieUtil#assertUnpackedWriterMatches] on their way to disk. Deletion-aware tries are not covered by a byte
/// comparison at all -- see [DeletionAwareFileWriter#writeUnpacked] -- only by the round trip in
/// [OnDiskDeletionAwareTrieTest].
@RunWith(Parameterized.class)
public class UnpackedFileWriterTest
{
    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    @Parameterized.Parameter(0)
    public boolean isOrdered = true;

    @Parameterized.Parameters(name = "ordered {0}")
    public static List<Object[]> data()
    {
        return List.of(new Object[]{ Boolean.FALSE }, new Object[]{ Boolean.TRUE });
    }

    private final Random rand = new Random(1);

    @Test
    public void testEmpty() throws Exception
    {
        testKeys();
    }

    @Test
    public void testRootOnly() throws Exception
    {
        testKeys("");
    }

    @Test
    public void testRootContentWithChildren() throws Exception
    {
        testKeys("", "abc", "abd");
    }

    @Test
    public void testChain() throws Exception
    {
        testKeys("abcdef", "abfghijklmn");
    }

    /// A chain longer than [OnDiskWriteNodeType#MAX_CHAIN_LENGTH_INCLUSIVE] is written as several chain nodes, the
    /// deepest holding the last 64 bytes. The split is what the range form of `writeChain` has to reproduce.
    @Test
    public void testLongChains() throws Exception
    {
        for (int length : CHAIN_LENGTHS)
            testKeys("ab" + repeat('c', length));
    }

    /// The fold: a node with content and exactly one child whose edge carries a chain puts its transition into that
    /// chain rather than emitting a one-byte chain node of its own ([FileWriter.Node#make]). The unpacked writer has
    /// to decide this as it writes the child, before the parent exists. Lengths around the chain-node maximum matter
    /// because the folded byte can push the chain into one more node.
    @Test
    public void testSingleChildChainFold() throws Exception
    {
        for (int length : CHAIN_LENGTHS)
            testKeys("ab", "ab" + repeat('c', length));
    }

    /// The same, with the parent's content on the ascent side, which is the other payload slot.
    @Test
    public void testSingleChildChainFoldWithAscentContent() throws Exception
    {
        Assume.assumeTrue(isOrdered);
        for (int length : CHAIN_LENGTHS)
            testKeys("ab~", "ab" + repeat('c', length));
    }

    /// A parent that gains a second child after the first one has been written must not fold, and one whose only
    /// child arrives with no chain has nothing to fold.
    @Test
    public void testNoFold() throws Exception
    {
        testKeys("ab", "abcde", "abfgh");     // two children
        testKeys("ab", "abc");                // single child, no chain to fold into
        testKeys("ab", "abc", "abcde");       // single child, itself a prefix node
        // A chain that fills its shallowest node exactly, on an edge that cannot fold; the foldable form of the
        // same shape is covered by the tests above.
        for (int length : new int[]{ 64, 128 })
            testKeys("ab" + repeat('c', length + 1), "abd");
    }

    private static final int[] CHAIN_LENGTHS = { 1, 2, 3, 62, 63, 64, 65, 66, 127, 128, 129 };

    @Test
    public void testSparse() throws Exception
    {
        testNodesWithChildren(2, 3, 5, 10, 20, 25);
    }

    @Test
    public void testBitmap() throws Exception
    {
        testNodesWithChildren(26, 100, 200);
    }

    @Test
    public void testDense() throws Exception
    {
        testNodesWithChildren(250, 256);
    }

    /// Enough content to push the pointers past one byte, which changes the type selection as well as the widths.
    @Test
    public void testLargeTrie() throws Exception
    {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 5000; ++i)
            keys.add(String.format("%06d", rand.nextInt(50000)) + randomString(rand, 1 + rand.nextInt(80)));
        testKeys(keys.toArray(String[]::new));
    }

    /// Range tries are written with the ascent-side payload slot in use throughout, and are read back by a different
    /// cursor than a plain trie's.
    @Test
    public void testRangeTrie() throws Exception
    {
        Assume.assumeTrue(isOrdered);   // a range trie is always written ordered
        List<TestRangeState> markers = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
        {
            markers.add(new TestRangeState(TrieUtil.directComparable(String.format("%04d", i * 2)), -1, i + 1));
            markers.add(new TestRangeState(TrieUtil.directComparable(String.format("%04d", i * 2 + 1)), i + 1, -1));
        }
        TestRangeState.verify(markers);

        InMemoryRangeTrie<TestRangeState> expected = TestRangeState.fromList(markers);
        byte[] bytes = assertUnpackedWriterMatches(expected, true, RANGE_SERDE);
        try (OnDiskRangeTrie<TestRangeState> actual = OnDiskRangeTrie.open(fileOf(bytes), RANGE_SERDE, VERSION, -1))
        {
            TrieUtil.assertTriesEqual(expected, actual);
        }
    }

    private void testNodesWithChildren(int... childCounts) throws Exception
    {
        for (String postfix : List.of("ef", ""))
        {
            List<String> keys = new ArrayList<>();
            for (int count : childCounts)
            {
                String prefix = "ab00" + (count == 256 ? "0100" : String.format("%02x", count)) + "00cd";
                BitSet children = new BitSet(256);
                while (children.cardinality() < count)
                    children.set(rand.nextInt(256));
                for (int j = 0; j < 256; ++j)
                    if (children.get(j))
                        keys.add(prefix + String.format("%02x", j) + postfix);
            }
            testTrie(hexTrie(keys));
        }
    }

    /// Build a trie from the given keys -- a `~` suffix puts the value on the ascent side -- write it with both
    /// writers, compare, and read the result back.
    private void testKeys(String... keys) throws Exception
    {
        InMemoryTrie<String> trie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (String key : keys)
        {
            boolean ascentSide = key.endsWith("~");
            String value = key;
            if (ascentSide)
                key = key.substring(0, key.length() - 1);
            trie.putRecursive(TrieUtil.directComparable(key), value, ascentSide, (x, y) -> y);
        }
        testTrie(trie);
    }

    private InMemoryTrie<String> hexTrie(List<String> keys) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (String key : keys)
            trie.putRecursive(ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(key)), key, (x, y) -> y);
        return trie;
    }

    private void testTrie(InMemoryTrie<String> trie) throws IOException
    {
        byte[] bytes = assertUnpackedWriterMatches(trie, isOrdered, STRING_SERDE);
        try (OnDiskTrie<String> read = OnDiskTrie.open(fileOf(bytes), STRING_SERDE, VERSION, isOrdered, -1))
        {
            TrieUtil.assertTriesEqual(trie, read);
        }
    }

    /// The on-disk readers only open a file, so the bytes the unpacked writer produced have to be given one.
    private static File fileOf(byte[] bytes) throws IOException
    {
        File file = new File(java.io.File.createTempFile("unpacked", ".trie"));
        Files.write(file.toPath(), bytes);
        return file;
    }

    private static String repeat(char c, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            sb.append(c);
        return sb.toString();
    }

    private static String randomString(Random rand, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            sb.append((char) ('a' + rand.nextInt('z' - 'a' + 1)));
        return sb.toString();
    }
}
