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

package org.apache.cassandra.test.microbench.tries;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/// Cost of constructing cursors on a populated long-lived [InMemoryTrie], one operation per invocation so that
/// `-prof gc`'s `gc.alloc.rate.norm` reads directly as bytes per operation.
///
/// Keys are an 8-byte prefix followed by an 8-byte suffix, approximating a partition key token followed by a
/// clustering. `getDirect` is the control: [InMemoryTrie#get] walks node addresses without creating a cursor.
///
/// `Cursor` is package-private, so the cursor-based lookup goes through a tail trie at the empty prefix, a view whose
/// `get` is the default `BaseTrie.get`: one `InMemoryCursor` from `tailCursor`, then `descendAlong`.
///
/// Under `-f 0` the `@Fork` JVM arguments are ignored and must be given on the command line: `-ea`,
/// `-XX:CompileCommandFile=conf/hotspot_compiler` (the path is relative to the repo root and the JVM silently ignores
/// a missing file) and the heap size.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-XX:CompileCommandFile=conf/hotspot_compiler", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Benchmark)
public class TrieCursorAllocationBench
{
    @Param({"ON_HEAP", "OFF_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"OSS50"})
    ByteComparable.Version byteComparableVersion = ByteComparable.Version.OSS50;

    /// `prefixCount`x`suffixesPerPrefix`. One parameter rather than two because JMH runs the cross product of
    /// parameters. `1000000x1` has more branching nodes on the path to each key, so more lookups need more
    /// backtracking entries than a cursor's initial allocation holds.
    @Param({"1000x100", "1000000x1"})
    String shape = "1000x100";

    int prefixCount;
    int suffixesPerPrefix;

    /// Number of entries covered by each `sliceIterateSmall` subtrie. When a prefix has fewer keys than this, slices
    /// cover consecutive keys across prefixes.
    @Param({"8"})
    int sliceSize = 8;

    InMemoryTrie<Integer> trie;
    /// The whole of `trie` as a view without the [InMemoryTrie#get] shortcut.
    Trie<Integer> cursorView;
    /// Full keys, in random order.
    ByteComparable[] keys;
    /// Key prefixes, in random order.
    ByteComparable[] prefixes;
    /// Inclusive bounds of subtries each covering `sliceSize` consecutive keys, in random order.
    ByteComparable[] sliceLefts;
    ByteComparable[] sliceRights;

    int nextKey;
    int nextPrefix;
    int nextSlice;

    final static InMemoryTrie.UpsertTransformer<Integer, Integer> resolver = (x, y) -> y;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        String[] dimensions = shape.split("x");
        prefixCount = Integer.parseInt(dimensions[0]);
        suffixesPerPrefix = Integer.parseInt(dimensions[1]);
        trie = InMemoryTrie.longLived(byteComparableVersion, bufferType, null);
        Random rand = new Random(1);

        int count = prefixCount * suffixesPerPrefix;
        keys = new ByteComparable[count];
        prefixes = new ByteComparable[prefixCount];
        int slicesPerPrefix = suffixesPerPrefix / sliceSize;
        sliceLefts = new ByteComparable[slicesPerPrefix > 0 ? prefixCount * slicesPerPrefix : count / sliceSize];
        sliceRights = new ByteComparable[sliceLefts.length];
        int keyIndex = 0;
        int sliceIndex = 0;
        for (int p = 0; p < prefixCount; ++p)
        {
            long prefix = rand.nextLong();
            prefixes[p] = ByteComparable.preencoded(byteComparableVersion, ByteBuffer.allocate(8).putLong(0, prefix));

            long[] suffixes = new long[suffixesPerPrefix];
            for (int s = 0; s < suffixesPerPrefix; ++s)
                suffixes[s] = rand.nextLong() >>> 1; // non-negative, so that numeric order is byte order
            Arrays.sort(suffixes);
            ByteComparable[] prefixKeys = new ByteComparable[suffixesPerPrefix];
            for (int s = 0; s < suffixesPerPrefix; ++s)
            {
                ByteBuffer key = ByteBuffer.allocate(16).putLong(0, prefix).putLong(8, suffixes[s]);
                prefixKeys[s] = ByteComparable.preencoded(byteComparableVersion, key);
                trie.putRecursive(prefixKeys[s], keyIndex, resolver);
                keys[keyIndex++] = prefixKeys[s];
            }
            for (int s = 0; s + sliceSize <= suffixesPerPrefix; s += sliceSize)
            {
                sliceLefts[sliceIndex] = prefixKeys[s];
                sliceRights[sliceIndex] = prefixKeys[s + sliceSize - 1];
                ++sliceIndex;
            }
        }
        if (slicesPerPrefix == 0)
        {
            // Values are key indexes, so iterating the trie lists the keys in order.
            int position = 0;
            for (Integer index : trie.values())
            {
                if (position / sliceSize == sliceLefts.length)
                    break;
                if (position % sliceSize == 0)
                    sliceLefts[position / sliceSize] = keys[index];
                if (position % sliceSize == sliceSize - 1)
                    sliceRights[position / sliceSize] = keys[index];
                ++position;
            }
        }
        shuffle(keys, rand);
        shuffle(prefixes, rand);
        shufflePairs(sliceLefts, sliceRights, rand);

        cursorView = trie.tailTrie(ByteComparable.EMPTY);

        // Check that the benchmarks find what they are meant to.
        Integer expected = trie.get(keys[0]);
        if (expected == null || !expected.equals(cursorView.get(keys[0])))
            throw new AssertionError("getViaCursor does not find the key");
        int sliceEntries = 0;
        for (Integer value : trie.subtrie(sliceLefts[0], sliceRights[0]).values())
            ++sliceEntries;
        if (sliceEntries != sliceSize)
            throw new AssertionError("Slice has " + sliceEntries + " entries instead of " + sliceSize);
        if (trie.tailTrie(prefixes[0]).valueIterator().next() == null)
            throw new AssertionError("Tail trie has no entries");

        System.out.format("Trie size on heap %,d off heap %,d\n",
                          trie.usedSizeOnHeap(), trie.usedSizeOffHeap());
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        trie.discardBuffers();
        trie = null;
        cursorView = null;
    }

    @Benchmark
    public void getDirect(Blackhole bh)
    {
        bh.consume(trie.get(nextKey()));
    }

    /// One cursor per lookup, as in the default `BaseTrie.get` used by tries that are views.
    @Benchmark
    public void getViaCursor(Blackhole bh)
    {
        bh.consume(cursorView.get(nextKey()));
    }

    /// Tail trie at a prefix, then its first entry, as a partition read does at the start of the partition.
    @Benchmark
    public void tailTrieFirst(Blackhole bh)
    {
        Trie<Integer> tail = trie.tailTrie(nextPrefix());
        Iterator<Integer> values = tail.valueIterator();
        bh.consume(values.next());
    }

    /// For the `1000000x1` shape the eight keys share only about two leading bytes, so this is effectively an
    /// eight-partition range scan; compare results only within a shape.
    @Benchmark
    public void sliceIterateSmall(Blackhole bh)
    {
        int i = nextSlice();
        for (Integer value : trie.subtrie(sliceLefts[i], sliceRights[i]).values())
            bh.consume(value);
    }

    /// The pure allocation cost of a cursor.
    @Benchmark
    public void cursorOnly(Blackhole bh)
    {
        bh.consume(trie.cursor(Direction.FORWARD));
    }

    private ByteComparable nextKey()
    {
        ByteComparable key = keys[nextKey];
        if (++nextKey == keys.length)
            nextKey = 0;
        return key;
    }

    private ByteComparable nextPrefix()
    {
        ByteComparable prefix = prefixes[nextPrefix];
        if (++nextPrefix == prefixes.length)
            nextPrefix = 0;
        return prefix;
    }

    private int nextSlice()
    {
        int i = nextSlice;
        if (++nextSlice == sliceLefts.length)
            nextSlice = 0;
        return i;
    }

    private static <T> void shuffle(T[] array, Random rand)
    {
        for (int i = array.length - 1; i > 0; --i)
        {
            int j = rand.nextInt(i + 1);
            T t = array[i];
            array[i] = array[j];
            array[j] = t;
        }
    }

    private static <T> void shufflePairs(T[] a, T[] b, Random rand)
    {
        for (int i = a.length - 1; i > 0; --i)
        {
            int j = rand.nextInt(i + 1);
            T t = a[i];
            a[i] = a[j];
            a[j] = t;
            t = b[i];
            b[i] = b[j];
            b[j] = t;
        }
    }
}
