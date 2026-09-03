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

import org.apache.cassandra.io.util.DataOutputPlus;

/// A [FileWriter] that emits every node as soon as its children are done, instead of building the whole trie as
/// [FileWriter.Node] objects and writing it once the root completes.
///
/// The node tree exists only so that [FileWriter#layoutChildren] can reorder branches to fit page boundaries. A
/// destination that reports no page limit -- an in-memory [org.apache.cassandra.io.util.DataOutputBuffer], which is
/// what the commit log and message paths write to -- never packs pages, so for it the tree is pure overhead: a node
/// and a children array per trie node, retained until the whole trie has been walked. This writer keeps only the
/// nodes on the current path, and past the first few tries it allocates nothing at all.
///
/// The output is byte-for-byte what [FileWriter] produces for the same trie and an unlimited page size: with page
/// packing disabled `writeNodeRecursively` emits children in walk order, which is the order an immediate writer
/// produces anyway. This writer therefore must not be given a paged destination.
///
/// The one thing it does not reproduce is the layout of a deletion-aware trie with more than one deletion branch;
/// see [DeletionAwareFileWriter#writeUnpacked].
public class UnpackedFileWriter<T> extends FileWriter<T>
{
    /// Number of bytes in the chain node whose code byte has not been written yet, or 0 when nothing is pending.
    ///
    /// When a node has a single child whose edge carries a chain, [FileWriter.Node#make] puts the transition into
    /// that chain instead of giving it a chain node of its own. That is decided when the parent completes, which can
    /// be long after an immediate writer has emitted the child. Only the last two bytes of the chain differ between
    /// the two outcomes, though -- `[chain code][transition][chain code for one byte]` against
    /// `[transition][chain code for one more byte]` -- so all that has to wait is the shallowest chain node's code
    /// byte and the transition that may join it. A chain that ends on a full node needs no decision at all: the
    /// folded byte would start a node of its own, which is exactly what the parent writes without the fold.
    private int pendingChainLength;
    /// The transition to the pending chain's node, which is no longer in `keyBytes` by the time it is written.
    private int pendingChainTransition;
    /// Index in `nodesOnPath` of the node whose transition folds if the chain turns out to be its only child's.
    private int pendingChainParent;

    public UnpackedFileWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        super(out, dataSerializer, swapAscentAndDescentSides);
    }

    @Override
    public void ascendTo(long newEncodedPosition) throws IOException
    {
        // ascend until closest node on path (if there's content, it will be current depth)
        //   if ascend depth reached, exit
        if (lastNodeOnPath < 0)
        {
            // fully empty trie; file is left empty, root is at 0
            return;
        }

        // throw away any path that did not result in content
        InProgressNode<T> node = nodesOnPath[lastNodeOnPath];
        assert node.depth <= keyPos;
        keyPos = node.depth;

        int newLength = Math.max(Cursor.depth(newEncodedPosition) - 1, -1);
        // If we are returning to an ascent-path position that doesn't advance, prepare to add content to the node at
        // this depth rather than add a new child to parent.
        if (Cursor.isOnReturnPath(newEncodedPosition) && (newLength == -1 || (keyBytes[newLength] & 0xFF) == Cursor.incomingTransition(newEncodedPosition)))
            ++newLength;

        // repeat:
        while (newLength < node.depth)
        {
            //   the chain written last may still be waiting for its closing byte; it takes this node's transition
            //   only if this node is the one it was written for -- so that no further child can arrive -- and that
            //   pending child is its only one
            resolvePendingChain(lastNodeOnPath == pendingChainParent && node.childCount == 0);
            --lastNodeOnPath;
            InProgressNode<T> next = lastNodeOnPath >= 0 ? nodesOnPath[lastNodeOnPath] : null;
            //   the path to reach this node includes the key bytes until there's node on path or we reach ascent depth - 1
            int stopDepth = (next != null ? Math.max(next.depth, newLength) : newLength);

            //   write the node; the chain above it is keyBytes[stopDepth + 1, chainEnd) and the byte that reaches it
            //   from its parent is keyBytes[stopDepth], both still in place in the key
            int chainEnd = keyPos;
            writeNodeBody(node);
            keyPos = stopDepth;

            //   if reached exhausted, child is root
            if (keyPos < 0)
            {
                OnDiskWriteNodeType.writeChain(out, keyBytes, stopDepth + 1, chainEnd);
                return; // the current file position is the root position
            }

            //   if reached ascend depth, add node on path, add child and exit
            if (next == null || next.depth < keyPos)
                next = addNewNode(keyPos);

            //   write the chain and add the child, leaving the fold decision to resolvePendingChain
            writeChainAndAddChild(stopDepth + 1, chainEnd, keyBytes[keyPos] & 0xFF);

            node = next;
        }
    }

    /// Write the chain formed by `keyBytes[from, to)` above the node just written, and record it as a child of the
    /// node the walk has ascended to -- `nodesOnPath[lastNodeOnPath]` -- reached by `transition`. Everything but the
    /// shallowest chain node's code byte is emitted; that byte and the transition are held back until
    /// [#resolvePendingChain] knows whether they fold.
    private void writeChainAndAddChild(int from, int to, int transition) throws IOException
    {
        int shallowest = (to - from) % OnDiskWriteNodeType.MAX_CHAIN_LENGTH_INCLUSIVE;
        OnDiskWriteNodeType.writeChain(out, keyBytes, from + shallowest, to);
        if (shallowest == 0)
        {
            nodesOnPath[lastNodeOnPath].addChild(transition, out.position());
            return;
        }

        for (int i = from + shallowest - 1; i >= from; --i)
            out.writeByte(keyBytes[i]);
        pendingChainLength = shallowest;
        pendingChainTransition = transition;
        pendingChainParent = lastNodeOnPath;
    }

    /// Close the chain [#writeChainAndAddChild] left open, with or without the parent's transition in it.
    private void resolvePendingChain(boolean fold) throws IOException
    {
        if (pendingChainLength == 0)
            return;

        int length = pendingChainLength;
        int transition = pendingChainTransition;
        InProgressNode<T> parent = nodesOnPath[pendingChainParent];
        pendingChainLength = 0;
        if (fold)
        {
            out.writeByte(transition);
            out.writeByte(OnDiskWriteNodeType.CHAIN.bits | length);
            // The transition is part of the chain now, so the parent has no child node to write for it.
            parent.addChild(-1, out.position());
        }
        else
        {
            out.writeByte(OnDiskWriteNodeType.CHAIN.bits | (length - 1));
            parent.addChild(transition, out.position());
        }
    }

    @Override
    void flushPendingChain() throws IOException
    {
        // Something else is going into the stream, so the chain cannot still take a transition that has not arrived.
        resolvePendingChain(false);
    }

    /// Write the children and payload of a completed node, and clear it for reuse. The chain above the node is the
    /// caller's to write, because whether the node's own transition belongs to it is only known there.
    private void writeNodeBody(InProgressNode<T> node) throws IOException
    {
        int size = node.childCount;
        boolean hasChildren = size > 0;
        assert hasChildren || node.ascentPathContent != null || node.descentPathContent != null;

        if (hasChildren)
            writeChildren(node.children, size);

        T descentContent = swapAscentAndDescentSides ? node.ascentPathContent : node.descentPathContent;
        T ascentContent = swapAscentAndDescentSides ? node.descentPathContent : node.ascentPathContent;
        if (descentContent != null || ascentContent != null)
            OnDiskWriteNodeType.writePayload(out, dataSerializer, descentContent, ascentContent, hasChildren);

        node.resetKeepingChildHolders();
    }

    /// Serialize a trie, as [FileWriter#write(BaseTrie, boolean, DataSerializer, org.apache.cassandra.io.util.File)]
    /// does, into a destination that does not page.
    public static <T> void write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, DataOutputPlus out) throws IOException
    {
        FileWriter.write(trie, new UnpackedFileWriter<>(out, serializer, isOrdered));
    }
}
