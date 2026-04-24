/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Boolean tree structure for multi-engine query decomposition.
 * <p>
 * Wraps the root {@link IndexFilterTreeNode} and provides compact
 * binary serialization for JNI transport to the native (Rust) layer.
 * <p>
 * The tree supports two leaf types:
 * <ul>
 *   <li>{@link IndexFilterTreeNode.CollectorLeaf} — resolved by a collector
 *       callback from an index engine (e.g. Lucene). Format-aware via providerId.</li>
 *   <li>{@link IndexFilterTreeNode.PredicateLeaf} — a reference to a predicate
 *       resolved from the Substrait plan on the Rust side.</li>
 * </ul>
 * <p>
 * Before evaluation, call {@link #normalize()} to apply De Morgan's
 * NOT push-down.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterTree implements Closeable {

    private final IndexFilterTreeNode root;
    private final int collectorLeafCount;

    /**
     * Creates an IndexFilterTree with the given root node.
     *
     * @param root the root node of the boolean tree
     * @param collectorLeafCount the number of distinct CollectorLeaf nodes in the tree
     */
    public IndexFilterTree(IndexFilterTreeNode root, int collectorLeafCount) {
        this.root = Objects.requireNonNull(root, "root must not be null");
        this.collectorLeafCount = collectorLeafCount;
    }

    /** Returns the root node of the boolean tree. */
    public IndexFilterTreeNode root() {
        return root;
    }

    /** Returns the number of CollectorLeaf nodes in the tree. */
    public int collectorLeafCount() {
        return collectorLeafCount;
    }

    /**
     * Serializes the tree into the compact binary wire format.
     *
     * @return the serialized byte array for JNI transport
     */
    public byte[] serialize() {
        return root.serialize();
    }

    /**
     * Returns a new IndexFilterTree with De Morgan's NOT push-down applied.
     * After normalization, NOT nodes only appear directly above CollectorLeaf
     * nodes (where bitmap complement is exact). PredicateLeaf nodes under NOT
     * are wrapped in NOT — the Rust evaluator handles the negation by inverting
     * the resolved predicate operator.
     */
    public IndexFilterTree normalize() {
        IndexFilterTreeNode normalizedRoot = root.normalize();
        return new IndexFilterTree(normalizedRoot, collectorLeafCount);
    }

    @Override
    public void close() throws IOException {}
}
