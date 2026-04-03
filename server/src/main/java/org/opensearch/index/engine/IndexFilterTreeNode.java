/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A node in a boolean filter tree for multi-engine query decomposition.
 * <p>
 * The tree supports five node types:
 * <ul>
 *   <li>{@link And} — logical conjunction of children</li>
 *   <li>{@link Or} — logical disjunction of children</li>
 *   <li>{@link Not} — logical negation of a single child</li>
 *   <li>{@link CollectorLeaf} — backed by a collector callback from an index engine (e.g. Lucene)</li>
 *   <li>{@link PredicateLeaf} — backed by columnar storage predicate</li>
 * </ul>
 * <p>
 * Nodes are immutable once constructed. Use the static factory methods
 * to build trees, then serialize via {@link #serialize()} for JNI transport.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class IndexFilterTreeNode {

    // Wire-format tags matching the Rust bool_tree.rs deserializer
    static final byte TAG_AND = 0;
    static final byte TAG_OR = 1;
    static final byte TAG_NOT = 2;
    static final byte TAG_COLLECTOR = 3;
    static final byte TAG_PREDICATE = 4;

    IndexFilterTreeNode() {}

    // ── Static factory methods ──────────────────────────────────────

    /** Creates an AND node with the given children. */
    public static And and(IndexFilterTreeNode... children) {
        return new And(Arrays.asList(children));
    }

    /** Creates an AND node with the given children. */
    public static And and(List<IndexFilterTreeNode> children) {
        return new And(children);
    }

    /** Creates an OR node with the given children. */
    public static Or or(IndexFilterTreeNode... children) {
        return new Or(Arrays.asList(children));
    }

    /** Creates an OR node with the given children. */
    public static Or or(List<IndexFilterTreeNode> children) {
        return new Or(children);
    }

    /** Creates a NOT node wrapping the given child. */
    public static Not not(IndexFilterTreeNode child) {
        return new Not(child);
    }

    /**
     * Creates a collector leaf referencing the collector at the given index,
     * associated with the given provider.
     *
     * @param providerId     identifies which collector engine handles this leaf
     * @param collectorIndex the index of the collector within the provider
     */
    public static CollectorLeaf collectorLeaf(int providerId, int collectorIndex) {
        return new CollectorLeaf(providerId, collectorIndex);
    }

    /** Creates a predicate leaf referencing a predicate by ID (resolved from Substrait plan on Rust side). */
    public static PredicateLeaf predicateLeaf(int predicateId) {
        return new PredicateLeaf(predicateId);
    }

    /**
     * Serializes this node into the compact binary wire format for JNI transport.
     *
     * @return the serialized byte array
     */
    public abstract byte[] serialize();

    /** Returns the number of bytes this node will occupy when serialized. */
    abstract int serializedSize();

    /**
     * Applies De Morgan's NOT push-down normalization.
     * After normalization, NOT nodes only appear directly above CollectorLeaf nodes.
     * PredicateLeaf nodes under NOT have their operator inverted instead.
     *
     * @return a normalized copy of this tree
     */
    public abstract IndexFilterTreeNode normalize();

    /**
     * Returns the negated form of this node for De Morgan's push-down.
     * Called when this node is the child of a NOT during normalization.
     */
    abstract IndexFilterTreeNode negate();

    // ── Node implementations ────────────────────────────────────────

    /** Logical AND of children. @opensearch.experimental */
    @ExperimentalApi
    public static final class And extends IndexFilterTreeNode {
        private final List<IndexFilterTreeNode> children;

        And(List<IndexFilterTreeNode> children) {
            if (children.size() < 2) {
                throw new IllegalArgumentException("AND requires at least 2 children");
            }
            this.children = Collections.unmodifiableList(children);
        }

        public List<IndexFilterTreeNode> children() {
            return children;
        }

        @Override
        public byte[] serialize() {
            ByteBuffer buf = ByteBuffer.allocate(serializedSize()).order(ByteOrder.LITTLE_ENDIAN);
            buf.put(TAG_AND);
            buf.putShort((short) children.size());
            for (IndexFilterTreeNode child : children) {
                buf.put(child.serialize());
            }
            return buf.array();
        }

        @Override
        int serializedSize() {
            int size = 1 + 2;
            for (IndexFilterTreeNode child : children) {
                size += child.serializedSize();
            }
            return size;
        }

        @Override
        public IndexFilterTreeNode normalize() {
            IndexFilterTreeNode[] normalized = new IndexFilterTreeNode[children.size()];
            for (int i = 0; i < children.size(); i++) {
                normalized[i] = children.get(i).normalize();
            }
            return IndexFilterTreeNode.and(normalized);
        }

        @Override
        IndexFilterTreeNode negate() {
            // De Morgan's: NOT(AND(a,b,...)) -> OR(NOT(a), NOT(b), ...)
            IndexFilterTreeNode[] negated = new IndexFilterTreeNode[children.size()];
            for (int i = 0; i < children.size(); i++) {
                negated[i] = children.get(i).negate();
            }
            return IndexFilterTreeNode.or(negated);
        }
    }

    /** Logical OR of children. @opensearch.experimental */
    @ExperimentalApi
    public static final class Or extends IndexFilterTreeNode {
        private final List<IndexFilterTreeNode> children;

        Or(List<IndexFilterTreeNode> children) {
            if (children.size() < 2) {
                throw new IllegalArgumentException("OR requires at least 2 children");
            }
            this.children = Collections.unmodifiableList(children);
        }

        public List<IndexFilterTreeNode> children() {
            return children;
        }

        @Override
        public byte[] serialize() {
            ByteBuffer buf = ByteBuffer.allocate(serializedSize()).order(ByteOrder.LITTLE_ENDIAN);
            buf.put(TAG_OR);
            buf.putShort((short) children.size());
            for (IndexFilterTreeNode child : children) {
                buf.put(child.serialize());
            }
            return buf.array();
        }

        @Override
        int serializedSize() {
            int size = 1 + 2;
            for (IndexFilterTreeNode child : children) {
                size += child.serializedSize();
            }
            return size;
        }

        @Override
        public IndexFilterTreeNode normalize() {
            IndexFilterTreeNode[] normalized = new IndexFilterTreeNode[children.size()];
            for (int i = 0; i < children.size(); i++) {
                normalized[i] = children.get(i).normalize();
            }
            return IndexFilterTreeNode.or(normalized);
        }

        @Override
        IndexFilterTreeNode negate() {
            // De Morgan's: NOT(OR(a,b,...)) -> AND(NOT(a), NOT(b), ...)
            IndexFilterTreeNode[] negated = new IndexFilterTreeNode[children.size()];
            for (int i = 0; i < children.size(); i++) {
                negated[i] = children.get(i).negate();
            }
            return IndexFilterTreeNode.and(negated);
        }
    }

    /** Logical NOT of a single child. @opensearch.experimental */
    @ExperimentalApi
    public static final class Not extends IndexFilterTreeNode {
        private final IndexFilterTreeNode child;

        Not(IndexFilterTreeNode child) {
            this.child = Objects.requireNonNull(child);
        }

        public IndexFilterTreeNode child() {
            return child;
        }

        @Override
        public byte[] serialize() {
            byte[] childBytes = child.serialize();
            byte[] result = new byte[1 + childBytes.length];
            result[0] = TAG_NOT;
            System.arraycopy(childBytes, 0, result, 1, childBytes.length);
            return result;
        }

        @Override
        int serializedSize() {
            return 1 + child.serializedSize();
        }

        @Override
        public IndexFilterTreeNode normalize() {
            // Push NOT down into the child
            return child.negate();
        }

        @Override
        IndexFilterTreeNode negate() {
            // Double negation: NOT(NOT(x)) -> normalize(x)
            return child.normalize();
        }
    }

    /**
     * Collector leaf resolved by a collector callback from an index engine.
     * Carries a {@code providerId} identifying which engine handles it and
     * a {@code collectorIndex} referencing the specific collector within that provider.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class CollectorLeaf extends IndexFilterTreeNode {
        private final int providerId;
        private final int collectorIndex;

        CollectorLeaf(int providerId, int collectorIndex) {
            if (providerId < 0) {
                throw new IllegalArgumentException("providerId must be non-negative");
            }
            if (collectorIndex < 0) {
                throw new IllegalArgumentException("collectorIndex must be non-negative");
            }
            this.providerId = providerId;
            this.collectorIndex = collectorIndex;
        }

        /** Returns the provider ID identifying which collector engine handles this leaf. */
        public int providerId() {
            return providerId;
        }

        /** Returns the collector index within the provider. */
        public int collectorIndex() {
            return collectorIndex;
        }

        @Override
        public byte[] serialize() {
            // TAG_COLLECTOR(1 byte) + provider_id:u16(2 bytes) + collector_idx:u16(2 bytes) = 5 bytes
            ByteBuffer buf = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
            buf.put(TAG_COLLECTOR);
            buf.putShort((short) providerId);
            buf.putShort((short) collectorIndex);
            return buf.array();
        }

        @Override
        int serializedSize() {
            return 5;
        }

        @Override
        public IndexFilterTreeNode normalize() {
            return this;
        }

        @Override
        IndexFilterTreeNode negate() {
            // NOT(CollectorLeaf) stays as NOT(CollectorLeaf) — bitmap complement is exact
            return IndexFilterTreeNode.not(this);
        }
    }

    /**
     * Predicate leaf resolved by the primary engine evaluating a columnar predicate on Parquet data.
     *
    /**
     * Predicate leaf referencing a predicate by ID.
     * <p>
     * The actual column name, operator, and value are resolved on the Rust side
     * from the Substrait plan during tree resolution. This avoids duplicating
     * predicate details in both the tree wire format and the Substrait plan.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class PredicateLeaf extends IndexFilterTreeNode {
        private final int predicateId;

        PredicateLeaf(int predicateId) {
            if (predicateId < 0) {
                throw new IllegalArgumentException("predicateId must be non-negative");
            }
            this.predicateId = predicateId;
        }

        /** Returns the predicate ID (index into the predicate list from the Substrait plan). */
        public int predicateId() {
            return predicateId;
        }

        @Override
        public byte[] serialize() {
            // TAG_PREDICATE(1 byte) + predicate_id:u16(2 bytes) = 3 bytes
            ByteBuffer buf = ByteBuffer.allocate(3).order(ByteOrder.LITTLE_ENDIAN);
            buf.put(TAG_PREDICATE);
            buf.putShort((short) predicateId);
            return buf.array();
        }

        @Override
        int serializedSize() {
            return 3;
        }

        @Override
        public IndexFilterTreeNode normalize() {
            return this;
        }

        @Override
        IndexFilterTreeNode negate() {
            // NOT(PredicateLeaf) — the negation is handled by the Rust evaluator
            // which resolves the predicate from the Substrait plan and can invert
            // the operator there. We keep the NOT wrapper here.
            return IndexFilterTreeNode.not(this);
        }
    }
}
