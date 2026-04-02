/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Tests for {@link IndexFilterTree} and {@link IndexFilterTreeNode}.
 *
 * Covers: tree construction, binary serialization (wire format
 * compatibility with Rust bool_tree.rs), De Morgan's NOT push-down
 * normalization.
 */
public class IndexFilterTreeTests extends OpenSearchTestCase {

    // ── CollectorLeaf serialization ─────────────────────────────────

    public void testCollectorLeafSerialization() {
        IndexFilterTreeNode.CollectorLeaf leaf = IndexFilterTreeNode.collectorLeaf(0, 7);
        byte[] bytes = leaf.serialize();
        // tag(1) + provider_id(2) + collector_idx(2) = 5 bytes
        assertEquals(5, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(3, buf.get()); // TAG_COLLECTOR
        assertEquals(0, buf.getShort()); // provider_id
        assertEquals(7, buf.getShort()); // collector_idx
    }

    public void testCollectorLeafNegativeIndex() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.collectorLeaf(0, -1));
    }

    public void testCollectorLeafNegativeProviderId() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.collectorLeaf(-1, 0));
    }

    public void testCollectorLeafWithProviderId() {
        IndexFilterTreeNode.CollectorLeaf leaf = IndexFilterTreeNode.collectorLeaf(5, 3);
        byte[] bytes = leaf.serialize();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(3, buf.get()); // TAG_COLLECTOR
        assertEquals(5, buf.getShort()); // provider_id
        assertEquals(3, buf.getShort()); // collector_idx
    }

    // ── PredicateLeaf serialization ─────────────────────────────────

    public void testPredicateLeafSerialization() {
        IndexFilterTreeNode.PredicateLeaf leaf = IndexFilterTreeNode.predicateLeaf(42);
        byte[] bytes = leaf.serialize();
        // tag(1) + predicate_id(2) = 3 bytes
        assertEquals(3, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(4, buf.get()); // TAG_PREDICATE
        assertEquals(42, buf.getShort()); // predicate_id
    }

    public void testPredicateLeafNegativeId() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.predicateLeaf(-1));
    }

    // ── AND/OR/NOT serialization ────────────────────────────────────

    public void testAndSerialization() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.predicateLeaf(0)
        );
        byte[] bytes = tree.serialize();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0, buf.get()); // TAG_AND
        assertEquals(2, buf.getShort()); // child_count
        // First child: CollectorLeaf(0, 0)
        assertEquals(3, buf.get()); // TAG_COLLECTOR
        assertEquals(0, buf.getShort()); // provider_id
        assertEquals(0, buf.getShort()); // collector_idx
        // Second child: PredicateLeaf(0)
        assertEquals(4, buf.get()); // TAG_PREDICATE
        assertEquals(0, buf.getShort()); // predicate_id
    }

    public void testOrSerialization() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.or(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.collectorLeaf(0, 1)
        );
        byte[] bytes = tree.serialize();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(1, buf.get()); // TAG_OR
        assertEquals(2, buf.getShort()); // child_count
    }

    public void testNotSerialization() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(IndexFilterTreeNode.collectorLeaf(0, 0));
        byte[] bytes = tree.serialize();
        assertEquals(2, bytes[0]); // TAG_NOT
        assertEquals(3, bytes[1]); // TAG_COLLECTOR (child)
    }

    public void testAndRequiresAtLeastTwoChildren() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.and(IndexFilterTreeNode.collectorLeaf(0, 0)));
    }

    public void testOrRequiresAtLeastTwoChildren() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.or(IndexFilterTreeNode.collectorLeaf(0, 0)));
    }

    // ── Complex tree serialization ──────────────────────────────────

    /**
     * Tree: AND(collector(0,0), OR(collector(0,1), predicate(0)))
     */
    public void testComplexTreeSerialization() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 1),
                IndexFilterTreeNode.predicateLeaf(0)
            )
        );
        byte[] bytes = tree.serialize();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // AND
        assertEquals(0, buf.get());
        assertEquals(2, buf.getShort());
        // collector(0, 0)
        assertEquals(3, buf.get());
        assertEquals(0, buf.getShort()); // provider_id
        assertEquals(0, buf.getShort()); // collector_idx
        // OR
        assertEquals(1, buf.get());
        assertEquals(2, buf.getShort());
        // collector(0, 1)
        assertEquals(3, buf.get());
        assertEquals(0, buf.getShort()); // provider_id
        assertEquals(1, buf.getShort()); // collector_idx
        // predicate(0)
        assertEquals(4, buf.get());
        assertEquals(0, buf.getShort()); // predicate_id
        // Should have consumed all bytes
        assertEquals(0, buf.remaining());
    }

    // ── De Morgan's NOT push-down normalization ─────────────────────

    /**
     * NOT(AND(Collector(0,0), Predicate(0)))
     * → OR(NOT(Collector(0,0)), NOT(Predicate(0)))
     * Both leaves keep NOT wrappers (collector: bitmap complement; predicate: Rust handles negation)
     */
    public void testNormalizationDeMorganAndToOr() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.predicateLeaf(0)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();

        assertTrue(normalized instanceof IndexFilterTreeNode.Or);
        IndexFilterTreeNode.Or orNode = (IndexFilterTreeNode.Or) normalized;
        assertEquals(2, orNode.children().size());

        // First child: NOT(Collector(0,0))
        assertTrue(orNode.children().get(0) instanceof IndexFilterTreeNode.Not);
        IndexFilterTreeNode.Not notChild0 = (IndexFilterTreeNode.Not) orNode.children().get(0);
        assertTrue(notChild0.child() instanceof IndexFilterTreeNode.CollectorLeaf);

        // Second child: NOT(Predicate(0))
        assertTrue(orNode.children().get(1) instanceof IndexFilterTreeNode.Not);
        IndexFilterTreeNode.Not notChild1 = (IndexFilterTreeNode.Not) orNode.children().get(1);
        assertTrue(notChild1.child() instanceof IndexFilterTreeNode.PredicateLeaf);
    }

    /**
     * NOT(OR(Collector(0,0), Collector(0,1)))
     * → AND(NOT(Collector(0,0)), NOT(Collector(0,1)))
     */
    public void testNormalizationDeMorganOrToAnd() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();

        assertTrue(normalized instanceof IndexFilterTreeNode.And);
        IndexFilterTreeNode.And andNode = (IndexFilterTreeNode.And) normalized;
        assertEquals(2, andNode.children().size());
        for (IndexFilterTreeNode child : andNode.children()) {
            assertTrue(child instanceof IndexFilterTreeNode.Not);
            assertTrue(((IndexFilterTreeNode.Not) child).child() instanceof IndexFilterTreeNode.CollectorLeaf);
        }
    }

    /**
     * NOT(NOT(Collector(0,0))) → Collector(0,0)
     */
    public void testNormalizationDoubleNegation() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.not(IndexFilterTreeNode.collectorLeaf(0, 0))
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertTrue(normalized instanceof IndexFilterTreeNode.CollectorLeaf);
        assertEquals(0, ((IndexFilterTreeNode.CollectorLeaf) normalized).collectorIndex());
    }

    /**
     * NOT(Predicate(0)) stays as NOT(Predicate(0))
     * Rust evaluator handles negation by inverting the resolved predicate operator.
     */
    public void testNormalizationPredicateKeepsNot() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(IndexFilterTreeNode.predicateLeaf(0));
        IndexFilterTreeNode normalized = tree.normalize();
        assertTrue(normalized instanceof IndexFilterTreeNode.Not);
        assertTrue(((IndexFilterTreeNode.Not) normalized).child() instanceof IndexFilterTreeNode.PredicateLeaf);
    }

    /**
     * NOT(Collector(0,0)) stays as NOT(Collector(0,0)) — bitmap complement is exact.
     */
    public void testNormalizationCollectorLeafKeepsNot() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(IndexFilterTreeNode.collectorLeaf(0, 0));
        IndexFilterTreeNode normalized = tree.normalize();
        assertTrue(normalized instanceof IndexFilterTreeNode.Not);
        assertTrue(((IndexFilterTreeNode.Not) normalized).child() instanceof IndexFilterTreeNode.CollectorLeaf);
    }

    /**
     * Leaves without NOT are unchanged by normalization.
     */
    public void testNormalizationLeavesUnchanged() {
        IndexFilterTreeNode collector = IndexFilterTreeNode.collectorLeaf(0, 5);
        assertSame(collector, collector.normalize());

        IndexFilterTreeNode predicate = IndexFilterTreeNode.predicateLeaf(3);
        assertSame(predicate, predicate.normalize());
    }

    /**
     * Verify serialized output is valid after normalization.
     */
    public void testNormalizationComplexTreeSerializesCorrectly() {
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 2),
                IndexFilterTreeNode.predicateLeaf(0)
            )
        );
        IndexFilterTree filterTree = new IndexFilterTree(tree, 3);
        IndexFilterTree normalized = filterTree.normalize();

        byte[] bytes = normalized.serialize();
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Verify root is OR (tag 1) after De Morgan's
        assertEquals(1, bytes[0]);
    }

    // ── IndexFilterTree wrapper tests ───────────────────────────────

    public void testIndexFilterTreeBasics() {
        IndexFilterTreeNode root = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.collectorLeaf(0, 1)
        );
        IndexFilterTree tree = new IndexFilterTree(root, 2);

        assertEquals(2, tree.collectorLeafCount());
        assertSame(root, tree.root());
    }

    public void testIndexFilterTreeNullRoot() {
        expectThrows(NullPointerException.class, () -> new IndexFilterTree(null, 0));
    }

    // ── Complex boolean tree (9 collector leaves + predicates) ──────

    public void testComplexNestedBoolTreeCase9() {
        // Branch_A: predicate(0) AND predicate(1) AND OR(predicate(2), AND(predicate(3), predicate(4), collector(0,0), NOT(predicate(5))))
        IndexFilterTreeNode branchA = IndexFilterTreeNode.and(
            IndexFilterTreeNode.predicateLeaf(0),
            IndexFilterTreeNode.predicateLeaf(1),
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.predicateLeaf(2),
                IndexFilterTreeNode.and(
                    IndexFilterTreeNode.or(
                        IndexFilterTreeNode.predicateLeaf(3),
                        IndexFilterTreeNode.predicateLeaf(4)
                    ),
                    IndexFilterTreeNode.predicateLeaf(5),
                    IndexFilterTreeNode.collectorLeaf(0, 0),
                    IndexFilterTreeNode.not(IndexFilterTreeNode.predicateLeaf(6))
                )
            )
        );

        // Branch_B with collectors
        IndexFilterTreeNode branchB = IndexFilterTreeNode.and(
            IndexFilterTreeNode.predicateLeaf(7),
            IndexFilterTreeNode.predicateLeaf(8),
            IndexFilterTreeNode.predicateLeaf(9),
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 1),
                IndexFilterTreeNode.collectorLeaf(0, 2),
                IndexFilterTreeNode.collectorLeaf(0, 3)
            ),
            IndexFilterTreeNode.not(IndexFilterTreeNode.predicateLeaf(10))
        );

        IndexFilterTreeNode filter1 = IndexFilterTreeNode.or(branchA, branchB);

        IndexFilterTreeNode branchC = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 4),
            IndexFilterTreeNode.predicateLeaf(11)
        );
        IndexFilterTreeNode branchD = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 5),
            IndexFilterTreeNode.predicateLeaf(12)
        );
        IndexFilterTreeNode branchE = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 6),
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.predicateLeaf(13),
                IndexFilterTreeNode.predicateLeaf(14)
            )
        );
        IndexFilterTreeNode filter2 = IndexFilterTreeNode.or(branchC, branchD, branchE);

        IndexFilterTreeNode should1 = IndexFilterTreeNode.and(
            IndexFilterTreeNode.predicateLeaf(15),
            IndexFilterTreeNode.predicateLeaf(16),
            IndexFilterTreeNode.collectorLeaf(0, 7)
        );
        IndexFilterTreeNode should2 = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 8),
            IndexFilterTreeNode.predicateLeaf(17)
        );
        IndexFilterTreeNode shouldClause = IndexFilterTreeNode.or(should1, should2);

        IndexFilterTreeNode root = IndexFilterTreeNode.and(filter1, filter2, shouldClause);

        IndexFilterTree tree = new IndexFilterTree(root, 9);

        // Serialize
        byte[] bytes = tree.serialize();
        assertNotNull(bytes);
        assertTrue("Serialized tree should be non-trivial", bytes.length > 50);

        // Normalize (De Morgan's)
        IndexFilterTree normalized = tree.normalize();
        byte[] normalizedBytes = normalized.serialize();
        assertNotNull(normalizedBytes);
        assertTrue(normalizedBytes.length > 0);
    }

    // ── Deep NOT normalization stress test ───────────────────────────

    public void testDeepNestedNotNormalization() {
        IndexFilterTreeNode branchA = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.not(IndexFilterTreeNode.predicateLeaf(0))
        );
        IndexFilterTreeNode branchB = IndexFilterTreeNode.and(
            IndexFilterTreeNode.not(IndexFilterTreeNode.collectorLeaf(0, 1)),
            IndexFilterTreeNode.predicateLeaf(1)
        );
        IndexFilterTreeNode branchC = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 2),
                IndexFilterTreeNode.predicateLeaf(2)
            )
        );
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.or(branchA, branchB, branchC)
        );

        IndexFilterTreeNode normalized = tree.normalize();

        // NOT(OR(...)) → AND(NOT(branchA), NOT(branchB), NOT(branchC))
        assertTrue("Root should be AND after De Morgan's", normalized instanceof IndexFilterTreeNode.And);
        IndexFilterTreeNode.And andNode = (IndexFilterTreeNode.And) normalized;
        assertEquals(3, andNode.children().size());

        // Verify it serializes without error
        byte[] bytes = normalized.serialize();
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }
}
