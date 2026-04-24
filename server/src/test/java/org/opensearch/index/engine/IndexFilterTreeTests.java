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
 * Unit tests for {@link IndexFilterTreeNode} and {@link IndexFilterTree}.
 * Covers serialization wire format, validation, De Morgan's normalization,
 * and the IndexFilterTree wrapper.
 */
public class IndexFilterTreeTests extends OpenSearchTestCase {

    // ── CollectorLeaf serialization ─────────────────────────────────

    public void testCollectorLeafSerialization() {
        IndexFilterTreeNode.CollectorLeaf leaf = IndexFilterTreeNode.collectorLeaf(1, 2);
        byte[] bytes = leaf.serialize();
        // TAG_COLLECTOR(0x03) + provider_id:u16(1) + collector_idx:u16(2) = 5 bytes
        assertEquals(5, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x03, buf.get());
        assertEquals(1, buf.getShort());
        assertEquals(2, buf.getShort());
    }

    public void testCollectorLeafZeroIndices() {
        IndexFilterTreeNode.CollectorLeaf leaf = IndexFilterTreeNode.collectorLeaf(0, 0);
        byte[] bytes = leaf.serialize();
        assertEquals(5, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x03, buf.get());
        assertEquals(0, buf.getShort());
        assertEquals(0, buf.getShort());
    }

    // ── PredicateLeaf serialization ─────────────────────────────────

    public void testPredicateLeafSerialization() {
        IndexFilterTreeNode.PredicateLeaf leaf = IndexFilterTreeNode.predicateLeaf(5);
        byte[] bytes = leaf.serialize();
        // TAG_PREDICATE(0x04) + predicate_id:u16(5) = 3 bytes
        assertEquals(3, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x04, buf.get());
        assertEquals(5, buf.getShort());
    }

    public void testPredicateLeafZeroId() {
        IndexFilterTreeNode.PredicateLeaf leaf = IndexFilterTreeNode.predicateLeaf(0);
        byte[] bytes = leaf.serialize();
        assertEquals(3, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x04, buf.get());
        assertEquals(0, buf.getShort());
    }

    // ── AND serialization ───────────────────────────────────────────

    public void testAndSerialization() {
        IndexFilterTreeNode node = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.predicateLeaf(1)
        );
        byte[] bytes = node.serialize();
        // TAG_AND(0x00) + child_count:u16(2) + CollectorLeaf(5) + PredicateLeaf(3) = 1+2+5+3 = 11
        assertEquals(11, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x00, buf.get());       // AND tag
        assertEquals(2, buf.getShort());     // child count
        assertEquals(0x03, buf.get());       // CollectorLeaf tag
        assertEquals(0, buf.getShort());     // provider_id
        assertEquals(0, buf.getShort());     // collector_idx
        assertEquals(0x04, buf.get());       // PredicateLeaf tag
        assertEquals(1, buf.getShort());     // predicate_id
    }

    // ── OR serialization ────────────────────────────────────────────

    public void testOrSerialization() {
        IndexFilterTreeNode node = IndexFilterTreeNode.or(
            IndexFilterTreeNode.predicateLeaf(0),
            IndexFilterTreeNode.predicateLeaf(1)
        );
        byte[] bytes = node.serialize();
        // TAG_OR(0x01) + child_count:u16(2) + PredicateLeaf(3) + PredicateLeaf(3) = 1+2+3+3 = 9
        assertEquals(9, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x01, buf.get());       // OR tag
        assertEquals(2, buf.getShort());     // child count
        assertEquals(0x04, buf.get());       // PredicateLeaf tag
        assertEquals(0, buf.getShort());     // predicate_id 0
        assertEquals(0x04, buf.get());       // PredicateLeaf tag
        assertEquals(1, buf.getShort());     // predicate_id 1
    }

    // ── NOT serialization ───────────────────────────────────────────

    public void testNotSerialization() {
        IndexFilterTreeNode node = IndexFilterTreeNode.not(
            IndexFilterTreeNode.collectorLeaf(2, 3)
        );
        byte[] bytes = node.serialize();
        // TAG_NOT(0x02) + CollectorLeaf(5) = 1+5 = 6
        assertEquals(6, bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x02, buf.get());       // NOT tag
        assertEquals(0x03, buf.get());       // CollectorLeaf tag
        assertEquals(2, buf.getShort());     // provider_id
        assertEquals(3, buf.getShort());     // collector_idx
    }

    // ── Validation ──────────────────────────────────────────────────

    public void testCollectorLeafNegativeProviderIdThrows() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.collectorLeaf(-1, 0));
    }

    public void testCollectorLeafNegativeCollectorIndexThrows() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.collectorLeaf(0, -1));
    }

    public void testPredicateLeafNegativeIdThrows() {
        expectThrows(IllegalArgumentException.class, () -> IndexFilterTreeNode.predicateLeaf(-1));
    }

    public void testAndRequiresMinTwoChildren() {
        expectThrows(IllegalArgumentException.class, () ->
            IndexFilterTreeNode.and(IndexFilterTreeNode.predicateLeaf(0))
        );
    }

    public void testOrRequiresMinTwoChildren() {
        expectThrows(IllegalArgumentException.class, () ->
            IndexFilterTreeNode.or(IndexFilterTreeNode.predicateLeaf(0))
        );
    }

    // ── De Morgan's normalization ───────────────────────────────────

    public void testNormalizationDeMorganAndToOr() {
        // NOT(AND(a, b)) → OR(NOT(a), NOT(b))
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertInstanceOf(IndexFilterTreeNode.Or.class, normalized);
        IndexFilterTreeNode.Or or = (IndexFilterTreeNode.Or) normalized;
        assertEquals(2, or.children().size());
        // Each child should be NOT(CollectorLeaf)
        for (IndexFilterTreeNode child : or.children()) {
            assertInstanceOf(IndexFilterTreeNode.Not.class, child);
            assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class, ((IndexFilterTreeNode.Not) child).child());
        }
    }

    public void testNormalizationDeMorganOrToAnd() {
        // NOT(OR(a, b)) → AND(NOT(a), NOT(b))
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertInstanceOf(IndexFilterTreeNode.And.class, normalized);
        IndexFilterTreeNode.And and = (IndexFilterTreeNode.And) normalized;
        assertEquals(2, and.children().size());
        for (IndexFilterTreeNode child : and.children()) {
            assertInstanceOf(IndexFilterTreeNode.Not.class, child);
            assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class, ((IndexFilterTreeNode.Not) child).child());
        }
    }

    public void testNormalizationDoubleNegation() {
        // NOT(NOT(x)) → x
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.not(
                IndexFilterTreeNode.collectorLeaf(0, 0)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class, normalized);
        assertEquals(0, ((IndexFilterTreeNode.CollectorLeaf) normalized).providerId());
        assertEquals(0, ((IndexFilterTreeNode.CollectorLeaf) normalized).collectorIndex());
    }

    public void testNormalizationPredicateKeepsNot() {
        // NOT(PredicateLeaf) → PredicateLeaf (Rust handles inversion)
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.predicateLeaf(7)
        );
        IndexFilterTreeNode normalized = tree.normalize();
        // PredicateLeaf.negate() returns itself — NOT is absorbed
        assertInstanceOf(IndexFilterTreeNode.PredicateLeaf.class, normalized);
        assertEquals(7, ((IndexFilterTreeNode.PredicateLeaf) normalized).predicateId());
    }

    public void testNormalizationCollectorLeafKeepsNot() {
        // NOT(CollectorLeaf) stays as NOT(CollectorLeaf) — bitmap complement
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.collectorLeaf(1, 2)
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertInstanceOf(IndexFilterTreeNode.Not.class, normalized);
        IndexFilterTreeNode.Not notNode = (IndexFilterTreeNode.Not) normalized;
        assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class, notNode.child());
        assertEquals(1, ((IndexFilterTreeNode.CollectorLeaf) notNode.child()).providerId());
        assertEquals(2, ((IndexFilterTreeNode.CollectorLeaf) notNode.child()).collectorIndex());
    }

    public void testNormalizationLeavesUnchanged() {
        // Leaves without NOT wrapper are unchanged
        IndexFilterTreeNode collector = IndexFilterTreeNode.collectorLeaf(0, 0);
        assertSame(collector, collector.normalize());

        IndexFilterTreeNode predicate = IndexFilterTreeNode.predicateLeaf(0);
        assertSame(predicate, predicate.normalize());
    }

    // ── Complex nested tree ─────────────────────────────────────────

    public void testComplexTreeSerialization() {
        // AND(OR(Collector(0,0), Predicate(1)), NOT(Collector(1,0)))
        IndexFilterTreeNode tree = IndexFilterTreeNode.and(
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.predicateLeaf(1)
            ),
            IndexFilterTreeNode.not(
                IndexFilterTreeNode.collectorLeaf(1, 0)
            )
        );
        byte[] bytes = tree.serialize();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // AND
        assertEquals(0x00, buf.get());
        assertEquals(2, buf.getShort());

        // OR child
        assertEquals(0x01, buf.get());
        assertEquals(2, buf.getShort());
        // Collector(0,0)
        assertEquals(0x03, buf.get());
        assertEquals(0, buf.getShort());
        assertEquals(0, buf.getShort());
        // Predicate(1)
        assertEquals(0x04, buf.get());
        assertEquals(1, buf.getShort());

        // NOT child
        assertEquals(0x02, buf.get());
        // Collector(1,0)
        assertEquals(0x03, buf.get());
        assertEquals(1, buf.getShort());
        assertEquals(0, buf.getShort());

        // Should have consumed all bytes
        assertFalse(buf.hasRemaining());
    }

    public void testComplexNestedBoolTreeCase9() {
        // NOT(AND(OR(Collector(0,0), Predicate(0)), Collector(0,1)))
        // After normalization:
        // OR(AND(NOT(Collector(0,0)), PredicateLeaf(0)), NOT(Collector(0,1)))
        // Wait — let's trace carefully:
        // NOT(AND(a, b)) → OR(NOT(a), NOT(b))
        //   a = OR(Collector(0,0), Predicate(0))
        //   b = Collector(0,1)
        // → OR(NOT(OR(Collector(0,0), Predicate(0))), NOT(Collector(0,1)))
        // NOT(OR(c, d)) → AND(NOT(c), NOT(d))
        //   c = Collector(0,0), d = Predicate(0)
        // → OR(AND(NOT(Collector(0,0)), Predicate(0)), NOT(Collector(0,1)))
        // (PredicateLeaf.negate() returns itself)

        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.or(
                    IndexFilterTreeNode.collectorLeaf(0, 0),
                    IndexFilterTreeNode.predicateLeaf(0)
                ),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();

        // Top level: OR
        assertInstanceOf(IndexFilterTreeNode.Or.class, normalized);
        IndexFilterTreeNode.Or topOr = (IndexFilterTreeNode.Or) normalized;
        assertEquals(2, topOr.children().size());

        // First child: AND(NOT(Collector(0,0)), Predicate(0))
        assertInstanceOf(IndexFilterTreeNode.And.class, topOr.children().get(0));
        IndexFilterTreeNode.And andChild = (IndexFilterTreeNode.And) topOr.children().get(0);
        assertEquals(2, andChild.children().size());
        assertInstanceOf(IndexFilterTreeNode.Not.class, andChild.children().get(0));
        assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class,
            ((IndexFilterTreeNode.Not) andChild.children().get(0)).child());
        assertInstanceOf(IndexFilterTreeNode.PredicateLeaf.class, andChild.children().get(1));

        // Second child: NOT(Collector(0,1))
        assertInstanceOf(IndexFilterTreeNode.Not.class, topOr.children().get(1));
        assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class,
            ((IndexFilterTreeNode.Not) topOr.children().get(1)).child());
    }

    public void testDeepNestedNotNormalization() {
        // NOT(NOT(NOT(Collector(0,0))))
        // Step 1: NOT(NOT(x)) where x = NOT(Collector(0,0))
        //   → normalize(NOT(Collector(0,0)))
        //   → Collector(0,0).negate()
        //   → NOT(Collector(0,0))
        IndexFilterTreeNode tree = IndexFilterTreeNode.not(
            IndexFilterTreeNode.not(
                IndexFilterTreeNode.not(
                    IndexFilterTreeNode.collectorLeaf(0, 0)
                )
            )
        );
        IndexFilterTreeNode normalized = tree.normalize();
        assertInstanceOf(IndexFilterTreeNode.Not.class, normalized);
        assertInstanceOf(IndexFilterTreeNode.CollectorLeaf.class,
            ((IndexFilterTreeNode.Not) normalized).child());
    }

    // ── IndexFilterTree wrapper ─────────────────────────────────────

    public void testIndexFilterTreeConstruction() {
        IndexFilterTreeNode root = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.predicateLeaf(0)
        );
        IndexFilterTree tree = new IndexFilterTree(root, 1);
        assertSame(root, tree.root());
        assertEquals(1, tree.collectorLeafCount());
    }

    public void testIndexFilterTreeNullRootThrows() {
        expectThrows(NullPointerException.class, () -> new IndexFilterTree(null, 0));
    }

    public void testIndexFilterTreeSerialize() {
        IndexFilterTreeNode root = IndexFilterTreeNode.collectorLeaf(0, 0);
        IndexFilterTree tree = new IndexFilterTree(root, 1);
        byte[] bytes = tree.serialize();
        // Should match the root's serialization
        assertArrayEquals(root.serialize(), bytes);
    }

    public void testIndexFilterTreeNormalize() {
        // NOT(AND(Collector(0,0), Collector(0,1))) → OR(NOT(Collector(0,0)), NOT(Collector(0,1)))
        IndexFilterTreeNode root = IndexFilterTreeNode.not(
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
        );
        IndexFilterTree tree = new IndexFilterTree(root, 2);
        IndexFilterTree normalized = tree.normalize();

        // collectorLeafCount preserved
        assertEquals(2, normalized.collectorLeafCount());

        // Root should be OR after normalization
        assertInstanceOf(IndexFilterTreeNode.Or.class, normalized.root());
    }

    public void testIndexFilterTreeCloseIsNoOp() throws Exception {
        IndexFilterTree tree = new IndexFilterTree(IndexFilterTreeNode.predicateLeaf(0), 0);
        tree.close(); // should not throw
    }

    // ── Helper ──────────────────────────────────────────────────────

    private static <T> void assertInstanceOf(Class<T> expected, Object actual) {
        assertNotNull("Expected instance of " + expected.getSimpleName() + " but got null", actual);
        assertTrue(
            "Expected instance of " + expected.getSimpleName() + " but got " + actual.getClass().getSimpleName(),
            expected.isInstance(actual)
        );
    }
}
