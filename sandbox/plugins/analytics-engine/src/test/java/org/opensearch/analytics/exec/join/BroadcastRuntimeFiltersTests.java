/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.core.JoinRelType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class BroadcastRuntimeFiltersTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    // ── The correctness guard ────────────────────────────────────────────

    /**
     * Filtering the probe side is sound only when a probe row with no match cannot reach the output. The
     * expectations below are the same permission set Spark encodes as {@code canPruneLeft} (Inner,
     * LeftSemi, RightOuter) and {@code canPruneRight} (Inner, LeftSemi, LeftOuter), derived here from
     * Calcite's null-generation predicates instead of an enumeration.
     */
    public void testProbeSideFilteringIsAllowedExactlyWhereItCannotDropRows() {
        // Inner: neither side preserved, so either side may be filtered.
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.INNER, 0));
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.INNER, 1));

        // Semi: output is left rows that HAVE a match, so filtering left is sound.
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.SEMI, 0));
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.SEMI, 1));

        // Left outer preserves the LEFT input: filtering it would drop rows that belong in the result
        // null-extended. The right input is not preserved and may be filtered.
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.LEFT, 0));
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.LEFT, 1));

        // Right outer is the mirror image.
        assertTrue(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.RIGHT, 0));
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.RIGHT, 1));

        // Full preserves both.
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.FULL, 0));
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.FULL, 1));

        // Anti's output is exactly the probe rows that do NOT match, so filtering to the matching keys is
        // backwards. Neither side, regardless of null generation.
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.ANTI, 0));
        assertFalse(RuntimeFilterEligibility.canFilterProbeSide(JoinRelType.ANTI, 1));
    }

    /** No join type may be filtered on a side the join preserves — stated independently of the table above. */
    public void testAPreservedSideIsNeverFilterable() {
        for (JoinRelType type : JoinRelType.values()) {
            if (type.generatesNullsOnRight()) {
                assertFalse("input 0 is preserved under " + type, RuntimeFilterEligibility.canFilterProbeSide(type, 0));
            }
            if (type.generatesNullsOnLeft()) {
                assertFalse("input 1 is preserved under " + type, RuntimeFilterEligibility.canFilterProbeSide(type, 1));
            }
        }
    }

    // ── Key-set extraction ──────────────────────────────────────────────

    public void testExtractsDistinctValuesAcrossBatches() throws Exception {
        byte[] ipc = bigIntStream(new Long[] { 5L, 3L, 5L }, new Long[] { 3L, 9L });
        assertArrayEquals(new long[] { 3, 5, 9 }, sortedValues(ipc, 100));
    }

    public void testSkipsNulls() throws Exception {
        // A null build key matches nothing under equi-join semantics, so it contributes no candidate —
        // and it cannot widen the filter either, since a null probe key matches nothing either.
        assertArrayEquals(new long[] { 7, 8 }, sortedValues(bigIntStream(new Long[] { 7L, null, 8L }), 100));
    }

    public void testAbortsOverTheCap() throws Exception {
        byte[] ipc = bigIntStream(new Long[] { 1L, 2L, 3L, 4L });
        assertNull(
            "4 distinct values over a cap of 3 must yield no filter",
            BroadcastRuntimeFilters.distinctIntegralValues(ipc, 0, allocator, 3)
        );
        assertNotNull("at the cap it is still produced", BroadcastRuntimeFilters.distinctIntegralValues(ipc, 0, allocator, 4));
    }

    public void testEmptyBuildSideYieldsNoFilter() throws Exception {
        // Deliberately not "prune everything": an empty build side is a whole-query short circuit and
        // deserves to be an explicit decision, not an emergent one. It also looks identical to a failed
        // extraction.
        assertNull("no batches", BroadcastRuntimeFilters.distinctIntegralValues(bigIntStream(), 0, allocator, 100));
        assertNull("empty batch", BroadcastRuntimeFilters.distinctIntegralValues(bigIntStream(new Long[0]), 0, allocator, 100));
        assertNull(
            "all-null column has no candidates",
            BroadcastRuntimeFilters.distinctIntegralValues(bigIntStream(new Long[] { null, null }), 0, allocator, 100)
        );
    }

    public void testRejectsNonIntegralColumns() throws Exception {
        // Floating point and text do not compare as a plain long against parquet row-group statistics, so
        // a filter built from them could prune a shard that holds matching rows.
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(doubleStream(1.5, 2.5), 0, allocator, 100));
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(varCharStream("a", "b"), 0, allocator, 100));
    }

    public void testRejectsAnOrdinalOutsideTheSchema() throws Exception {
        byte[] ipc = bigIntStream(new Long[] { 1L });
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(ipc, 1, allocator, 100));
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(ipc, -1, allocator, 100));
    }

    public void testUnreadableBytesYieldNoFilter() {
        // Fail open: a corrupt capture must cost the optimization, not the query.
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(new byte[] { 1, 2, 3 }, 0, allocator, 100));
        assertNull(BroadcastRuntimeFilters.distinctIntegralValues(new byte[0], 0, allocator, 100));
    }

    // ── Fixtures ─────────────────────────────────────────────────────────

    /** Distinct values, sorted, so assertions do not depend on encounter order. */
    private long[] sortedValues(byte[] ipc, int maxValues) {
        long[] values = BroadcastRuntimeFilters.distinctIntegralValues(ipc, 0, allocator, maxValues);
        assertNotNull(values);
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted;
    }

    /** Arrow IPC stream with one nullable `int64` column named `k`, one record batch per argument. */
    private byte[] bigIntStream(Long[]... batches) throws Exception {
        Schema schema = new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector("k");
            return writeStream(root, batches.length, batchIdx -> {
                Long[] batch = batches[batchIdx];
                vector.allocateNew(batch.length);
                for (int i = 0; i < batch.length; i++) {
                    if (batch[i] == null) {
                        vector.setNull(i);
                    } else {
                        vector.setSafe(i, batch[i]);
                    }
                }
                return batch.length;
            });
        }
    }

    /** Single-batch stream with one nullable `float64` column named `k`. */
    private byte[] doubleStream(double... values) throws Exception {
        ArrowType type = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        Schema schema = new Schema(List.of(new Field("k", FieldType.nullable(type), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            Float8Vector vector = (Float8Vector) root.getVector("k");
            return writeStream(root, 1, batchIdx -> {
                vector.allocateNew(values.length);
                for (int i = 0; i < values.length; i++) {
                    vector.setSafe(i, values[i]);
                }
                return values.length;
            });
        }
    }

    /** Single-batch stream with one nullable `utf8` column named `k`. */
    private byte[] varCharStream(String... values) throws Exception {
        Schema schema = new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector vector = (VarCharVector) root.getVector("k");
            return writeStream(root, 1, batchIdx -> {
                vector.allocateNew(values.length);
                for (int i = 0; i < values.length; i++) {
                    vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
                }
                return values.length;
            });
        }
    }

    /** Writes {@code batchCount} record batches, asking {@code filler} to populate each and report its row count. */
    private byte[] writeStream(VectorSchemaRoot root, int batchCount, BatchFiller filler) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            for (int batchIdx = 0; batchIdx < batchCount; batchIdx++) {
                for (FieldVector v : root.getFieldVectors()) {
                    v.clear();
                }
                root.setRowCount(filler.fill(batchIdx));
                writer.writeBatch();
            }
            writer.end();
        }
        return out.toByteArray();
    }

    private interface BatchFiller {
        int fill(int batchIdx);
    }
}
