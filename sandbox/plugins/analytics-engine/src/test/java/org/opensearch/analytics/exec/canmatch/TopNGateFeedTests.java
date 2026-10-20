/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@link TopNGate#feed} — reading a shard stream's Arrow batches into the heap, and the data-only
 * checks that retire the gate. Separate from {@link TopNGateTests}, which drives {@code offer} directly.
 */
public class TopNGateFeedTests extends OpenSearchTestCase {

    private static final String COLUMN = "@timestamp";

    private static final ArrowType MILLIS = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    private static final ArrowType NANOS = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType INT32 = new ArrowType.Int(32, true);

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    // ── the happy path ───────────────────────────────────────────────────

    /** Every row of every batch reaches the gate, so the bar is the K-th best across all of them. */
    public void testFeedsEveryRowAcrossBatches() {
        TopNGate gate = new TopNGate(3, true, COLUMN);

        try (VectorSchemaRoot first = batch(MILLIS, 100L, 98L); VectorSchemaRoot second = batch(MILLIS, 95L, 12L)) {
            gate.feed(first);
            assertFalse("two keys is not yet K", gate.isArmed());
            gate.feed(second);
        }

        assertTrue("all four keys observed, so the heap is full", gate.isArmed());
        assertEquals("bar is the 3rd-best of {100,98,95,12}", 95L, gate.bottom());
    }

    /** An empty batch is not a schema to resolve against — it must not disable the gate. */
    public void testEmptyBatchIsSkippedWithoutResolvingTheColumn() {
        TopNGate gate = new TopNGate(1, true, COLUMN);

        try (VectorSchemaRoot empty = batch(MILLIS); VectorSchemaRoot real = batch(MILLIS, 7L)) {
            gate.feed(empty);
            assertFalse("nothing observed yet", gate.isArmed());
            assertFalse("an empty batch is not a disagreement", gate.isDisabled());
            gate.feed(real);
        }

        assertTrue(gate.isArmed());
        assertEquals(7L, gate.bottom());
    }

    /** Every supported type is read as a raw long: both timestamp units and {@code Int64}/{@code Int32}. */
    public void testReadsEverySupportedVectorType() {
        for (ArrowType type : List.of(MILLIS, NANOS, INT64, INT32)) {
            TopNGate gate = new TopNGate(1, true, COLUMN);
            try (VectorSchemaRoot root = batch(type, 5L)) {
                gate.feed(root);
            }
            assertFalse(type + " must be readable", gate.isDisabled());
            assertTrue(type + " must arm the gate", gate.isArmed());
            assertEquals(type + " read the raw value", 5L, gate.bottom());
        }
    }

    // ── the checks that protect correctness ──────────────────────────────

    /** The sort column is projected away: with nothing to observe, retire the gate rather than arm on partial data. */
    public void testAbsentColumnDisablesTheGate() {
        TopNGate gate = new TopNGate(1, true, COLUMN);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema("something_else", MILLIS), allocator)) {
            TimeStampMilliVector vector = (TimeStampMilliVector) root.getVector(0);
            vector.setSafe(0, 42L);
            vector.setValueCount(1);
            root.setRowCount(1);
            gate.feed(root);
        }

        assertTrue("a missing sort column disables the gate", gate.isDisabled());
        assertFalse("and nothing was observed", gate.isArmed());
    }

    /**
     * A type {@code readInto} can't read contributes nothing, so the gate stays unarmed and eliminates
     * nothing — fail-open, without retiring.
     */
    public void testUnreadableVectorTypeIsSilentlyIgnored() {
        TopNGate gate = new TopNGate(1, true, COLUMN);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema(COLUMN, new ArrowType.Utf8()), allocator)) {
            VarCharVector strings = (VarCharVector) root.getVector(0);
            strings.setSafe(0, "not a timestamp".getBytes(StandardCharsets.UTF_8));
            strings.setValueCount(1);
            root.setRowCount(1);
            gate.feed(root);
        }

        assertFalse("an unreadable type is not a reason to retire the gate", gate.isDisabled());
        assertFalse("but nothing was observed, so the gate cannot arm", gate.isArmed());
    }

    /**
     * A null in the data contradicts the bounds' no-nulls claim, which elimination relies on
     * ({@code DESC} is {@code NULLS FIRST}). That retires the gate for good — no later batch undoes it.
     */
    public void testNullsInTheDataDisableTheGateForGood() {
        TopNGate gate = new TopNGate(1, true, COLUMN);

        try (VectorSchemaRoot withNull = millisBatchWithNull(); VectorSchemaRoot clean = batch(MILLIS, 2L)) {
            gate.feed(withNull);
            assertTrue("nulls the bounds did not report disable the gate", gate.isDisabled());
            assertFalse("and no key from that batch was admitted", gate.isArmed());

            gate.feed(clean);
            assertTrue("still disabled", gate.isDisabled());
            assertFalse("feed() is a no-op once retired, so nothing armed", gate.isArmed());
        }
    }

    /** A gate disabled by one stream's batch stops reading for every other stream too. */
    public void testDisabledGateStopsReading() {
        TopNGate gate = new TopNGate(1, true, COLUMN);
        gate.disable();

        try (VectorSchemaRoot root = batch(MILLIS, 5L)) {
            gate.feed(root);
        }

        assertFalse("a disabled gate admits nothing", gate.isArmed());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Schema schema(String column, ArrowType type) {
        return new Schema(List.of(new Field(column, FieldType.nullable(type), null)));
    }

    /** A single-column {@code @timestamp} batch of {@code type}. */
    private VectorSchemaRoot batch(ArrowType type, long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema(COLUMN, type), allocator);
        FieldVector vector = root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            setLong(vector, i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /** Arrow has no shared long-valued setter, so dispatch on the concrete class as {@code readInto} does. */
    private static void setLong(FieldVector vector, int index, long value) {
        if (vector instanceof TimeStampMilliVector millis) {
            millis.setSafe(index, value);
        } else if (vector instanceof TimeStampNanoVector nanos) {
            nanos.setSafe(index, value);
        } else if (vector instanceof BigIntVector longs) {
            longs.setSafe(index, value);
        } else if (vector instanceof IntVector ints) {
            ints.setSafe(index, (int) value);
        } else {
            throw new AssertionError("no setter wired for " + vector.getClass().getSimpleName());
        }
    }

    /** One real key and one null — the shape that retires the gate. */
    private VectorSchemaRoot millisBatchWithNull() {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema(COLUMN, MILLIS), allocator);
        TimeStampMilliVector vector = (TimeStampMilliVector) root.getVector(0);
        vector.setSafe(0, 1L);
        vector.setNull(1);
        vector.setValueCount(2);
        root.setRowCount(2);
        return root;
    }
}
