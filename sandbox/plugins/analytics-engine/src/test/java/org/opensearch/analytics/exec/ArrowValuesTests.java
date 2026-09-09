/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Null, BigInt pass-through, VarChar, Date/Time/Timestamp formatting (scalar + list). */
public class ArrowValuesTests extends OpenSearchTestCase {

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

    public void testNullCellReturnsNull() {
        try (BigIntVector v = new BigIntVector("x", allocator)) {
            v.allocateNew(1);
            v.setNull(0);
            v.setValueCount(1);
            assertNull(ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testBigIntPassThroughUnchanged() {
        try (BigIntVector v = new BigIntVector("x", allocator)) {
            v.allocateNew(1);
            v.set(0, 42L);
            v.setValueCount(1);
            assertEquals(42L, ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testVarCharDecodedAsString() {
        try (VarCharVector v = new VarCharVector("x", allocator)) {
            v.allocateNew();
            v.setSafe(0, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            v.setValueCount(1);
            assertEquals("hello", ArrowValues.toJavaValue(v, 0));
        }
    }

    /** DataFusion CAST(timestamp AS VARCHAR) emits ISO-T — swap to space. */
    public void testVarCharIsoTimestampGetsSpaceSeparator() {
        try (VarCharVector v = new VarCharVector("x", allocator)) {
            v.allocateNew();
            v.setSafe(0, "2019-03-24T01:34:46.123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            v.setValueCount(1);
            assertEquals("2019-03-24 01:34:46.123456789", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testVarCharNonTemporalUnchanged() {
        try (VarCharVector v = new VarCharVector("x", allocator)) {
            v.allocateNew();
            v.setSafe(0, "hello T world".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            v.setValueCount(1);
            assertEquals("hello T world", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testDateDayFormatsIsoDate() {
        try (DateDayVector v = new DateDayVector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 20610);
            v.setValueCount(1);
            assertEquals("2026-06-06", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testDateMilliFormatsIsoDate() {
        try (DateMilliVector v = new DateMilliVector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 20_610L * 86_400_000L);
            v.setValueCount(1);
            assertEquals("2026-06-06", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeNanoNoEpochPrefix() {
        try (TimeNanoVector v = new TimeNanoVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_000_000_000L);
            v.setValueCount(1);
            assertEquals("07:40:05", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeMicroPreservesMicroseconds() {
        try (TimeMicroVector v = new TimeMicroVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_123_456L);
            v.setValueCount(1);
            assertEquals("07:40:05.123456", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeMilliFormat() {
        try (TimeMilliVector v = new TimeMilliVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_000);
            v.setValueCount(1);
            assertEquals("07:40:05", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeSecFormat() {
        try (TimeSecVector v = new TimeSecVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605);
            v.setValueCount(1);
            assertEquals("07:40:05", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampMilliSpaceSeparator() {
        try (TimeStampMilliVector v = new TimeStampMilliVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_000L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampSecondSpaceSeparator() {
        try (TimeStampSecVector v = new TimeStampSecVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampMicroPreservesMicroseconds() {
        try (TimeStampMicroVector v = new TimeStampMicroVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_123_456L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05.123456", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampNanoPreservesNanoseconds() {
        try (TimeStampNanoVector v = new TimeStampNanoVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_123_456_789L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05.123456789", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testListOfTimestampElementsFormatWithSpace() {
        Field child = new Field("item", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);
        Field list = new Field("ts_list", FieldType.nullable(new ArrowType.List()), List.of(child));
        try (ListVector v = (ListVector) list.createVector(allocator)) {
            UnionListWriter w = v.getWriter();
            w.startList();
            w.timeStampMicro().writeTimeStampMicro(1_780_731_605_000_000L);
            w.endList();
            w.setValueCount(1);
            Object cell = ArrowValues.toJavaValue(v, 0);
            assertEquals(List.of("2026-06-06 07:40:05"), cell);
        }
    }

    public void testListOfTimeElementsHasNoEpochPrefix() {
        Field child = new Field("item", FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), null);
        Field list = new Field("t_list", FieldType.nullable(new ArrowType.List()), List.of(child));
        try (ListVector v = (ListVector) list.createVector(allocator)) {
            UnionListWriter w = v.getWriter();
            w.startList();
            w.timeMicro().writeTimeMicro(27_605_000_000L);
            w.endList();
            w.setValueCount(1);
            Object cell = ArrowValues.toJavaValue(v, 0);
            assertEquals(List.of("07:40:05"), cell);
        }
    }

    // ---- toSourceValue / toSourceMap (moved from GetService.NativeBridgeExecutor) ----

    public void testToSourceValueNullReturnsNull() {
        try (BigIntVector v = new BigIntVector("x", allocator)) {
            v.allocateNew(1);
            v.setNull(0);
            v.setValueCount(1);
            assertNull(ArrowValues.toSourceValue(v, 0));
        }
    }

    public void testToSourceValueBinaryDropped() {
        try (VarBinaryVector v = new VarBinaryVector("b", allocator)) {
            v.allocateNew();
            v.setSafe(0, new byte[] { 1, 2, 3 });
            v.setValueCount(1);
            assertNull(ArrowValues.toSourceValue(v, 0));
        }
    }

    public void testToSourceValueUtf8AsString() {
        try (VarCharVector v = new VarCharVector("s", allocator)) {
            v.allocateNew();
            v.setSafe(0, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            v.setValueCount(1);
            assertEquals("hello", ArrowValues.toSourceValue(v, 0));
        }
    }

    public void testToSourceValueIntAsLong() {
        try (IntVector v = new IntVector("n", allocator)) {
            v.allocateNew(1);
            v.set(0, 42);
            v.setValueCount(1);
            assertEquals(42L, ArrowValues.toSourceValue(v, 0));
        }
    }

    public void testToSourceValueFloatingPointAsDouble() {
        try (Float8Vector v = new Float8Vector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 3.5d);
            v.setValueCount(1);
            assertEquals(3.5d, (double) ArrowValues.toSourceValue(v, 0), 0.0d);
        }
    }

    public void testToSourceValueBoolAsBoolean() {
        try (BitVector v = new BitVector("flag", allocator)) {
            v.allocateNew(1);
            v.set(0, 1);
            v.setValueCount(1);
            assertEquals(Boolean.TRUE, ArrowValues.toSourceValue(v, 0));
        }
    }

    /** Standard timestamp vectors decode to LocalDateTime; toSourceValue renders that via toString() (ISO-8601, 'T' separator, no 'Z'). */
    public void testToSourceValueTimestampLocalDateTimeToString() {
        try (TimeStampMilliVector v = new TimeStampMilliVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_000L);
            v.setValueCount(1);
            assertEquals("2026-06-06T07:40:05", ArrowValues.toSourceValue(v, 0));
        }
    }

    /** When a Timestamp cell decodes to a raw Number, toSourceValue formats it as ISO-8601 UTC (with 'Z') across all units. */
    public void testToSourceValueTimestampNumberRendersIsoUtc() {
        assertEquals("2026-06-06T07:40:05Z", numericTimestampSource(1_780_731_605L, TimeUnit.SECOND));
        assertEquals("2026-06-06T07:40:05.123Z", numericTimestampSource(1_780_731_605_123L, TimeUnit.MILLISECOND));
        assertEquals("2026-06-06T07:40:05.123456Z", numericTimestampSource(1_780_731_605_123_456L, TimeUnit.MICROSECOND));
        assertEquals("2026-06-06T07:40:05.123456789Z", numericTimestampSource(1_780_731_605_123_456_789L, TimeUnit.NANOSECOND));
    }

    /** Builds a Timestamp-typed vector whose cell decodes to a raw Long, exercising the numeric ISO branch + toInstant. */
    private static Object numericTimestampSource(long raw, TimeUnit unit) {
        FieldVector vec = mock(FieldVector.class);
        when(vec.isNull(0)).thenReturn(false);
        when(vec.getField()).thenReturn(Field.nullable("ts", new ArrowType.Timestamp(unit, null)));
        when(vec.getObject(0)).thenReturn(raw);
        return ArrowValues.toSourceValue(vec, 0);
    }

    public void testToSourceMapDropsNullsAndPreservesFieldOrder() {
        VarCharVector name = new VarCharVector("name", allocator);
        IntVector age = new IntVector("age", allocator);
        VarCharVector missing = new VarCharVector("missing", allocator);
        name.allocateNew();
        name.setSafe(0, "alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        name.setValueCount(1);
        age.allocateNew(1);
        age.set(0, 30);
        age.setValueCount(1);
        missing.allocateNew();
        missing.setNull(0);
        missing.setValueCount(1);
        // VectorSchemaRoot.of takes ownership; closing root closes the child vectors.
        try (VectorSchemaRoot root = VectorSchemaRoot.of(name, age, missing)) {
            Map<String, Object> out = ArrowValues.toSourceMap(root, 0);
            assertEquals(2, out.size());
            assertEquals("alice", out.get("name"));
            assertEquals(30L, out.get("age"));
            assertFalse(out.containsKey("missing"));
        }
    }
}
