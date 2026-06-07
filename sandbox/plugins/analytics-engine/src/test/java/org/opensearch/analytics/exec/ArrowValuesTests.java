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
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/** Covers null, bigint pass-through, VarChar, and Date/Time/Timestamp formatting (scalar + list). */
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

    /** ISO-T timestamp strings (from DataFusion CAST) get the T replaced with a space. */
    public void testVarCharIsoTimestampGetsSpaceSeparator() {
        try (VarCharVector v = new VarCharVector("x", allocator)) {
            v.allocateNew();
            v.setSafe(0, "2019-03-24T01:34:46.123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            v.setValueCount(1);
            assertEquals("2019-03-24 01:34:46.123456789", ArrowValues.toJavaValue(v, 0));
        }
    }

    /** Non-temporal strings pass through unchanged. */
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
            // µs-typed time prints exactly 6 fractional digits — no zero-padding to nano width
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
            // µs-typed timestamp prints exactly 6 fractional digits — no zero-padding to nano width
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

    public void testTimestampMilliFractionPrintsThreeDigits() {
        // ms-typed timestamp with non-zero fraction prints exactly 3 digits, not nano-padded
        try (TimeStampMilliVector v = new TimeStampMilliVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_123L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05.123", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampNanoTrailingZerosTrimToMicroWidth() {
        // ns-typed timestamp with µs-aligned fraction (.123456000 ns) trims to 6-digit µs width
        try (TimeStampNanoVector v = new TimeStampNanoVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_123_456_000L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05.123456", ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampNanoTrailingZerosTrimToMilliWidth() {
        // ns-typed timestamp with ms-aligned fraction (.123000000 ns) trims to 3-digit ms width
        try (TimeStampNanoVector v = new TimeStampNanoVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_123_000_000L);
            v.setValueCount(1);
            assertEquals("2026-06-06 07:40:05.123", ArrowValues.toJavaValue(v, 0));
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
}
