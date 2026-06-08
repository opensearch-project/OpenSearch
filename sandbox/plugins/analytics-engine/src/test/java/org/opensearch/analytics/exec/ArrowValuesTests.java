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
import org.opensearch.test.OpenSearchTestCase;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/** Covers null, bigint pass-through, VarChar, and Date/Time/Timestamp normalization to java.time.* */
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

    public void testDateDayReturnsLocalDate() {
        try (DateDayVector v = new DateDayVector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 20610);
            v.setValueCount(1);
            assertEquals(LocalDate.of(2026, 6, 6), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testDateMilliReturnsLocalDate() {
        try (DateMilliVector v = new DateMilliVector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 20_610L * 86_400_000L);
            v.setValueCount(1);
            assertEquals(LocalDate.of(2026, 6, 6), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeNanoReturnsLocalTime() {
        try (TimeNanoVector v = new TimeNanoVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_000_000_000L);
            v.setValueCount(1);
            assertEquals(LocalTime.of(7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeMicroReturnsLocalTime() {
        try (TimeMicroVector v = new TimeMicroVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_000_000L);
            v.setValueCount(1);
            assertEquals(LocalTime.of(7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeMilliReturnsLocalTime() {
        try (TimeMilliVector v = new TimeMilliVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605_000);
            v.setValueCount(1);
            assertEquals(LocalTime.of(7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimeSecReturnsLocalTime() {
        try (TimeSecVector v = new TimeSecVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, 27_605);
            v.setValueCount(1);
            assertEquals(LocalTime.of(7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampMilliReturnsLocalDateTimeUtc() {
        try (TimeStampMilliVector v = new TimeStampMilliVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_000L);
            v.setValueCount(1);
            assertEquals(LocalDateTime.of(2026, 6, 6, 7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampSecondReturnsLocalDateTimeUtc() {
        try (TimeStampSecVector v = new TimeStampSecVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605L);
            v.setValueCount(1);
            assertEquals(LocalDateTime.of(2026, 6, 6, 7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampMicroReturnsLocalDateTimeUtc() {
        try (TimeStampMicroVector v = new TimeStampMicroVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_000_000L);
            v.setValueCount(1);
            assertEquals(LocalDateTime.of(2026, 6, 6, 7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testTimestampNanoReturnsLocalDateTimeUtc() {
        try (TimeStampNanoVector v = new TimeStampNanoVector("ts", allocator)) {
            v.allocateNew(1);
            v.set(0, 1_780_731_605_000_000_000L);
            v.setValueCount(1);
            assertEquals(LocalDateTime.of(2026, 6, 6, 7, 40, 5), ArrowValues.toJavaValue(v, 0));
        }
    }
}
