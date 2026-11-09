/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.unit;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.test.OpenSearchTestCase;

public class MemorySizeValueTests extends OpenSearchTestCase {

    // --- parseBytesSizeValueOrHeapRatio tests (regression) ---

    public void testHeapRatioWithPercentage() {
        ByteSizeValue value = MemorySizeValue.parseBytesSizeValueOrHeapRatio("10%", "test.setting");
        long expected = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.10);
        assertEquals(expected, value.getBytes());
    }

    public void testHeapRatioWithAbsoluteValue() {
        ByteSizeValue value = MemorySizeValue.parseBytesSizeValueOrHeapRatio("512mb", "test.setting");
        assertEquals(512 * 1024 * 1024L, value.getBytes());
    }

    public void testHeapRatioRejectsInvalidPercentage() {
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrHeapRatio("101%", "test.setting"));
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrHeapRatio("-1%", "test.setting"));
    }

    public void testHeapRatioRejectsNonNumericPercentage() {
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrHeapRatio("abc%", "test.setting"));
    }

    // --- parseBytesSizeValueOrNativeMemoryRatio tests ---

    public void testNativeRatioWithAbsoluteValue() {
        ByteSizeValue value = MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("256mb", "test.setting");
        assertEquals(256 * 1024 * 1024L, value.getBytes());
    }

    public void testNativeRatioWithPercentageReturnsPositiveValue() {
        long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        if (totalPhysical <= 0 || totalPhysical <= jvmHeap) {
            // Cannot test percentage path on this system — skip
            return;
        }
        ByteSizeValue value = MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("5%", "test.setting");
        assertTrue("Expected positive bytes, got: " + value.getBytes(), value.getBytes() > 0);
        long expectedMax = (long) (0.05 * (totalPhysical - jvmHeap));
        assertEquals(expectedMax, value.getBytes());
    }

    public void testNativeRatioRejectsInvalidPercentage() {
        long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        if (totalPhysical <= 0 || totalPhysical <= jvmHeap) {
            // On systems where native memory can't be determined, even valid % will throw
            return;
        }
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("101%", "test.setting"));
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("-5%", "test.setting"));
    }

    public void testNativeRatioRejectsNonNumericPercentage() {
        long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        if (totalPhysical <= 0 || totalPhysical <= jvmHeap) {
            return;
        }
        expectThrows(OpenSearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("xyz%", "test.setting"));
    }

    public void testNativeRatioZeroPercentReturnsZero() {
        long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        if (totalPhysical <= 0 || totalPhysical <= jvmHeap) {
            return;
        }
        ByteSizeValue value = MemorySizeValue.parseBytesSizeValueOrNativeMemoryRatio("0%", "test.setting");
        assertEquals(0L, value.getBytes());
    }
}
