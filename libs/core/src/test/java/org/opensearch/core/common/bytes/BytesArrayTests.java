/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.bytes;

import org.opensearch.test.OpenSearchTestCase;

public class BytesArrayTests extends OpenSearchTestCase {

    /**
     * Reproducer for opensearch-project/OpenSearch#22311 Bug A: an oversized UTF-16 string must be rejected by
     * {@link BytesArray#BytesArray(String)} instead of overflowing {@code UnicodeUtil#maxUTF8Length} inside
     * {@code BytesRef}.
     * <p>
     * The smallest string that reaches the guard is 715,827,883 chars, about 683 MiB as a Latin-1 compact string. The
     * guard throws before {@code BytesRef} allocates, so only the string itself has to fit in the test heap.
     */
    public void testStringConstructorRejectsOversizedUTF16Length() {
        int overflowingLength = (Integer.MAX_VALUE / 3) + 1;
        assumeTrue("needs ~683MiB of heap to build the oversized string", Runtime.getRuntime().maxMemory() >= 1_500_000_000L);

        String oversized = "a".repeat(overflowingLength);

        IllegalArgumentException guarded = expectThrows(IllegalArgumentException.class, () -> new BytesArray(oversized));

        assertTrue(guarded.getMessage().contains("UTF16 string length"));
        assertTrue(guarded.getMessage().contains(String.valueOf(overflowingLength)));
    }

    /**
     * The boundary is exclusive, so the largest encodable length must pass the guard. This asserts the guard directly
     * rather than through the constructor: at this length {@code BytesRef} would request a
     * {@code maxUTF8Length} array of 2,147,483,646 bytes, which exceeds the maximum array size the JVM allows at any
     * heap setting.
     */
    public void testMaxAllowedUTF16LengthPassesGuard() {
        int maxAllowedLength = Integer.MAX_VALUE / 3;
        BytesArray.ensureUTF16LengthIsValidForUTF8Encoding(maxAllowedLength);
    }
}
