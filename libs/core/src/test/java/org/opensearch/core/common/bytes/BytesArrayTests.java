/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

public class BytesArrayTests extends OpenSearchTestCase {

    /**
     * Reproducer for opensearch-project/OpenSearch#22311 Bug A: overflow in {@code UnicodeUtil#maxUTF8Length} must be
     * caught before constructing a {@link BytesRef} from an oversized UTF-16 string.
     */
    public void testOverflowGuardPreventsBytesRefIntegerOverflow() {
        int overflowingLength = (Integer.MAX_VALUE / 3) + 1;
        CharSequence text = overflowingCharSequence(overflowingLength);

        IllegalArgumentException guarded = expectThrows(
            IllegalArgumentException.class,
            () -> BytesArray.ensureUTF16LengthIsValidForUTF8Encoding(text.length())
        );
        assertTrue(guarded.getMessage().contains("UTF16 string length"));
        assertTrue(guarded.getMessage().contains(String.valueOf(overflowingLength)));

        expectThrows(ArithmeticException.class, () -> new BytesRef(text));
    }

    public void testStringConstructorAcceptsMaxAllowedUTF16Length() {
        int maxAllowedLength = Integer.MAX_VALUE / 3;
        BytesArray.ensureUTF16LengthIsValidForUTF8Encoding(maxAllowedLength);
    }

    private static CharSequence overflowingCharSequence(int overflowingLength) {
        return new CharSequence() {
            @Override
            public int length() {
                return overflowingLength;
            }

            @Override
            public char charAt(int i) {
                throw new AssertionError("not reached");
            }

            @Override
            public CharSequence subSequence(int start, int end) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
