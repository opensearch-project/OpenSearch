/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.th.ThaiTokenizer;

/**
 * {@link ThaiTokenizer} as it behaved before Lucene 10.6, for indices created before {@link org.opensearch.Version#V_3_10_0}.
 *
 * <p>Lucene 10.6 also treats whitespace as a safe place to cut when the internal buffer is full. That can move the token
 * boundaries of long text, so this tokenizer only accepts the line and paragraph separators it accepted before.
 */
final class LegacyThaiTokenizer extends ThaiTokenizer {

    LegacyThaiTokenizer() {
        super();
    }

    @Override
    protected boolean isSafeEnd(char ch) {
        switch (ch) {
            case 0x000D:
            case 0x000A:
            case 0x0085:
            case 0x2028:
            case 0x2029:
                return true;
            default:
                return false;
        }
    }
}
