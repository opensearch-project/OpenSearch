/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;

public class Lucene95CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene95CustomCodecMode() {
        Lucene95CustomStoredFieldsFormat lucene95CustomStoredFieldsFormat = new Lucene95CustomStoredFieldsFormat();
        assertEquals(Lucene95CustomCodec.Mode.ZSTD, lucene95CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene95CustomCodecMode() {
        Lucene95CustomStoredFieldsFormat lucene95CustomStoredFieldsFormat = new Lucene95CustomStoredFieldsFormat(
            Lucene95CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene95CustomCodec.Mode.ZSTD_NO_DICT, lucene95CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = generateRandomNumber(6, 1);
        Lucene95CustomStoredFieldsFormat lucene95CustomStoredFieldsFormat = new Lucene95CustomStoredFieldsFormat(
            Lucene95CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene95CustomCodec.Mode.ZSTD, lucene95CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene95CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene95CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = generateRandomNumber(6, 1);
        Lucene95CustomStoredFieldsFormat lucene95CustomStoredFieldsFormat = new Lucene95CustomStoredFieldsFormat(
            Lucene95CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene95CustomCodec.Mode.ZSTD_NO_DICT, lucene95CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene95CustomStoredFieldsFormat.getCompressionLevel());
    }

    private int generateRandomNumber(int max, int min) {
        return Randomness.get().nextInt(max - min + 1) + min;
    }

}
