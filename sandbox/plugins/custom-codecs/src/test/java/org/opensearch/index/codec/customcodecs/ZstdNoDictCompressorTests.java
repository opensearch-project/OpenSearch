/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;

/**
 * Test ZSTD compression (with no dictionary).
 */
public class ZstdNoDictCompressorTests extends AbstractCompressorTests {

    private final Compressor compressor = new ZstdNoDictCompressionMode().newCompressor();
    private final Decompressor decompressor = new ZstdNoDictCompressionMode().newDecompressor();

    @Override
    Compressor compressor() {
        return compressor;
    }

    @Override
    Decompressor decompressor() {
        return decompressor;
    }
}
