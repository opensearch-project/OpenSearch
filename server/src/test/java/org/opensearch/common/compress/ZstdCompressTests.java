/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

/**
 * Test streaming compression
 */
public class ZstdCompressTests extends AbstractCompressorTests {

    private final Compressor compressor = new ZstdCompressor();

    @Override
    Compressor compressor() {
        return compressor;
    }
}
