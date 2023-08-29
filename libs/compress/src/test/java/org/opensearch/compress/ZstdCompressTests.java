/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.compress;

import org.opensearch.core.compress.Compressor;
import org.opensearch.test.core.compress.AbstractCompressorTestCase;

/**
 * Test streaming compression
 */
public class ZstdCompressTests extends AbstractCompressorTestCase {

    private final Compressor compressor = new ZstdCompressor();

    @Override
    protected Compressor compressor() {
        return compressor;
    }
}
