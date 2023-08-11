/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.compress.spi;

import org.opensearch.compress.ZstdCompressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.compress.spi.CompressorProvider;

import java.util.List;

/**
 * Additional "optional" compressor implementations provided by the opensearch compress library
 *
 * @opensearch.internal
 */
public class CompressionProvider implements CompressorProvider {

    /** Returns the concrete {@link org.opensearch.core.common.compress.Compressor}s provided by the compress library */
    @Override
    public List<CompressorRegistry.Entry> getCompressors() {
        return List.of(new CompressorRegistry.Entry(ZstdCompressor.NAME, new ZstdCompressor()));
    }
}
