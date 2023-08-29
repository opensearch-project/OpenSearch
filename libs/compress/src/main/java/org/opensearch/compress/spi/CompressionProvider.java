/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.compress.spi;

import org.opensearch.compress.ZstdCompressor;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.spi.CompressorProvider;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * Additional "optional" compressor implementations provided by the opensearch compress library
 *
 * @opensearch.internal
 */
public class CompressionProvider implements CompressorProvider {

    /** Returns the concrete {@link Compressor}s provided by the compress library */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry<>(ZstdCompressor.NAME, new ZstdCompressor()));
    }
}
