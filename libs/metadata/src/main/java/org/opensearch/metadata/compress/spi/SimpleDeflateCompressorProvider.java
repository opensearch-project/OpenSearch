/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress.spi;

import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.spi.CompressorProvider;
import org.opensearch.metadata.compress.SimpleDeflateCompressor;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * SPI provider that registers {@link SimpleDeflateCompressor} as {@code "SIMPLE_DEFLATE"}.
 *
 * @opensearch.internal
 */
public class SimpleDeflateCompressorProvider implements CompressorProvider {

    /** Creates a new provider instance. */
    public SimpleDeflateCompressorProvider() {}

    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry<>(SimpleDeflateCompressor.NAME, new SimpleDeflateCompressor()));
    }
}
