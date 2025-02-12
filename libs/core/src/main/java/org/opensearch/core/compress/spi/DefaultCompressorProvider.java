/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress.spi;

import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * Default {@link Compressor} implementations provided by the
 * opensearch core library
 *
 * @opensearch.internal
 */
public class DefaultCompressorProvider implements CompressorProvider {
    /** Returns the default {@link Compressor}s provided by the core library */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry(NoneCompressor.NAME, new NoneCompressor()));
    }
}
