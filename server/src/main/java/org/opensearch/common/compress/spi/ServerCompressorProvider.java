/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress.spi;

import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.spi.CompressorProvider;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * Default {@link Compressor} implementations provided by the
 * opensearch core library
 *
 * @opensearch.internal
 *
 * @deprecated This class is deprecated and will be removed when the {@link DeflateCompressor} is moved to the compress
 * library as a default compression option
 */
@Deprecated
public class ServerCompressorProvider implements CompressorProvider {
    /** Returns the concrete {@link Compressor}s provided by the server module */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry(DeflateCompressor.NAME, new DeflateCompressor()));
    }
}
