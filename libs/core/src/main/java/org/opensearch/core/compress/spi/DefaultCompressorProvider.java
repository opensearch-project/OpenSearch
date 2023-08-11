/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress.spi;

import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.compress.NoneCompressor;

import java.util.List;

/**
 * Default {@link Compressor} implementations provided by the
 * opensearch core library
 *
 * @opensearch.internal
 */
public class DefaultCompressorProvider implements CompressorProvider {
    /** Returns the default {@link Compressor}s provided by the core library */
    @Override
    public List<CompressorRegistry.Entry> getCompressors() {
        return List.of(new CompressorRegistry.Entry(NoneCompressor.NAME, new NoneCompressor()));
    }
}
