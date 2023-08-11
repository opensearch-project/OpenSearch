/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress.spi;

import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.compress.spi.CompressorProvider;

import java.util.List;

/**
 * Default {@link org.opensearch.core.common.compress.Compressor} implementations provided by the
 * opensearch core library
 *
 * @opensearch.internal
 *
 * @Deprecated This class is deprecated and will be removed when the {@link DeflateCompressor} is moved to the compress
 * library as a default compression option
 */
@Deprecated
public class ServerCompressorProvider implements CompressorProvider {
    /** Returns the concrete {@link org.opensearch.core.common.compress.Compressor}s provided by the server module */
    @Override
    public List<CompressorRegistry.Entry> getCompressors() {
        return List.of(new CompressorRegistry.Entry(DeflateCompressor.NAME, new DeflateCompressor()));
    }
}
