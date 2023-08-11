/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress.spi;

import org.opensearch.core.common.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;

import java.util.List;

/**
 * Service Provider Interface for plugins, modules, extensions providing custom
 * compression algorithms
 *
 * see {@link Compressor} for implementing methods
 * and {@link org.opensearch.core.compress.CompressorRegistry} for the registration of custom
 * Compressors
 *
 * @opensearch.experimental
 * @opensearch.api
 */
public interface CompressorProvider {
    /** Extensions that implement their own concrete {@link Compressor}s provide them through this interface method*/
    List<CompressorRegistry.Entry> getCompressors();
}
