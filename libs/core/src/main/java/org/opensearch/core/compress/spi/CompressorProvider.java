/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.compress.Compressor;

import java.util.List;
import java.util.Map;

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
@ExperimentalApi
@PublicApi(since = "2.10.0")
public interface CompressorProvider {
    /** Extensions that implement their own concrete {@link Compressor}s provide them through this interface method*/
    List<Map.Entry<String, Compressor>> getCompressors();
}
