/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.xcontent.spi;

import org.opensearch.core.xcontent.MediaType;

import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface for plugins, modules, extensions providing
 * their own Media Types
 *
 * @opensearch.experimental
 * @opensearch.api
 */
public interface MediaTypeProvider {
    /** Extensions that implement their own concrete {@link MediaType}s provide them through this interface method */
    List<MediaType> getMediaTypes();

    /** Extensions that implement additional {@link MediaType} aliases provide them through this interface method */
    Map<String, MediaType> getAdditionalMediaTypes();
}
