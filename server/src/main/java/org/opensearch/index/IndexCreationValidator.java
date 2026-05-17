/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.mapper.MapperService;

/**
 * A validator that is called during index creation after mappings have been merged,
 * allowing plugins to validate the combination of index settings and mappings.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface IndexCreationValidator {
    /**
     * Validates the index settings against the merged mappings.
     * Throw {@link IllegalArgumentException} to reject index creation.
     *
     * @param mapperService the mapper service with merged mappings
     * @param indexSettings the index settings
     */
    void validate(MapperService mapperService, org.opensearch.index.IndexSettings indexSettings);
}
