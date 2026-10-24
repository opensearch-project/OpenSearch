/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;

/**
 * A validator that is called after mappings have been merged, during index creation and on every
 * mapping update, allowing plugins to validate the combination of index settings and mappings.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexCreationValidator {
    /**
     * Validates the index settings against the merged mappings.
     * Throw {@link IllegalArgumentException} to reject the index creation or mapping update.
     *
     * @param mapperService the mapper service with merged mappings
     * @param indexSettings the index settings
     */
    void validate(MapperService mapperService, IndexSettings indexSettings);
}
