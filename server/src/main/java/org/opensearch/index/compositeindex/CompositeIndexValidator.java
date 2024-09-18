/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeValidator;
import org.opensearch.index.mapper.MapperService;

/**
 * Validation for composite indices as part of mappings
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexValidator {

    public static void validate(MapperService mapperService, CompositeIndexSettings compositeIndexSettings, IndexSettings indexSettings) {
        StarTreeValidator.validate(mapperService, compositeIndexSettings, indexSettings);
    }

    public static void validate(
        MapperService mapperService,
        CompositeIndexSettings compositeIndexSettings,
        IndexSettings indexSettings,
        boolean isCompositeFieldPresent
    ) {
        if (!isCompositeFieldPresent && mapperService.isCompositeIndexPresent()) {
            throw new IllegalArgumentException(
                "Composite fields must be specified during index creation, addition of new composite fields during update is not supported"
            );
        }
        StarTreeValidator.validate(mapperService, compositeIndexSettings, indexSettings);
    }
}
