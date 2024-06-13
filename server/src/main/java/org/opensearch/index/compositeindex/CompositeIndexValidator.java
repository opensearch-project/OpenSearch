/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.util.Locale;
import java.util.Set;

/**
 * Validation for composite indices
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexValidator {

    public static void validate(MapperService mapperService, CompositeIndexSettings compositeIndexSettings) {
        validateStarTreeMappings(mapperService, compositeIndexSettings);
    }

    private static void validateStarTreeMappings(MapperService mapperService, CompositeIndexSettings compositeIndexSettings) {
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        for (CompositeMappedFieldType compositeFieldType : compositeFieldTypes) {
            if (!(compositeFieldType instanceof StarTreeMapper.StarTreeFieldType)) {
                return;
            }
            if (!compositeIndexSettings.isStarTreeIndexCreationEnabled()) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "star tree index cannot be created, enable it using [%s] setting",
                        CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey()
                    )
                );
            }
            CompositeDataCubeFieldType dataCubeFieldType = (CompositeDataCubeFieldType) compositeFieldType;
            for (Dimension dim : dataCubeFieldType.getDimensions()) {
                MappedFieldType ft = mapperService.fieldType(dim.getField());
                if (!ft.isAggregatable()) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Aggregations not supported for the dimension field [%s] with field type [%s] as part of star tree field",
                            dim.getField(),
                            ft.typeName()
                        )
                    );
                }
            }
            for (Metric metric : dataCubeFieldType.getMetrics()) {
                MappedFieldType ft = mapperService.fieldType(metric.getField());
                if (!ft.isAggregatable()) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Aggregations not supported for the metrics field [%s] with field type [%s] as part of star tree field",
                            metric.getField(),
                            ft.typeName()
                        )
                    );
                }
            }
        }
    }
}
