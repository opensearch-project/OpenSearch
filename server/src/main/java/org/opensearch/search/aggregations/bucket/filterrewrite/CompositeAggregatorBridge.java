/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.RoundingValuesSource;

/**
 * For composite aggregation to do optimization when it only has a single date histogram source
 */
public abstract class CompositeAggregatorBridge extends DateHistogramAggregatorBridge {
    protected boolean canOptimize(CompositeValuesSourceConfig[] sourceConfigs) {
        if (sourceConfigs.length != 1 || !(sourceConfigs[0].valuesSource() instanceof RoundingValuesSource)) return false;
        return canOptimize(sourceConfigs[0].missingBucket(), sourceConfigs[0].hasScript(), sourceConfigs[0].fieldType());
    }

    private boolean canOptimize(boolean missing, boolean hasScript, MappedFieldType fieldType) {
        if (!missing && !hasScript) {
            if (fieldType instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }
}
