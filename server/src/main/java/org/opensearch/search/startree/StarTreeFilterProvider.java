/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface StarTreeFilterProvider<T> {

    public StarTreeFilter getFilter(T rawFilter, CompositeDataCubeFieldType compositeFieldType);

    static enum Registry {

        TERM_FILTER_PROVIDER((StarTreeFilterProvider<TermQueryBuilder>) (rawFilter, compositeFieldType) -> {
            String field = rawFilter.fieldName();
            List<Dimension> matchedDimension = compositeFieldType.getDimensions().stream().filter(dim -> dim.getField().equals(field)).collect(Collectors.toList());
            // FIXME : DocValuesType validation is field type specific and not query builder specific should happen elsewhere.
            if (matchedDimension.size() != 1 || matchedDimension.get(0).getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
                return null;
            }
            return new StarTreeFilter(Map.of(field, List.of(new ExactMatchDimFilter(field, List.of(rawFilter.value())))));
        });

        private final StarTreeFilterProvider<?> starTreeFilterProvider;

        Registry(StarTreeFilterProvider<?> starTreeFilterProvider) {
            this.starTreeFilterProvider = starTreeFilterProvider;
        }

        public StarTreeFilterProvider<?> getStarTreeFilterProvider() {
            return starTreeFilterProvider;
        }

    }

}
