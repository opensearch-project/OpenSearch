/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ExperimentalApi
public interface StarTreeFilterProvider {

    public StarTreeFilter getFilter(QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType);

    class SingletonFactory {

        private static final Map<Class<? extends QueryBuilder>, StarTreeFilterProvider> QUERY_BUILDERS_TO_STF_PROVIDER = Map.of(
            TermQueryBuilder.class,
            (rawFilter, compositeFieldType) -> {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) rawFilter;
                String field = termQueryBuilder.fieldName();
                List<Dimension> matchedDimension = compositeFieldType.getDimensions()
                    .stream()
                    .filter(dim -> dim.getField().equals(field))
                    .collect(Collectors.toList());
                // FIXME : DocValuesType validation is field type specific and not query builder specific should happen elsewhere.
                if (matchedDimension.size() != 1 || matchedDimension.get(0).getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
                    return null;
                }
                return new StarTreeFilter(Map.of(field, List.of(new ExactMatchDimFilter(field, List.of(termQueryBuilder.value())))));
            }
        );

        public static StarTreeFilterProvider getProvider(QueryBuilder queryBuilder) {
            if (queryBuilder != null) {
                return QUERY_BUILDERS_TO_STF_PROVIDER.get(queryBuilder.getClass());
            } else {
                return null;
            }
        }

    }

}
