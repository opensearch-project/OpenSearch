/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.core;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Objects;

/**
 * Representation of a Query Shape primarily used to group Top N queries by latency and resource usage.
 * https://github.com/opensearch-project/OpenSearch/issues/13357
 * @opensearch.internal
 */
public class QueryShape {
    private QueryBuilderFullShape queryBuilderFullShape;
    private SortShape sortShape;
    private AggregationFullShape aggregationFullShape;
    private PipelineAggregationShape pipelineAggregationShape;

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilderFullShape, sortShape, aggregationFullShape, pipelineAggregationShape);
    }

    public void parse(SearchSourceBuilder source) {
        // Parse the QueryBuilder to QueryShape
        // Populate QueryBuilderFullShape, SortShape, AggregationFullShape, PipelineAggregationShape
    }

    public String getStringQueryShape() {
        // Provide the String Query Shape
        return null;
    }
}
