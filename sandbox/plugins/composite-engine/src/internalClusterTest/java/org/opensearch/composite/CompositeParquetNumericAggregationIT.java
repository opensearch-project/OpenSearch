/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;

import java.util.Arrays;

/**
 * End-to-end coverage that a standard OpenSearch numeric aggregation over a Parquet-backed composite
 * index reads its values through the Lucene searcher path: the composite engine builds a searcher from
 * the lucene secondary, stamps each segment with its backing Parquet file, and the Parquet DocValues
 * codec (installed as a reader wrapper) serves the {@code value} field's doc values from Parquet.
 *
 * <p>The {@code value} integer field is Parquet-resident only (the lucene secondary holds the keyword
 * {@code name} and the row-id), so these aggregations exercise {@code ParquetNumericDocValues} rather
 * than any Lucene-native doc values.
 */
public class CompositeParquetNumericAggregationIT extends AbstractCompositeEngineIT {

    public void testNumericMetricAggregationsReadFromParquet() {
        String index = "agg-parquet";
        createCompositeIndex(index); // parquet primary + lucene secondary; value=integer
        int docs = 10;               // values 0..9
        indexDocs(index, docs, 0);
        refreshIndex(index);
        flushIndex(index);

        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("value"))
            .addAggregation(AggregationBuilders.count("cnt").field("value"))
            .addAggregation(AggregationBuilders.min("lo").field("value"))
            .addAggregation(AggregationBuilders.max("hi").field("value"))
            .addAggregation(AggregationBuilders.avg("mean").field("value"))
            .get();

        assertEquals("no shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());

        Sum total = response.getAggregations().get("total");
        ValueCount count = response.getAggregations().get("cnt");
        Min min = response.getAggregations().get("lo");
        Max max = response.getAggregations().get("hi");
        Avg mean = response.getAggregations().get("mean");

        assertEquals(45.0, total.getValue(), 0.0);   // 0 + 1 + ... + 9
        assertEquals(docs, count.getValue());
        assertEquals(0.0, min.getValue(), 0.0);
        assertEquals(9.0, max.getValue(), 0.0);
        assertEquals(4.5, mean.getValue(), 0.0);
    }
}
