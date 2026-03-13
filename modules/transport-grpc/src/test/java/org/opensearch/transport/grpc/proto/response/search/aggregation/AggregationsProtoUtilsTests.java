/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.SearchResponse;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AggregationsProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithMultipleAggregations() throws IOException {
        InternalMin minAgg = new InternalMin("min_price", 10.0, DocValueFormat.RAW, Collections.emptyMap());
        InternalMax maxAgg = new InternalMax("max_price", 100.0, DocValueFormat.RAW, Collections.emptyMap());

        List<InternalAggregation> aggList = new ArrayList<>();
        aggList.add(minAgg);
        aggList.add(maxAgg);
        Aggregations aggregations = InternalAggregations.from(aggList);

        SearchResponse.Builder builder = SearchResponse.newBuilder();
        AggregationsProtoUtils.toProto(aggregations, builder);

        assertEquals("Should have 2 aggregations", 2, builder.getAggregationsCount());
        assertTrue("Should have min_price", builder.containsAggregations("min_price"));
        assertTrue("Should have max_price", builder.containsAggregations("max_price"));
    }

    public void testToProtoWithSingleAggregation() throws IOException {
        InternalMin minAgg = new InternalMin("single_min", 42.0, DocValueFormat.RAW, Collections.emptyMap());
        Aggregations aggregations = InternalAggregations.from(Collections.singletonList(minAgg));

        SearchResponse.Builder builder = SearchResponse.newBuilder();
        AggregationsProtoUtils.toProto(aggregations, builder);

        assertEquals("Should have 1 aggregation", 1, builder.getAggregationsCount());
        assertTrue("Should have single_min", builder.containsAggregations("single_min"));
    }

    public void testToProtoWithEmptyAggregations() throws IOException {
        Aggregations aggregations = InternalAggregations.EMPTY;

        SearchResponse.Builder builder = SearchResponse.newBuilder();
        AggregationsProtoUtils.toProto(aggregations, builder);

        assertEquals("Should have 0 aggregations", 0, builder.getAggregationsCount());
    }

    public void testToProtoWithNullAggregations() throws IOException {
        SearchResponse.Builder builder = SearchResponse.newBuilder();
        AggregationsProtoUtils.toProto(null, builder);

        assertEquals("Should have 0 aggregations", 0, builder.getAggregationsCount());
    }
}
