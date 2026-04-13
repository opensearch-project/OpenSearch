/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class TermsBucketTranslatorTests extends OpenSearchTestCase {

    private final TermsBucketTranslator translator = new TermsBucketTranslator();
    private final ConversionContext ctx = TestUtils.createContext();
    private final TermsAggregationBuilder brandAgg = new TermsAggregationBuilder("by_brand").field("brand");

    public void testGetGrouping() {
        assertEquals(List.of("brand"), translator.getGrouping(brandAgg).getFieldNames());
    }

    public void testResolveGroupByIndices() throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(brandAgg);
        assertTrue(grouping instanceof FieldGrouping);
        List<Integer> indices = ((FieldGrouping) grouping).resolveIndices(ctx.getRowType());

        assertEquals(List.of(2), indices); // brand is index 2
    }

    public void testGetSubAggregations() {
        TermsAggregationBuilder aggWithSub = new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        assertEquals(1, translator.getSubAggregations(aggWithSub).size());
    }

    public void testEmptySubAggregations() {
        assertTrue(translator.getSubAggregations(brandAgg).isEmpty());
    }

    public void testReportsCorrectType() {
        assertEquals(TermsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testThrowsForUnknownField() {
        TermsAggregationBuilder badAgg = new TermsAggregationBuilder("by_bad").field("nonexistent");

        GroupingInfo grouping = translator.getGrouping(badAgg);
        assertTrue(grouping instanceof FieldGrouping);
        expectThrows(ConversionException.class, () -> ((FieldGrouping) grouping).resolveIndices(ctx.getRowType()));
    }

    public void testToBucketAggregationNotYetImplemented() {
        expectThrows(UnsupportedOperationException.class, () -> translator.toBucketAggregation(brandAgg, List.of()));
    }
}
