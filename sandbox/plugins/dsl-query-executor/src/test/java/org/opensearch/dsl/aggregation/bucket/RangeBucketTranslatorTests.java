/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.ExpressionGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.script.Script;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RangeBucketTranslatorTests extends OpenSearchTestCase {

    private final RangeBucketTranslator translator = new RangeBucketTranslator(() -> null);

    private static RangeAggregationBuilder priceRanges() {
        // [*,100), [100,200), [200,*) — contiguous, non-overlapping
        return new RangeAggregationBuilder("price_ranges").field("price").addUnboundedTo(100).addRange(100, 200).addUnboundedFrom(200);
    }

    public void testValidateAcceptsNonOverlappingRanges() throws Exception {
        translator.validate(priceRanges()); // no exception
    }

    public void testValidateAcceptsTouchingBoundsAsNonOverlap() throws Exception {
        // to is exclusive, so [0,100) and [100,200) share only the point 100 and do NOT overlap
        translator.validate(new RangeAggregationBuilder("r").field("price").addRange(0, 100).addRange(100, 200));
    }

    public void testValidateRejectsOverlappingRanges() {
        RangeAggregationBuilder agg = new RangeAggregationBuilder("r").field("price").addRange(0, 100).addRange(50, 150);
        ConversionException e = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(e.getMessage(), e.getMessage().contains("overlapping ranges"));
    }

    public void testValidateRejectsOverlapAcrossUnboundedRanges() {
        // [*,100) and [50,*) overlap on [50,100)
        RangeAggregationBuilder agg = new RangeAggregationBuilder("r").field("price").addUnboundedTo(100).addUnboundedFrom(50);
        expectThrows(ConversionException.class, () -> translator.validate(agg));
    }

    public void testValidateRejectsScript() {
        RangeAggregationBuilder agg = new RangeAggregationBuilder("r").script(new Script("doc['price'].value")).addRange(0, 100);
        ConversionException e = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(e.getMessage(), e.getMessage().contains("[script]"));
    }

    public void testValidateRejectsMissing() {
        RangeAggregationBuilder agg = new RangeAggregationBuilder("r").field("price").missing(0).addRange(0, 100);
        ConversionException e = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(e.getMessage(), e.getMessage().contains("[missing]"));
    }

    public void testGetGroupingProducesOrdinalExpressionGrouping() {
        GroupingInfo grouping = translator.getGrouping(priceRanges());
        assertThat(grouping, instanceOf(ExpressionGrouping.class));
        ExpressionGrouping eg = (ExpressionGrouping) grouping;

        // synthetic ordinal column, one per aggregation, cannot collide with a mapped field ('$')
        assertThat(eg.getFieldNames(), contains("_range$price_ranges"));
        assertEquals("price", eg.getSourceField());
        assertTrue("expression groupings carry no missing substitution", eg.getMissingByField().isEmpty());

        // bounds in declaration order, ordinal i -> bounds.get(i), with ±infinity for open ends
        assertThat(eg.getBounds(), hasSize(3));
        assertEquals(Double.NEGATIVE_INFINITY, eg.getBounds().get(0).from(), 0.0);
        assertEquals(100.0, eg.getBounds().get(0).to(), 0.0);
        assertEquals(100.0, eg.getBounds().get(1).from(), 0.0);
        assertEquals(200.0, eg.getBounds().get(1).to(), 0.0);
        assertEquals(200.0, eg.getBounds().get(2).from(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, eg.getBounds().get(2).to(), 0.0);
    }

    public void testGetSubAggregationsPassedThrough() {
        RangeAggregationBuilder agg = priceRanges().subAggregation(new AvgAggregationBuilder("avg_rating").field("rating"));
        assertThat(translator.getSubAggregations(agg), hasSize(1));
    }

    public void testGetSubAggregationsEmptyWhenNone() {
        assertTrue(translator.getSubAggregations(priceRanges()).isEmpty());
    }

    public void testRangeHasNoBucketOrder() {
        assertNull(translator.getBucketOrder(priceRanges()));
    }

    public void testGetAggregationType() {
        assertEquals(RangeAggregationBuilder.class, translator.getAggregationType());
    }

    public void testResolveFormatHonorsUserSuppliedFormat() {
        MapperService mapperService = mock(MapperService.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(mapperService.fieldType("price")).thenReturn(fieldType);
        when(fieldType.docValueFormat(any(), any())).thenReturn(DocValueFormat.RAW);

        RangeBucketTranslator t = new RangeBucketTranslator(() -> mapperService);
        t.toBucketAggregation(priceRanges().format("0.0"), List.of());

        // classic search honors the user-supplied format for key/from/to rendering; so must we
        verify(fieldType).docValueFormat("0.0", null);
    }
}
