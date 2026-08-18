/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.test.OpenSearchTestCase;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiTermsBucketTranslatorTests extends OpenSearchTestCase {

    private final MultiTermsBucketTranslator translator = new MultiTermsBucketTranslator();

    private static MultiTermsAggregationBuilder twoFieldAgg(String name, String field1, String field2) {
        return new MultiTermsAggregationBuilder(name).terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(field1).build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName(field2).build()
            )
        );
    }

    public void testGetAggregationTypeIsMultiTermsBuilder() {
        assertEquals(MultiTermsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testGroupingPreservesDeclaredFieldOrder() throws ConversionException {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        assertEquals(List.of("brand", "status"), translator.getGrouping(agg).getFieldNames());
    }

    public void testMissingParameterRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region").setMissing("N/A").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue(ex.getMessage().contains("missing"));
    }

    public void testScriptTermSourceRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region")
                    .setScript(new org.opensearch.script.Script("doc['region'].value"))
                    .build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue(ex.getMessage().contains("script"));
    }

    public void testExcludeRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand")
                    .setIncludeExclude(new org.opensearch.search.aggregations.bucket.terms.IncludeExclude(null, "foo.*"))
                    .build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue(ex.getMessage().contains("'exclude'"));
    }

    public void testFormatParameterRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("timestamp").setFormat("yyyy-MM-dd").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue("Message should mention 'format', got: " + ex.getMessage(), ex.getMessage().contains("format"));
        assertTrue("Message should name the offending field, got: " + ex.getMessage(), ex.getMessage().contains("timestamp"));
    }

    public void testTimeZoneParameterRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("timestamp").setTimeZone(ZoneId.of("America/New_York")).build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue("Message should mention 'time_zone', got: " + ex.getMessage(), ex.getMessage().contains("time_zone"));
        assertTrue("Message should name the offending field, got: " + ex.getMessage(), ex.getMessage().contains("timestamp"));
    }

    public void testRejectionNamesOffendingField() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region").setMissing("N/A").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.getGrouping(agg));
        assertTrue("Message should name the offending field, got: " + ex.getMessage(), ex.getMessage().contains("region"));
    }

    public void testEmptyBucketsProducesEmptyResult() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        InternalAggregation result = translator.toBucketAggregation(agg, List.of());
        assertTrue(result instanceof InternalMultiTerms);
        InternalMultiTerms multiTerms = (InternalMultiTerms) result;
        assertTrue(multiTerms.getBuckets().isEmpty());
    }

    public void testCompositeKeyRendersAsArrayAndPipeJoinedString() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "status", "region");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("active", "us"), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(1, result.getBuckets().size());
        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        assertEquals(List.of("active", "us"), bucket.getKey());
        assertEquals("active|us", bucket.getKeyAsString());
    }

    public void testNullKeyAtFirstPositionSkipsBucket() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "status", "region");
        List<BucketEntry> entries = new ArrayList<>();
        entries.add(new BucketEntry(listWithNull(null, "us"), 3, InternalAggregations.EMPTY));
        entries.add(new BucketEntry(List.of("active", "eu"), 2, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(1, result.getBuckets().size());
        assertEquals(List.of("active", "eu"), result.getBuckets().get(0).getKey());
    }

    public void testNullKeyAtSecondPositionSkipsBucket() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "status", "region");
        List<BucketEntry> entries = new ArrayList<>();
        entries.add(new BucketEntry(listWithNull("active", null), 3, InternalAggregations.EMPTY));
        entries.add(new BucketEntry(List.of("inactive", "eu"), 2, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(1, result.getBuckets().size());
        assertEquals(List.of("inactive", "eu"), result.getBuckets().get(0).getKey());
    }

    public void testMinDocCountFiltersBuckets() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.minDocCount(3);
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("BrandA", "active"), 5, InternalAggregations.EMPTY),
            new BucketEntry(List.of("BrandB", "inactive"), 2, InternalAggregations.EMPTY)
        );

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(1, result.getBuckets().size());
        assertEquals(List.of("BrandA", "active"), result.getBuckets().get(0).getKey());
    }

    public void testSizeTruncatesAndReportsSumOtherDocCount() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.size(2);
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("A", "x"), 5, InternalAggregations.EMPTY),
            new BucketEntry(List.of("B", "y"), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of("C", "z"), 2, InternalAggregations.EMPTY),
            new BucketEntry(List.of("D", "w"), 1, InternalAggregations.EMPTY)
        );

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(2, result.getBuckets().size());
        assertEquals(3L, result.getSumOfOtherDocCounts());
    }

    public void testBooleanPositionUsesBooleanFormat() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "is_active");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", true), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        // Boolean renders as true/false via DocValueFormat.BOOLEAN
        List<Object> key = bucket.getKey();
        assertEquals("BrandA", key.get(0));
        assertEquals("true", key.get(1).toString());
    }

    public void testIntegralPositionStoresLongValue() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "price");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 42L), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        List<Object> key = bucket.getKey();
        assertEquals("BrandA", key.get(0));
        assertEquals(42L, key.get(1));
    }

    public void testDoublePositionStoresDoubleValue() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "rating");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 3.5), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        List<Object> key = bucket.getKey();
        assertEquals("BrandA", key.get(0));
        assertEquals(3.5, key.get(1));
    }

    public void testDefaultOrderIsCountDescending() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        // Supply entries in count-ASCENDING order
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("A", "x"), 1, InternalAggregations.EMPTY),
            new BucketEntry(List.of("B", "y"), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of("C", "z"), 2, InternalAggregations.EMPTY)
        );

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(3, result.getBuckets().size());
        assertEquals(3L, result.getBuckets().get(0).getDocCount());
        assertEquals(2L, result.getBuckets().get(1).getDocCount());
        assertEquals(1L, result.getBuckets().get(2).getDocCount());
    }

    public void testKeyAscOrderWithCompositeKeys() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.order(BucketOrder.key(true));
        // Supply in non-lexicographic order
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("b", "a"), 1, InternalAggregations.EMPTY),
            new BucketEntry(List.of("a", "z"), 1, InternalAggregations.EMPTY),
            new BucketEntry(List.of("a", "a"), 1, InternalAggregations.EMPTY)
        );

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(3, result.getBuckets().size());
        assertEquals(List.of("a", "a"), result.getBuckets().get(0).getKey());
        assertEquals(List.of("a", "z"), result.getBuckets().get(1).getKey());
        assertEquals(List.of("b", "a"), result.getBuckets().get(2).getKey());
    }

    public void testThreeTermSourcesGroupingPreservesOrder() throws ConversionException {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("a").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("b").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("c").build()
            )
        );
        assertEquals(List.of("a", "b", "c"), translator.getGrouping(agg).getFieldNames());
    }

    public void testSubAggregationsAreExposed() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status").subAggregation(
            new AvgAggregationBuilder("avg_price").field("price")
        );
        assertEquals(1, translator.getSubAggregations(agg).size());
    }

    public void testMetadataRoundTrips() {
        Map<String, Object> meta = Map.of("source", "dashboard");
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.setMetadata(meta);

        InternalAggregation result = translator.toBucketAggregation(agg, List.of());
        assertEquals(meta, result.getMetadata());

        // No meta → null in response
        MultiTermsAggregationBuilder noMeta = twoFieldAgg("combo2", "brand", "status");
        assertNull(translator.toBucketAggregation(noMeta, List.of()).getMetadata());

        // Empty meta → treated as absent
        MultiTermsAggregationBuilder emptyMeta = twoFieldAgg("combo3", "brand", "status");
        emptyMeta.setMetadata(Map.of());
        assertNull(translator.toBucketAggregation(emptyMeta, List.of()).getMetadata());
    }

    public void testMultiTermsTranslatorIsRegistered() {
        assertTrue(AggregationRegistryFactory.create().get(MultiTermsAggregationBuilder.class) instanceof MultiTermsBucketTranslator);
    }

    public void testDateTypedTermSourceStoresLongEpoch() {
        // Date fields arrive as Long epoch millis from Calcite; the raw epoch is stored and rendered
        // without format conversion. This documents the known raw-epoch divergence from classic
        // multi_terms which would apply the mapping's date format.
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "created_at");
        long epochMillis = 1609459200000L; // 2021-01-01T00:00:00Z
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", epochMillis), 3, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        List<Object> key = bucket.getKey();
        assertEquals(epochMillis, key.get(1));
        assertTrue("Epoch Long key should render as the epoch number", bucket.getKeyAsString().contains("1609459200000"));
    }

    public void testIpTypedTermSourceRendersDottedAddress() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "client_ip");
        byte[] ipv4 = new byte[] { (byte) 192, (byte) 168, 1, 42 };
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", ipv4), 2, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        assertTrue(
            "IP key should render as dotted address, got: " + bucket.getKeyAsString(),
            bucket.getKeyAsString().contains("192.168.1.42")
        );
    }

    public void testFloatKeyWidenedToDouble() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "score");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 2.5f), 4, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
        List<Object> key = bucket.getKey();
        assertTrue("Float should be widened to Double, got: " + key.get(1).getClass(), key.get(1) instanceof Double);
        assertEquals(2.5, (Double) key.get(1), 0.0001);
    }

    public void testThreeTermSourcesToBucketAggregation() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("status").build()
            )
        );
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("Nike", "US", "active"), 10, InternalAggregations.EMPTY),
            new BucketEntry(List.of("Adidas", "EU", "inactive"), 7, InternalAggregations.EMPTY)
        );

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals(2, result.getBuckets().size());
        InternalMultiTerms.Bucket first = result.getBuckets().get(0);
        assertEquals(3, first.getKey().size());
        assertEquals(List.of("Nike", "US", "active"), first.getKey());
        assertEquals("Nike|US|active", first.getKeyAsString());

        InternalMultiTerms.Bucket second = result.getBuckets().get(1);
        assertEquals(3, second.getKey().size());
        assertEquals(List.of("Adidas", "EU", "inactive"), second.getKey());
        assertEquals("Adidas|EU|inactive", second.getKeyAsString());
    }

    public void testKeyArityMismatchThrows() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", "active", "extra"), 5, InternalAggregations.EMPTY));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> translator.toBucketAggregation(agg, entries));
        assertTrue("Message should contain expected count, got: " + ex.getMessage(), ex.getMessage().contains("2 key(s)"));
        assertTrue("Message should contain actual count, got: " + ex.getMessage(), ex.getMessage().contains("supplied 3"));
    }

    // ---- Helpers ----

    private static List<Object> listWithNull(Object first, Object second) {
        List<Object> list = new ArrayList<>();
        list.add(first);
        list.add(second);
        return list;
    }
}
