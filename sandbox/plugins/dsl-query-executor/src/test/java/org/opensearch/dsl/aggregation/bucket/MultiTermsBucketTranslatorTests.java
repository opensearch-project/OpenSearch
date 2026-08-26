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
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiTermsBucketTranslatorTests extends OpenSearchTestCase {

    /** Field types the mapped translator resolves, mirroring a real index mapping. */
    private static final Map<String, MappedFieldType> FIELD_TYPES = Map.ofEntries(
        Map.entry("brand", new KeywordFieldMapper.KeywordFieldType("brand")),
        Map.entry("status", new KeywordFieldMapper.KeywordFieldType("status")),
        Map.entry("region", new KeywordFieldMapper.KeywordFieldType("region")),
        Map.entry("is_active", new BooleanFieldMapper.BooleanFieldType("is_active")),
        Map.entry("price", new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG)),
        Map.entry("rating", new NumberFieldMapper.NumberFieldType("rating", NumberFieldMapper.NumberType.DOUBLE)),
        Map.entry("score", new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.FLOAT)),
        Map.entry("client_ip", new IpFieldMapper.IpFieldType("client_ip")),
        Map.entry("created_at", new DateFieldMapper.DateFieldType("created_at")),
        Map.entry("count_i", new NumberFieldMapper.NumberFieldType("count_i", NumberFieldMapper.NumberType.INTEGER)),
        Map.entry("count_s", new NumberFieldMapper.NumberFieldType("count_s", NumberFieldMapper.NumberType.SHORT)),
        Map.entry("count_b", new NumberFieldMapper.NumberFieldType("count_b", NumberFieldMapper.NumberType.BYTE)),
        Map.entry("big", new NumberFieldMapper.NumberFieldType("big", NumberFieldMapper.NumberType.UNSIGNED_LONG)),
        Map.entry("ratio", new NumberFieldMapper.NumberFieldType("ratio", NumberFieldMapper.NumberType.HALF_FLOAT)),
        Map.entry("amount", scaledFloatType("amount"))
    );

    /** scaled_float lives in mapper-extras; a mock supplies its typeName and RAW key format. */
    private static MappedFieldType scaledFloatType(String name) {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn("scaled_float");
        when(fieldType.docValueFormat(null, null)).thenReturn(DocValueFormat.RAW);
        return fieldType;
    }

    private final MultiTermsBucketTranslator translator = new MultiTermsBucketTranslator(MultiTermsBucketTranslatorTests::mappedService);
    private final MultiTermsBucketTranslator unmappedTranslator = new MultiTermsBucketTranslator(() -> null);

    private static MapperService mappedService() {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(anyString())).thenAnswer(invocation -> FIELD_TYPES.get(invocation.<String>getArgument(0)));
        return mapperService;
    }

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

    // ---- validate(): unsupported per-source parameters are rejected ----

    public void testMissingParameterRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("region").setMissing("N/A").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
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
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
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
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(ex.getMessage().contains("'exclude'"));
    }

    public void testFormatParameterRejected() {
        MultiTermsAggregationBuilder agg = new MultiTermsAggregationBuilder("combo").terms(
            List.of(
                new MultiTermsValuesSourceConfig.Builder().setFieldName("brand").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName("timestamp").setFormat("yyyy-MM-dd").build()
            )
        );
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
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
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
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
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue("Message should name the offending field, got: " + ex.getMessage(), ex.getMessage().contains("region"));
    }

    /** A date-mapped term source cannot render its keys with mapping formats — reject at conversion. */
    public void testValidateRejectsDateTermSource() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "created_at");
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(
            "Message should name the date field, got: " + ex.getMessage(),
            ex.getMessage().contains("date term source [created_at]")
        );
    }

    public void testValidateRejectsDateNanosTermSource() {
        MappedFieldType nanosType = mock(MappedFieldType.class);
        when(nanosType.typeName()).thenReturn(DateFieldMapper.DATE_NANOS_CONTENT_TYPE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("created_nanos")).thenReturn(nanosType);
        MultiTermsBucketTranslator nanosTranslator = new MultiTermsBucketTranslator(() -> mapperService);

        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "created_nanos");
        ConversionException ex = expectThrows(ConversionException.class, () -> nanosTranslator.validate(agg));
        assertTrue(ex.getMessage().contains("date term source [created_nanos]"));
    }

    /** min_doc_count: 0 would require enumerating unmatched term combinations — reject it. */
    public void testValidateRejectsMinDocCountZero() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.minDocCount(0);
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.validate(agg));
        assertTrue(ex.getMessage().contains("min_doc_count: 0"));
    }

    /** Mapping-dependent validation is skipped when no MapperService is supplied (conversion-only use). */
    public void testValidateSkipsDateCheckWithoutMapperService() throws Exception {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "created_at");
        unmappedTranslator.validate(agg); // must not throw
    }

    public void testValidateAcceptsSupportedTermSources() throws Exception {
        translator.validate(twoFieldAgg("combo", "brand", "status")); // must not throw
    }

    // ---- Rendering ----

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

    public void testFloatKeyWidenedToDouble() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "score");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 2.5f), 4, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        assertTrue("Float should be widened to Double, got: " + key.get(1).getClass(), key.get(1) instanceof Double);
        assertEquals(2.5, (Double) key.get(1), 0.0001);
    }

    // ---- Per-type composite keys: numeric mappings mirror the single-field LongTerms/DoubleTerms path ----

    public void testShortPositionStoresLongValue() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "count_s");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", (short) 7), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        assertEquals(7L, key.get(1));
        assertEquals("BrandA|7", result.getBuckets().get(0).getKeyAsString());
    }

    public void testBytePositionStoresLongValue() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "count_b");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", (byte) 3), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        assertEquals(3L, key.get(1));
        assertEquals("BrandA|3", result.getBuckets().get(0).getKeyAsString());
    }

    public void testUnsignedLongPositionStoresLongAndRendersViaUnsignedFormat() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "big");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 42L), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        assertEquals(BigInteger.valueOf(42), key.get(1));
        assertEquals("BrandA|42", result.getBuckets().get(0).getKeyAsString());
    }

    public void testHalfFloatKeyWidenedToDouble() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "ratio");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 0.5f), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        assertTrue("half_float should widen to Double, got: " + key.get(1).getClass(), key.get(1) instanceof Double);
        assertEquals(0.5, (Double) key.get(1), 0.0001);
        assertEquals("BrandA|0.5", result.getBuckets().get(0).getKeyAsString());
    }

    /**
     * scaled_float keys arrive as the raw scaled value the engine delivers — {@code 4.44} at
     * {@code scaling_factor: 100} is delivered as the integral {@code 444} (as the
     * {@code range_scaled_float} plan shows {@code 1.5} planned as {@code 150}), not a pre-divided
     * double. This render path resolves the source's {@code docValueFormat(null, null)}, which for
     * scaled_float is {@link DocValueFormat#RAW}, so the raw scaled value passes through unchanged.
     *
     * <p>Residual gap: the scaling-factor division lives in {@code ScaledFloatFieldType.valueForDisplay},
     * which this path does not call, so the multi_terms render does NOT divide by the factor — a
     * scaled_float key renders as its raw scaled value ({@code 444}), not {@code 4.44}. Reproducing
     * the divided value would require the real scaled_float field type from mapper-extras, which is
     * not on this test classpath and must not be added as a dependency.
     */
    public void testScaledFloatKeyRendersRawScaledValueUndivided() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "amount");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", 444L), 5, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        List<Object> key = result.getBuckets().get(0).getKey();
        // Raw scaled value is integral and stored as a long under RAW — not divided by the factor.
        assertEquals(444L, key.get(1));
        assertEquals("BrandA|444", result.getBuckets().get(0).getKeyAsString());
    }

    /** Rendering fails loudly when no MapperService is available to resolve a position's key format. */
    public void testRenderFailsLoudlyWithoutMapperService() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "client_ip", "brand");
        byte[] ipv4 = new byte[] { (byte) 192, (byte) 168, 1, 42 };
        List<BucketEntry> entries = List.of(new BucketEntry(List.of(ipv4, "BrandA"), 2, InternalAggregations.EMPTY));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> unmappedTranslator.toBucketAggregation(agg, entries));
        assertTrue("Message should name the aggregation, got: " + ex.getMessage(), ex.getMessage().contains("combo"));
        assertTrue("Message should name the unresolvable field, got: " + ex.getMessage(), ex.getMessage().contains("client_ip"));
    }

    /** With a mapping the ip source resolves to DocValueFormat.IP and renders the encoded address. */
    public void testMappedIpSourceRendersViaIpFormat() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "client_ip");
        byte[] ipv4In16 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 10, 0, 0, 1 };
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", ipv4In16), 2, InternalAggregations.EMPTY));

        InternalMultiTerms result = (InternalMultiTerms) translator.toBucketAggregation(agg, entries);

        assertEquals("BrandA|10.0.0.1", result.getBuckets().get(0).getKeyAsString());
    }

    public void testKeyArityMismatchThrows() {
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA", "active", "extra"), 5, InternalAggregations.EMPTY));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> translator.toBucketAggregation(agg, entries));
        assertTrue("Message should contain expected count, got: " + ex.getMessage(), ex.getMessage().contains("2 key(s)"));
        assertTrue("Message should contain actual count, got: " + ex.getMessage(), ex.getMessage().contains("supplied 3"));
    }

    public void testMetadataRoundTrips() {
        Map<String, Object> meta = Map.of("source", "dashboard");
        MultiTermsAggregationBuilder agg = twoFieldAgg("combo", "brand", "status");
        agg.setMetadata(meta);
        assertEquals(meta, translator.toBucketAggregation(agg, List.of()).getMetadata());

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
}
