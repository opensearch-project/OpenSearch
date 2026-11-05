/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaEquivalenceGateTests extends OpenSearchTestCase {

    // Single index: the gate short-circuits before comparing anything, so even a lone divergent
    // field never throws.
    public void testSingleIndexIsNoOp() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", keyword("brand")));

        check(mappings, Set.of("brand"), Set.of("brand"));
    }

    public void testIdenticalMappingsAcrossIndicesPass() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", keyword("brand"), "price", longField("price")));
        mappings.put("idx2", Map.of("brand", keyword("brand"), "price", longField("price")));

        check(mappings, Set.of("brand", "price"), Set.of("brand"));
    }

    // keyword and text both convert to VARCHAR, both render through StringTermsStrategy with a RAW
    // format — harmless divergence.
    public void testKeywordVsTextOnAggregatedFieldPasses() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", keyword("brand")));
        mappings.put("idx2", Map.of("brand", text("brand")));

        check(mappings, Set.of("brand"), Set.of("brand"));
    }

    // long and scaled_float both collapse to BIGINT (conversion agrees) but render through
    // LongTermsStrategy vs DoubleTermsStrategy — only the render gate can see it.
    public void testLongVsScaledFloatOnAggregatedFieldRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("metric", longField("metric")));
        mappings.put("idx2", Map.of("metric", scaledFloat("metric", 100)));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> check(mappings, Set.of("metric"), Set.of("metric"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("metric"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
        assertTrue(e.getMessage(), e.getMessage().contains("LongTermsStrategy"));
        assertTrue(e.getMessage(), e.getMessage().contains("DoubleTermsStrategy"));
    }

    // Both date fields convert to TIMESTAMP and render through LongTermsStrategy; only the
    // docValueFormat (epoch_millis vs strict_date) differs.
    public void testDateFormatDivergenceOnAggregatedFieldRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("ts", date("ts", "epoch_millis")));
        mappings.put("idx2", Map.of("ts", date("ts", "strict_date")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("ts"), Set.of("ts")));
        assertTrue(e.getMessage(), e.getMessage().contains("ts"));
        assertTrue(e.getMessage(), e.getMessage().contains("format"));
    }

    // ip is VARBINARY, keyword is VARCHAR — the conversion gate rejects the split.
    public void testIpVsKeywordOnAggregatedFieldRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("addr", ip("addr")));
        mappings.put("idx2", Map.of("addr", keyword("addr")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("addr"), Set.of("addr")));
        assertTrue(e.getMessage(), e.getMessage().contains("addr"));
    }

    // Two scaled_float indices whose scaling_factor differs (100 vs 1000) agree on strategy
    // (DoubleTermsStrategy) and doc-value format (RAW), yet store 10.5 as raw 1050 vs 10500 — the
    // same logical value lands in different buckets, so the request must be rejected.
    public void testScaledFloatDifferentScalingFactorRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("price", scaledFloat("price", 100)));
        mappings.put("idx2", Map.of("price", scaledFloat("price", 1000)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("price"), Set.of("price")));
        assertTrue(e.getMessage(), e.getMessage().contains("price"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    // Two scaled_float indices with the SAME scaling_factor bucket identically — allowed.
    public void testScaledFloatSameScalingFactorPasses() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("price", scaledFloat("price", 100)));
        mappings.put("idx2", Map.of("price", scaledFloat("price", 100)));

        check(mappings, Set.of("price"), Set.of("price"));
    }

    // Both indices report typeName() 'scaled_float' but neither exposes getScalingFactor(), so the
    // reflective read throws NoSuchMethodException and the factor cannot be resolved. The gate
    // cannot prove the two fields share a scaling_factor, so it must fail CLOSED and reject rather
    // than collapse both to a bare 'scaled_float' and silently pass.
    public void testScaledFloatUnresolvableScalingFactorRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("price", scaledFloatNoAccessor("price")));
        mappings.put("idx2", Map.of("price", scaledFloatNoAccessor("price")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("price"), Set.of("price")));
        assertTrue(e.getMessage(), e.getMessage().contains("price"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    // A keyword with a lowercase normalizer folds 'FOO' -> 'foo' at index time; the un-normalized
    // index keeps 'FOO'. Both are VARCHAR / StringTermsStrategy / RAW, so only a normalizer-aware
    // render check can see the split.
    public void testKeywordNormalizerVsPlainKeywordRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", normalizedKeyword("brand", "lowercase")));
        mappings.put("idx2", Map.of("brand", keyword("brand")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("brand"), Set.of("brand")));
        assertTrue(e.getMessage(), e.getMessage().contains("brand"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    // The same normalizer on both indices folds identically — allowed.
    public void testKeywordSameNormalizerPasses() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", normalizedKeyword("brand", "lowercase")));
        mappings.put("idx2", Map.of("brand", normalizedKeyword("brand", "lowercase")));

        check(mappings, Set.of("brand"), Set.of("brand"));
    }

    // date stores millis-since-epoch, date_nanos stores nanos-since-epoch; both render through
    // LongTermsStrategy and — because the aggregation doc-value format is pinned to millisecond
    // resolution — through an equal DocValueFormat. The raw bucket key still diverges by a factor
    // of a million, so the request must be rejected.
    public void testDateVsDateNanosRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("ts", dateMillis("ts")));
        mappings.put("idx2", Map.of("ts", dateNanos("ts")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("ts"), Set.of("ts")));
        assertTrue(e.getMessage(), e.getMessage().contains("ts"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    // half_float and float both render through DoubleTermsStrategy with a RAW format, but fp16 and
    // fp32 round the same input to different stored values, so their buckets do not align.
    public void testHalfFloatVsFloatRejects() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("ratio", halfFloat("ratio")));
        mappings.put("idx2", Map.of("ratio", floatField("ratio")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("ratio"), Set.of("ratio")));
        assertTrue(e.getMessage(), e.getMessage().contains("ratio"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    // A field the plan never references is invisible to the gate, however badly it diverges.
    public void testDivergentButUnreferencedFieldPasses() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("ok", keyword("ok"), "z", keyword("z")));
        mappings.put("idx2", Map.of("ok", keyword("ok"), "z", longField("z")));

        check(mappings, Set.of("ok"), Set.of());
    }

    // Absent in one index expresses no opinion; the remaining defining indices agree.
    public void testFieldAbsentInOneIndexPasses() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("brand", keyword("brand")));
        mappings.put("idx2", Map.of());

        check(mappings, Set.of("brand"), Set.of("brand"));
    }

    // A query-referenced (non-aggregated) field whose conversion type diverges is rejected by the
    // conversion concern, which inspects every referenced field, not just bucket fields.
    public void testConversionRejectsQueryReferencedNonAggregatedField() {
        Map<String, Map<String, MappedFieldType>> mappings = new LinkedHashMap<>();
        mappings.put("idx1", Map.of("f", keyword("f")));
        mappings.put("idx2", Map.of("f", longField("f")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check(mappings, Set.of("f"), Set.of()));
        assertTrue(e.getMessage(), e.getMessage().contains("f"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx1"));
        assertTrue(e.getMessage(), e.getMessage().contains("idx2"));
    }

    private static void check(
        Map<String, Map<String, MappedFieldType>> mappings,
        Set<String> referencedFields,
        Set<String> aggregatedBucketFields
    ) {
        List<IndexMetadata> indices = new ArrayList<>(mappings.size());
        for (String name : mappings.keySet()) {
            indices.add(indexMetadata(name));
        }
        SchemaEquivalenceGate.FieldTypeResolver resolver = (index, field) -> mappings.getOrDefault(index.getIndex().getName(), Map.of())
            .get(field);
        SchemaEquivalenceGate.check(indices, resolver, referencedFields, aggregatedBucketFields);
    }

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static MappedFieldType keyword(String name) {
        return new KeywordFieldMapper.KeywordFieldType(name);
    }

    private static MappedFieldType text(String name) {
        return new TextFieldMapper.TextFieldType(name);
    }

    private static MappedFieldType longField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
    }

    private static MappedFieldType ip(String name) {
        return new IpFieldMapper.IpFieldType(name);
    }

    private static MappedFieldType date(String name, String pattern) {
        return new DateFieldMapper.DateFieldType(name, DateFormatter.forPattern(pattern));
    }

    private static MappedFieldType dateMillis(String name) {
        return new DateFieldMapper.DateFieldType(name, DateFieldMapper.Resolution.MILLISECONDS);
    }

    private static MappedFieldType dateNanos(String name) {
        return new DateFieldMapper.DateFieldType(name, DateFieldMapper.Resolution.NANOSECONDS);
    }

    private static MappedFieldType halfFloat(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.HALF_FLOAT);
    }

    private static MappedFieldType floatField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.FLOAT);
    }

    // A keyword whose index-time normalizer is a NamedAnalyzer with the given name, mirroring how
    // production wires a configured normalizer onto the field's index analyzer. The gate reads only
    // the analyzer name, so the underlying Analyzer implementation is immaterial.
    private static MappedFieldType normalizedKeyword(String name, String normalizerName) {
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(name);
        fieldType.setIndexAnalyzer(new NamedAnalyzer(normalizerName, AnalyzerScope.INDEX, new KeywordAnalyzer()));
        return fieldType;
    }

    // scaled_float lives in the mapper-extras module (off this plugin's classpath). The gate reads
    // typeName(), docValueFormat(null, null), and the scaling factor via a reflective
    // getScalingFactor() call, so this faithful double reports scaled_float with a RAW format and
    // exposes that same accessor.
    private static MappedFieldType scaledFloat(String name, double scalingFactor) {
        return new ScaledFloatFieldTypeDouble(name, scalingFactor);
    }

    // A scaled_float that reports the type name but omits getScalingFactor(), mirroring the mapper
    // renaming/removing the accessor. Reflection throws NoSuchMethodException, so the factor is
    // unresolvable and the gate must fail closed.
    private static MappedFieldType scaledFloatNoAccessor(String name) {
        return new ScaledFloatFieldTypeNoAccessor(name);
    }

    static final class ScaledFloatFieldTypeDouble extends NumberFieldMapper.NumberFieldType {
        private final double scalingFactor;

        ScaledFloatFieldTypeDouble(String name, double scalingFactor) {
            super(name, NumberFieldMapper.NumberType.DOUBLE);
            this.scalingFactor = scalingFactor;
        }

        @Override
        public String typeName() {
            return "scaled_float";
        }

        public double getScalingFactor() {
            return scalingFactor;
        }
    }

    // Reports typeName() 'scaled_float' with a RAW-format DOUBLE base but deliberately exposes no
    // getScalingFactor(), so the gate's reflective lookup throws NoSuchMethodException.
    static final class ScaledFloatFieldTypeNoAccessor extends NumberFieldMapper.NumberFieldType {
        ScaledFloatFieldTypeNoAccessor(String name) {
            super(name, NumberFieldMapper.NumberType.DOUBLE);
        }

        @Override
        public String typeName() {
            return "scaled_float";
        }
    }
}
