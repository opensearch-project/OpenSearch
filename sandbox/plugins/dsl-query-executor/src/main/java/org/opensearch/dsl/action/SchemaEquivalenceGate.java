/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.dsl.aggregation.bucket.TermsResponseStrategy;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.DocValueFormat;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Rejects a multi-index DSL request whose indices disagree on how a referenced field is typed or
 * rendered, before any plan executes. Two concerns, each comparing only the indices that define a
 * field (absent = no opinion): conversion equivalence — the collapsed Calcite type literals are
 * built from, via {@link OpenSearchSchemaBuilder#mapFieldType}, over every referenced field — and
 * render equivalence — the {@link TermsResponseStrategy}, {@link DocValueFormat}, and a
 * type-specific bucket-grouping discriminator (see {@link #renderDiscriminator}) bucket keys render
 * through, over every aggregated bucket field. A no-op below two indices.
 */
final class SchemaEquivalenceGate {

    /** Resolves a field's mapping within one index, returning null when that index does not define it. */
    @FunctionalInterface
    interface FieldTypeResolver {
        MappedFieldType resolve(IndexMetadata index, String field);
    }

    private SchemaEquivalenceGate() {}

    /**
     * @param indices resolved indices in resolution order; the gate is a no-op below two
     * @param fieldTypeResolver per-index field mapping lookup (null result = field absent in that index)
     * @param referencedFields every field the plan references (conversion concern)
     * @param aggregatedBucketFields the referenced fields that are terms-aggregation bucket keys (render concern)
     * @throws IllegalArgumentException if any defining indices diverge on conversion type or render strategy/format
     */
    static void check(
        List<IndexMetadata> indices,
        FieldTypeResolver fieldTypeResolver,
        Set<String> referencedFields,
        Set<String> aggregatedBucketFields
    ) {
        if (indices.size() <= 1) {
            return;
        }
        for (String field : referencedFields) {
            checkConversion(indices, fieldTypeResolver, field);
        }
        for (String field : aggregatedBucketFields) {
            checkRender(indices, fieldTypeResolver, field);
        }
    }

    private static void checkConversion(List<IndexMetadata> indices, FieldTypeResolver resolver, String field) {
        IndexMetadata definingIndex = null;
        SqlTypeName definingCalciteType = null;
        String definingTypeName = null;
        for (IndexMetadata index : indices) {
            MappedFieldType fieldType = resolver.resolve(index, field);
            if (fieldType == null) {
                continue;
            }
            SqlTypeName calciteType = OpenSearchSchemaBuilder.mapFieldType(fieldType.typeName());
            if (definingIndex == null) {
                definingIndex = index;
                definingCalciteType = calciteType;
                definingTypeName = fieldType.typeName();
                continue;
            }
            if (Objects.equals(definingCalciteType, calciteType) == false) {
                throw new IllegalArgumentException(
                    "field ["
                        + field
                        + "] resolves to incompatible conversion types across indices: ["
                        + definingIndex.getIndex().getName()
                        + "] maps ["
                        + definingTypeName
                        + "] -> "
                        + definingCalciteType
                        + " but ["
                        + index.getIndex().getName()
                        + "] maps ["
                        + fieldType.typeName()
                        + "] -> "
                        + calciteType
                );
            }
        }
    }

    private static void checkRender(List<IndexMetadata> indices, FieldTypeResolver resolver, String field) {
        IndexMetadata definingIndex = null;
        String definingTypeName = null;
        TermsResponseStrategy definingStrategy = null;
        DocValueFormat definingFormat = null;
        String definingDiscriminator = null;
        for (IndexMetadata index : indices) {
            MappedFieldType fieldType = resolver.resolve(index, field);
            if (fieldType == null) {
                continue;
            }
            TermsResponseStrategy strategy = TermsResponseStrategy.forType(fieldType.typeName());
            DocValueFormat format = fieldType.docValueFormat(null, null);
            String discriminator = renderDiscriminator(fieldType);
            if (definingIndex == null) {
                definingIndex = index;
                definingTypeName = fieldType.typeName();
                definingStrategy = strategy;
                definingFormat = format;
                definingDiscriminator = discriminator;
                continue;
            }
            if (definingStrategy != strategy) {
                throw new IllegalArgumentException(
                    "aggregated field ["
                        + field
                        + "] resolves to incompatible render strategies across indices: ["
                        + definingIndex.getIndex().getName()
                        + "] ("
                        + definingTypeName
                        + " -> "
                        + definingStrategy.getClass().getSimpleName()
                        + ") vs ["
                        + index.getIndex().getName()
                        + "] ("
                        + fieldType.typeName()
                        + " -> "
                        + strategy.getClass().getSimpleName()
                        + ")"
                );
            }
            if (formatsEqual(definingFormat, format) == false) {
                throw new IllegalArgumentException(
                    "aggregated field ["
                        + field
                        + "] resolves to incompatible doc-value formats across indices: ["
                        + definingIndex.getIndex().getName()
                        + "] ("
                        + definingTypeName
                        + " format "
                        + definingFormat
                        + ") vs ["
                        + index.getIndex().getName()
                        + "] ("
                        + fieldType.typeName()
                        + " format "
                        + format
                        + ")"
                );
            }
            if (Objects.equals(definingDiscriminator, discriminator) == false) {
                throw new IllegalArgumentException(
                    "aggregated field ["
                        + field
                        + "] resolves to incompatible bucket-grouping parameters across indices: ["
                        + definingIndex.getIndex().getName()
                        + "] ("
                        + definingTypeName
                        + " -> "
                        + definingDiscriminator
                        + ") vs ["
                        + index.getIndex().getName()
                        + "] ("
                        + fieldType.typeName()
                        + " -> "
                        + discriminator
                        + ")"
                );
            }
        }
    }

    /**
     * A grouping discriminator layered on top of {@code {strategy, docValueFormat}}. Two fields can
     * share a render strategy AND a doc-value format yet still place the SAME logical value in
     * DIFFERENT terms buckets, because a type-specific storage or index-time parameter — invisible
     * to both — changes the raw key the aggregation groups on:
     *
     * <ul>
     *   <li>{@code scaled_float}: {@code scaling_factor} turns a value into the stored long (10.5
     *       becomes 1050 at factor 100, 10500 at factor 1000), so differing factors never share a
     *       bucket. The concrete field type lives in the mapper-extras module, off this plugin's
     *       compile classpath, so the factor is read reflectively.</li>
     *   <li>{@code keyword}: a normalizer folds terms at index time ('FOO' becomes 'foo'), so a
     *       normalized index and a plain one bucket the same input onto different keys. The
     *       normalizer surfaces as the field's index analyzer; the built-in keyword analyzer
     *       ({@code "_keyword"}) means "no normalizer" and reads as null, so a plain keyword still
     *       matches a text field and that harmless divergence keeps passing.</li>
     *   <li>{@code date} / {@code date_nanos}: the aggregation doc-value format is pinned to
     *       millisecond resolution, so the stored resolution is invisible, yet millis and nanos are
     *       different raw longs for the same instant.</li>
     *   <li>{@code float} / {@code half_float} / {@code double}: all share the double render
     *       strategy and a RAW format, but fp16, fp32 and fp64 round the same input to different
     *       stored values.</li>
     * </ul>
     *
     * Returns null when no such parameter applies, so the base signature alone governs.
     */
    private static String renderDiscriminator(MappedFieldType fieldType) {
        String typeName = fieldType.typeName();
        switch (typeName) {
            case "scaled_float":
                Double factor = scaledFloatScalingFactor(fieldType);
                if (factor == null) {
                    // Fail CLOSED: the scaling_factor could not be read (accessor renamed/removed),
                    // so we cannot prove two scaled_float fields share it. Returning a bare
                    // "scaled_float" would collapse every unresolved field to one value, let
                    // Objects.equals pass, and silently reopen the grouping-corruption hole this
                    // gate closes. Keying on the field type's identity guarantees two unresolved
                    // fields never compare equal, forcing the gate's normal incompatible-parameters
                    // rejection rather than a silent pass.
                    return "scaled_float:unresolved:" + System.identityHashCode(fieldType);
                }
                return "scaled_float:" + factor;
            case "half_float":
            case "float":
            case "double":
                // Floating-point widths never bucket-align: the same input rounds to a different
                // stored value at each precision.
                return typeName;
            default:
                // Non-floating types carry their discriminator on the concrete field type below.
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            // Resolution (millis vs nanos) is absent from the millis-pinned aggregation format but
            // changes the raw long the bucket key is built from.
            return "resolution:" + ((DateFieldMapper.DateFieldType) fieldType).resolution();
        }
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
            NamedAnalyzer analyzer = fieldType.indexAnalyzer();
            String normalizer = analyzer == null ? null : analyzer.name();
            // "_keyword" is the built-in no-op analyzer a plain keyword carries; only a genuine
            // (renaming) normalizer changes the stored term, so treat the default as "no opinion".
            if (normalizer != null && "_keyword".equals(normalizer) == false) {
                return "normalizer:" + normalizer;
            }
        }
        return null;
    }

    /**
     * Reads {@code scaled_float}'s {@code scaling_factor} without a compile-time dependency on the
     * mapper-extras module that defines {@code ScaledFloatFieldType}. Returns null when the accessor
     * is absent, in which case the caller fails closed (see {@link #renderDiscriminator}).
     */
    private static Double scaledFloatScalingFactor(MappedFieldType fieldType) {
        try {
            Method accessor = fieldType.getClass().getMethod("getScalingFactor");
            Object value = accessor.invoke(fieldType);
            return value instanceof Number ? ((Number) value).doubleValue() : null;
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    /**
     * Two {@link DocValueFormat}s are equivalent by value {@code equals}, with a
     * {@link DocValueFormat#RAW} reference-equality fast path — RAW is a singleton, so it is
     * equivalent only to itself.
     */
    private static boolean formatsEqual(DocValueFormat a, DocValueFormat b) {
        if (a == DocValueFormat.RAW || b == DocValueFormat.RAW) {
            return a == b;
        }
        return a.equals(b);
    }
}
