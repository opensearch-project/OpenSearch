/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.List;
import java.util.Map;

/**
 * Strategy for building a terms aggregation response from bucket entries. Each implementation
 * handles one family of field types (string, long, double), mirroring the
 * {@code ValuesSourceRegistry} pattern from classic search: adding support for a new field type
 * is a registry entry, not a new branch in a switch.
 */
public interface TermsResponseStrategy {

    /**
     * Builds the typed {@code InternalTerms} response from the given bucket entries.
     *
     * @param agg           the original terms aggregation builder (name, order, request echo)
     * @param entries       the visible bucket set, exactly as the plan produced it — null keys
     *                      excluded, {@code min_doc_count} applied, ordered, truncated
     * @param otherDocCount the {@code sum_other_doc_count} to report, computed by the caller
     *                      from the plan's eligible-doc count
     * @param format        the resolved {@link DocValueFormat} for key rendering
     * @return a fully constructed aggregation ready for serialization
     */
    InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, long otherDocCount, DocValueFormat format);

    /**
     * Registry mapping field type names to their response strategy.
     *
     * <p>Key: {@code MappedFieldType.typeName()} (e.g. "keyword", "long", "date", "ip").
     * Value: the strategy that builds the correct {@code InternalTerms} subclass.
     */
    Map<String, TermsResponseStrategy> REGISTRY = Map.ofEntries(
        // Numeric integral types → LongTerms
        Map.entry("long", LongTermsStrategy.INSTANCE),
        Map.entry("integer", LongTermsStrategy.INSTANCE),
        Map.entry("short", LongTermsStrategy.INSTANCE),
        Map.entry("byte", LongTermsStrategy.INSTANCE),
        Map.entry("date", LongTermsStrategy.INSTANCE),
        Map.entry("date_nanos", LongTermsStrategy.INSTANCE),
        Map.entry("unsigned_long", LongTermsStrategy.INSTANCE),
        Map.entry("boolean", LongTermsStrategy.BOOLEAN_INSTANCE),

        // Floating point types → DoubleTerms
        Map.entry("float", DoubleTermsStrategy.INSTANCE),
        Map.entry("double", DoubleTermsStrategy.INSTANCE),
        Map.entry("half_float", DoubleTermsStrategy.INSTANCE),
        Map.entry("scaled_float", DoubleTermsStrategy.INSTANCE),

        // String/binary types → StringTerms
        Map.entry("keyword", StringTermsStrategy.INSTANCE),
        Map.entry("ip", StringTermsStrategy.INSTANCE),
        Map.entry("text", StringTermsStrategy.INSTANCE),
        Map.entry("wildcard", StringTermsStrategy.INSTANCE),
        Map.entry("constant_keyword", StringTermsStrategy.INSTANCE),
        Map.entry("match_only_text", StringTermsStrategy.INSTANCE)
    );

    /** Default strategy when the field type is unknown — falls back to StringTerms. */
    TermsResponseStrategy DEFAULT = StringTermsStrategy.INSTANCE;

    /**
     * Resolves the strategy for the given field type name.
     *
     * @param typeName the field's {@code MappedFieldType.typeName()}, or null if unknown
     * @return the matching strategy, or {@link #DEFAULT} if not registered
     */
    static TermsResponseStrategy forType(String typeName) {
        if (typeName == null) {
            return DEFAULT;
        }
        return REGISTRY.getOrDefault(typeName, DEFAULT);
    }
}
