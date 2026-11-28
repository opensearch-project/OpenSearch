/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper.SharedParameter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The storage-neutral field mapping parameters a {@link DataFormatPlugin} may contribute through
 * {@link DataFormatPlugin#getPluginMappingParameters}: {@value #CODEC}, {@value #BLOOM_FILTER} and
 * {@value #CARDINALITY}. Parameters built
 * here share one vocabulary across formats; each format supplies its own validator and translates the resolved value
 * to its physical layout.
 *
 * <p>All are {@link SharedParameter}s: when several formats participating in the same index contribute the same
 * one, the contributions merge into a single mapping parameter whose validator is the conjunction of theirs, so a
 * value is accepted only if every participating format can honour it. Composite plugins perform that merge with
 * {@link SharedParameter#mergeInto}.
 *
 * <p>These parameters only exist on indices that use a pluggable data format: the registry contributes them per
 * index, so a plain Lucene index rejects them as unknown.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldStorageParameters {

    /** Mapping parameter holding a {@link FieldCodec}: how the field's column is encoded and compressed. */
    public static final String CODEC = "codec";

    /** Boolean mapping parameter requesting a per-column bloom filter for equality lookups. */
    public static final String BLOOM_FILTER = "bloom_filter";

    /**
     * Mapping parameter hinting at the field's value cardinality, {@value #CARDINALITY_LOW} or
     * {@value #CARDINALITY_HIGH}. A storage hint, not a constraint: formats use it to pick a default layout (for
     * example dictionary encoding plus a bloom filter for low-cardinality columns) when no explicit {@value #CODEC}
     * is given.
     */
    public static final String CARDINALITY = "cardinality";
    /** Few distinct values relative to row count (status codes, regions, service names). */
    public static final String CARDINALITY_LOW = "low";
    /** Many distinct values, approaching unique (identifiers, trace ids, timestamps). */
    public static final String CARDINALITY_HIGH = "high";
    /** The accepted {@value #CARDINALITY} values. */
    public static final Set<String> CARDINALITIES = Set.of(CARDINALITY_LOW, CARDINALITY_HIGH);

    private FieldStorageParameters() {}

    /**
     * Creates a {@value #CODEC} parameter. Absent by default; not updateable, since files already written keep their
     * layout. The given validator runs after parsing and should reject codecs the format cannot honour for the
     * field's type, normally via {@link FieldCodec#requireSupported}.
     *
     * @param validator format-specific check applied to the parsed codec
     */
    public static SharedParameter<FieldCodec> codec(Consumer<FieldCodec> validator) {
        return new SharedParameter<>(CODEC, null, (name, context, value) -> FieldCodec.parse(value), List.of(validator)).setMappingValue(
            FieldCodec::toMappingValue
        );
    }

    /**
     * Creates a {@value #BLOOM_FILTER} parameter, defaulting to {@code false} and not updateable. Formats that
     * support bloom filters only for some field types pass a validator that rejects {@code true} elsewhere.
     */
    public static SharedParameter<Boolean> bloomFilter(Consumer<Boolean> validator) {
        return new SharedParameter<>(
            BLOOM_FILTER,
            false,
            (name, context, value) -> XContentMapValues.nodeBooleanValue(value),
            List.of(validator)
        );
    }

    /** Creates a {@value #BLOOM_FILTER} parameter that accepts either value; see {@link #bloomFilter(Consumer)}. */
    public static SharedParameter<Boolean> bloomFilter() {
        return bloomFilter(enabled -> {});
    }

    /**
     * Creates a {@value #CARDINALITY} parameter. Absent by default and not updateable. The value is validated against
     * {@link #CARDINALITIES} before the format's validator runs; the format's side effect, if any, is applied at
     * build time (for example a low-cardinality keyword opting out of inverted indexing).
     *
     * @param validator  format-specific check applied to the parsed hint
     * @param sideEffect format-specific adjustment of sibling parameters, applied when the mapper is built
     */
    public static SharedParameter<String> cardinality(
        Consumer<String> validator,
        BiConsumer<ParametrizedFieldMapper.Builder, String> sideEffect
    ) {
        return new SharedParameter<>(CARDINALITY, null, (name, context, value) -> {
            String hint = value.toString().trim().toLowerCase(Locale.ROOT);
            if (CARDINALITIES.contains(hint) == false) {
                throw new IllegalArgumentException("cardinality must be one of " + new TreeSet<>(CARDINALITIES) + ", got [" + value + "]");
            }
            return hint;
        }, List.of(validator), List.of(sideEffect)).setMappingValue(hint -> hint);
    }

    /** Creates a {@value #CARDINALITY} parameter with no format-specific validation or side effect. */
    public static SharedParameter<String> cardinality() {
        return cardinality(hint -> {}, (builder, hint) -> {});
    }

    /** Returns the fields of the mapping that declare a {@value #CARDINALITY} hint, with its value. */
    public static Map<String, String> cardinalities(MapperService mapperService) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : parameterValues(mapperService, CARDINALITY).entrySet()) {
            if (entry.getValue() instanceof String hint) {
                result.put(entry.getKey(), hint);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns the fields of the mapping that declare a {@value #CODEC}, with the parsed codec. Reads the value every
     * data format sees, so a plugin does not need its own mapping traversal.
     */
    public static Map<String, FieldCodec> codecs(MapperService mapperService) {
        Map<String, FieldCodec> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : parameterValues(mapperService, CODEC).entrySet()) {
            if (entry.getValue() instanceof FieldCodec codec) {
                result.put(entry.getKey(), codec);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /** Returns the fields of the mapping whose {@value #BLOOM_FILTER} parameter is {@code true}. */
    public static Set<String> bloomFilterFields(MapperService mapperService) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Object> entry : parameterValues(mapperService, BLOOM_FILTER).entrySet()) {
            if (Boolean.TRUE.equals(entry.getValue())) {
                result.add(entry.getKey());
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * Returns field name to resolved value for every field exposing the given plugin-contributed parameter.
     * Linear in the number of fields; intended for cold paths (index creation, settings sync).
     */
    public static Map<String, Object> parameterValues(MapperService mapperService, String parameterName) {
        if (mapperService == null || mapperService.documentMapper() == null || mapperService.documentMapper().mappers() == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new HashMap<>();
        for (Mapper mapper : mapperService.documentMapper().mappers()) {
            if (mapper instanceof ParametrizedFieldMapper parametrized) {
                Object value = parametrized.mappingPluginParameterValues().get(parameterName);
                if (value != null) {
                    result.put(mapper.name(), value);
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
