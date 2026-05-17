/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;

import java.util.Set;

/**
 * {@link DataFormat} descriptor for Lucene inverted indices.
 * <p>
 * Declares support for all standard OpenSearch field types with their Lucene-native capabilities:
 * <ul>
 *   <li>Text fields: inverted index ({@code FULL_TEXT_SEARCH}) and stored fields</li>
 *   <li>Keyword fields: inverted index, doc values ({@code COLUMNAR_STORAGE}), and stored fields</li>
 *   <li>Numeric fields: BKD tree ({@code POINT_RANGE}), doc values, and stored fields</li>
 *   <li>Date fields: BKD tree, doc values, and stored fields</li>
 *   <li>Boolean fields: inverted index, doc values, and stored fields</li>
 *   <li>IP fields: InetAddressPoint ({@code POINT_RANGE}), doc values, and stored fields</li>
 *   <li>Binary fields: stored fields only</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDataFormat extends DataFormat {

    /** The format name used to register Lucene in the {@link org.opensearch.index.engine.dataformat.DataFormatRegistry}. */
    public static final String LUCENE_FORMAT_NAME = "lucene";

    /** Capabilities common to text-like fields that use inverted index. */
    private static final Set<Capability> TEXT_CAPABILITIES = Set.of(Capability.FULL_TEXT_SEARCH, Capability.STORED_FIELDS);

    /** Capabilities common to keyword fields: inverted index + doc values. */
    private static final Set<Capability> KEYWORD_CAPABILITIES = Set.of(
        Capability.FULL_TEXT_SEARCH,
        Capability.COLUMNAR_STORAGE,
        Capability.STORED_FIELDS
    );

    /** Capabilities common to numeric and date fields: BKD tree + doc values. */
    private static final Set<Capability> NUMERIC_CAPABILITIES = Set.of(
        Capability.POINT_RANGE,
        Capability.COLUMNAR_STORAGE,
        Capability.STORED_FIELDS
    );

    /** Capabilities for boolean fields: inverted index + doc values. */
    private static final Set<Capability> BOOLEAN_CAPABILITIES = Set.of(
        Capability.FULL_TEXT_SEARCH,
        Capability.COLUMNAR_STORAGE,
        Capability.STORED_FIELDS
    );

    /** Capabilities for binary fields: stored only. */
    private static final Set<Capability> BINARY_CAPABILITIES = Set.of(Capability.STORED_FIELDS);

    private static final Set<FieldTypeCapabilities> SUPPORTED_FIELDS = Set.of(
        // Text fields — inverted index for full-text search
        new FieldTypeCapabilities("text", TEXT_CAPABILITIES),
        new FieldTypeCapabilities("match_only_text", Set.of(Capability.FULL_TEXT_SEARCH)),

        // Keyword — inverted index + doc values (sorted set)
        new FieldTypeCapabilities("keyword", KEYWORD_CAPABILITIES),

        // Numeric fields — BKD tree + sorted numeric doc values
        new FieldTypeCapabilities("integer", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("long", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("short", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("byte", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("float", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("half_float", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("double", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("unsigned_long", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("scaled_float", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("token_count", NUMERIC_CAPABILITIES),

        // Date fields — LongPoint + sorted numeric doc values
        new FieldTypeCapabilities("date", NUMERIC_CAPABILITIES),
        new FieldTypeCapabilities("date_nanos", NUMERIC_CAPABILITIES),

        // Boolean — inverted index (term query) + sorted numeric doc values
        new FieldTypeCapabilities("boolean", BOOLEAN_CAPABILITIES),

        // IP — InetAddressPoint + sorted set doc values
        new FieldTypeCapabilities("ip", NUMERIC_CAPABILITIES),

        // Binary — stored fields only
        new FieldTypeCapabilities("binary", BINARY_CAPABILITIES)
    );

    /** {@inheritDoc} Returns {@code "lucene"}. */
    @Override
    public String name() {
        return LUCENE_FORMAT_NAME;
    }

    /** {@inheritDoc} Returns {@code 50}, lower than the primary Parquet format. */
    @Override
    public long priority() {
        return 50L;
    }

    /** {@inheritDoc} Returns capabilities for all standard OpenSearch field types. */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return SUPPORTED_FIELDS;
    }
}
