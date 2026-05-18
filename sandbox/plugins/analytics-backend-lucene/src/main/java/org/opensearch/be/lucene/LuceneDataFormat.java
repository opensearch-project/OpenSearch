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
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.Set;

/**
 * {@link DataFormat} descriptor for Lucene inverted indices.
 * <p>
 * Declares support for {@code text} and {@code keyword} fields with inverted index and
 * stored field capabilities. Used by the composite engine to identify Lucene as a
 * secondary data format alongside Parquet (primary).
 * <p>
 * The priority value ({@code 50}) is lower than the primary Parquet format, ensuring
 * Lucene is treated as a secondary format in the composite engine's format ordering.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDataFormat extends DataFormat {

    /** The format name used to register Lucene in the {@link org.opensearch.index.engine.dataformat.DataFormatRegistry}. */
    public static final String LUCENE_FORMAT_NAME = "lucene";

    private static final Set<FieldTypeCapabilities> SUPPORTED_FIELDS = Set.of(
        new FieldTypeCapabilities(
            "text",
            Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.STORED_FIELDS)
        ),
        new FieldTypeCapabilities(
            "keyword",
            Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
                FieldTypeCapabilities.Capability.STORED_FIELDS)
        ),
        new FieldTypeCapabilities("match_only_text", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(IgnoredFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(IdFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(RoutingFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(SourceFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(SeqNoFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.POINT_RANGE)),
        new FieldTypeCapabilities(IndexFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(NestedPathFieldMapper.NAME, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(VersionFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(DocCountFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities("_feature", Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(FieldNamesFieldMapper.CONTENT_TYPE, Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH))
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

    /** {@inheritDoc} Returns capabilities for {@code text} and {@code keyword} fields. */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return SUPPORTED_FIELDS;
    }
}
