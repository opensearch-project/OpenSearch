/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * A synthetic {@link MappedFieldType} for the {@code _primary_term} metadata field,
 * used by {@link org.opensearch.index.engine.DataFormatAwareEngine} to pass the primary
 * term value to pluggable data format document inputs during indexing.
 * <p>
 * This field type is not searchable, stored, or doc-valued — it exists solely as a
 * type-safe carrier for the field name and type name so that
 * {@link org.opensearch.index.engine.dataformat.DocumentInput#addField} can route the
 * primary term to the appropriate format-specific handler.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrimaryTermFieldType extends MappedFieldType {

    public static PrimaryTermFieldType INSTANCE = new PrimaryTermFieldType();

    private PrimaryTermFieldType() {
        super(SeqNoFieldMapper.PRIMARY_TERM_NAME, false, false, false, TextSearchInfo.NONE, Map.of());
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String typeName() {
        return SeqNoFieldMapper.PRIMARY_TERM_NAME;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new UnsupportedOperationException();
    }
}
