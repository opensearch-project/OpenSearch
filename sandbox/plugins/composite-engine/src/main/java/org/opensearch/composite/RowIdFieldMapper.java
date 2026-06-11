/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.search.Query;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;

/**
 * Metadata field mapper for the internal {@code __row_id__} field.
 * <p>
 * This field is system-managed and assigned by the data format engine during
 * indexing. It cannot be set externally — any attempt to include it in a
 * document body will be rejected by the inherited
 * {@link MetadataFieldMapper#parseCreateField} with a {@code MapperParsingException}.
 * <p>
 * The actual row ID value is written directly into the Arrow vector by the
 * VSR layer, not through Lucene's doc values mechanism. The field type
 * declares {@code hasDocValues=true} as a metadata hint for the data format
 * engine to indicate the field is retrievable.
 *
 * @opensearch.experimental
 */
public class RowIdFieldMapper extends MetadataFieldMapper {

    /** Field name: {@value}. */
    public static final String NAME = DocumentInput.ROW_ID_FIELD;

    /** Content type identifier used for mapper registration. */
    public static final String CONTENT_TYPE = DocumentInput.ROW_ID_FIELD;

    /** Fixed type parser — the row ID mapper has no configurable parameters. */
    public static final TypeParser PARSER = new FixedTypeParser(c -> new RowIdFieldMapper());

    private static final RowIdFieldType ROW_ID_FIELD_TYPE_INSTANCE = new RowIdFieldType();

    /**
     * Private constructor — instances are created exclusively via {@link #PARSER}.
     */
    private RowIdFieldMapper() {
        super(ROW_ID_FIELD_TYPE_INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Field type for the {@code __row_id__} metadata field.
     * <p>
     * Not indexed, not stored, has doc values. Not searchable or queryable —
     * {@link #termQuery} and {@link #valueFetcher} throw
     * {@link UnsupportedOperationException}.
     */
    public static class RowIdFieldType extends MappedFieldType {

        /**
         * Creates the row ID field type: not indexed, not stored, has doc values.
         */
        public RowIdFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("valueFetcher operation is unsupported on: " + NAME);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("termQuery operation is unsupported on: " + NAME);
        }
    }
}
