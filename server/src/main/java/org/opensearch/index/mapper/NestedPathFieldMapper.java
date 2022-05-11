/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;

/**
 * Replacement for TypesFieldMapper used in nested fields
 *
 * @opensearch.internal
 */
public class NestedPathFieldMapper extends MetadataFieldMapper {
    // OpenSearch version 2.0 removed types; this name is used for bwc
    public static final String LEGACY_NAME = "_type";
    public static final String NAME = "_nested_path";

    /**
     * Default parameters for the field mapper
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    /** private ctor; using SINGLETON to control BWC */
    private NestedPathFieldMapper(String name) {
        super(new NestedPathFieldType(name));
    }

    /** returns the field name */
    public static String name(Version version) {
        if (version.before(Version.V_2_0_0)) {
            return LEGACY_NAME;
        }
        return NAME;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    private static final NestedPathFieldMapper LEGACY_INSTANCE = new NestedPathFieldMapper(LEGACY_NAME);
    private static final NestedPathFieldMapper INSTANCE = new NestedPathFieldMapper(NAME);

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> c.indexVersionCreated().before(Version.V_2_0_0) ? LEGACY_INSTANCE : INSTANCE
    );

    /** helper method to create a lucene field based on the opensearch version */
    public static Field field(Version version, String path) {
        return new Field(name(version), path, Defaults.FIELD_TYPE);
    }

    /** helper method to create a query based on the opensearch version */
    public static Query filter(Version version, String path) {
        return new TermQuery(new Term(name(version), new BytesRef(path)));
    }

    /**
     * field type for the NestPath field
     *
     * @opensearch.internal
     */
    public static final class NestedPathFieldType extends StringFieldType {
        private NestedPathFieldType(String name) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists() query against the nested field path");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }
}
