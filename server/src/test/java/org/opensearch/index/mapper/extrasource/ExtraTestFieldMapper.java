/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TermBasedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExtraTestFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = ExtraFieldValuesMapperPlugin.EXTRA_FIELDS_TEST;

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.emptyList();
        }

        @Override
        public ExtraTestFieldMapper build(BuilderContext context) {
            return new ExtraTestFieldMapper(name, buildFullName(context), multiFieldsBuilder.build(this, context), copyTo.build());
        }
    }

    static class ExtraTestFieldType extends TermBasedFieldType {
        ExtraTestFieldType(String name) {
            super(name, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Map.of());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return null; // not needed for these tests
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private ExtraTestFieldMapper(String simpleName, String fullName, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, new ExtraTestFieldType(fullName), multiFields, copyTo);
    }

    @Override
    public boolean supportsExtraFieldValues() {
        return true;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        ExtraFieldValue v = context.parseExternalValue(ExtraFieldValue.class);
        if (v == null) {
            throw new MapperParsingException("extra_fields_test requires an ExtraFieldValue (external or JSON)");
        }

        context.doc().add(new StoredField(fieldType().name() + "_type", new BytesRef(v.type().name())));

        if (v instanceof BytesValue bv) {
            BytesRef br = bv.bytes().toBytesRef();
            context.doc().add(new StoredField(fieldType().name(), br));
            context.doc().add(new StoredField(fieldType().name() + "_len", br.length));
        } else if (v instanceof FloatArrayValue fav) {
            context.doc().add(new StoredField(fieldType().name() + "_dim", fav.dimension()));
            if (fav.dimension() > 0) {
                context.doc().add(new StoredField(fieldType().name() + "_f0", fav.get(0)));
            }
        } else {
            throw new MapperParsingException("Unsupported ExtraFieldValue impl: " + v.getClass().getName());
        }

    }
}
