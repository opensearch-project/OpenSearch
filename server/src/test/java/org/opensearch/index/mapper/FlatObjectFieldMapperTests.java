/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;

public class FlatObjectFieldMapperTests extends MapperTestCase {
    private static final String FIELD_TYPE = "flat_object";
    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";

    protected boolean supportsMeta() {
        return false;
    }

    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    public void testMapperServiceHasParser() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> { minimalMapping(b); }));
        Mapper.TypeParser parser = mapperService.mapperRegistry.getMapperParsers().get(FIELD_TYPE);
        assertNotNull(parser);
        assertTrue(parser instanceof FlatObjectFieldMapper.TypeParser);
    }

    protected void assertExistsQuery(MapperService mapperService) throws IOException {
        ParseContext.Document fields = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        MappedFieldType fieldType = mapperService.fieldType("field");
        Query query = fieldType.existsQuery(queryShardContext);
        assertExistsQuery(fieldType, query, fields);

    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, ParseContext.Document fields) {
        // we always perform a term query against _field_names, even when the field
        // is not added to _field_names because it is not indexed nor stored
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertEquals(FieldNamesFieldMapper.NAME, termQuery.getTerm().field());
        assertEquals("field", termQuery.getTerm().text());
        if (fieldType.isSearchable() || fieldType.isStored()) {
            assertNotNull(fields.getField(FieldNamesFieldMapper.NAME));
        } else {
            assertNoFieldNamesField(fields);
        }
    }

    public void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", FIELD_TYPE);
    }

    /**
     * Writes a sample value for the field to the provided {@link XContentBuilder}.
     * @param builder builder
     */
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("foo", "bar");
        builder.endObject();
    }

    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMaximalWarnings();
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        String json = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject("field").field("foo", "bar").endObject().endObject()
        );

        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("field.foo"), fields[0].binaryValue());

        IndexableFieldType fieldType = fields[0].fieldType();
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        // Test internal substring fields as well
        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertTrue(fieldValues[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertTrue(fieldValueAndPaths[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("field.foo=bar"), fieldValueAndPaths[0].binaryValue());
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.nullField("field"))));
        assertThat(e.getMessage(), containsString("object mapping for [_doc] tried to parse field [field] as object"));

    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // In the future we will want to make sure parameter updates are covered.
    }

}
