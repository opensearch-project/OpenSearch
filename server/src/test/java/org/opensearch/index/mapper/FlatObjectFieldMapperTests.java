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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

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
        assertEquals(orig.toString(), parsedFromOrig.toString());
        assertParseMaximalWarnings();
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .toString();

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
        ParsedDocument parsedDocument = mapper.parse(source(b -> b.nullField("field")));
        assertEquals(1, parsedDocument.docs().size());
        IndexableField[] fields = parsedDocument.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        ParsedDocument doc;
        String json;
        IndexableField[] fieldValues;
        IndexableField[] fieldValueAndPaths;

        {
            // test1: {"field":null}
            doc = mapper.parse(source(b -> b.nullField("field")));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test2: {"field":{"age":3, "name": null}}
            json = "{\"field\":{\"age\":3, \"name\": null}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=3"), fieldValueAndPaths[0].binaryValue());

            // test3: {"field":{"name":null, "age":"5", "name1":null}}
            json = "{\"field\":{\"name\":null, \"age\":\"5\", \"name1\":null}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("5"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=5"), fieldValueAndPaths[0].binaryValue());

            // test4: {"field":{"name": {"name1": {"name2":null}}}}
            json = "{\"field\":{\"name\": {\"name1\": {\"name2\":null}}}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

        }

        {
            // test5: {"field":[null]}
            doc = mapper.parse(source(b -> b.array("field", (String[]) null)));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test6: {"field":{"labels": [null]}}
            json = "{\"field\":{\"labels\": [null]}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test7: {"field":{"r1": {"labels": [null]}}}
            json = "{\"field\":{\"r1\": {\"labels\": [null]}}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test8: {"field":{"r1": {"name": null,"labels": [null]}}}
            json = "{\"field\":{\"r1\": {\"name\": null,\"labels\": [null]}}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test9: {"field":{"name": [null,3],"age":4}}
            json = "{\"field\":{\"name\": [null,3],\"age\":4}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.age"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("4"), fieldValues[2].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.name=3"), fieldValueAndPaths[2].binaryValue());

            // test10: {"field":{"age": 4,"name": [null,"3"]}}
            json = "{\"field\":{\"age\": 4,\"name\": [null,\"3\"]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.age"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("4"), fieldValues[2].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.name=3"), fieldValueAndPaths[2].binaryValue());

            // test11: {"field":{"age":"4","labels": [null]}}
            json = "{\"field\":{\"age\":\"4\",\"labels\": [null]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("4"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());

            // test12: {"field":{"labels": [null], "age":"4"}}
            json = "{\"field\":{\"labels\": [null], \"age\":\"4\"}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("4"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());

            // test13: {"field":{"name": [null, {"d":{"name":"dsds"}}]}}
            json = "{\"field\":{\"name\": [null, {\"d\":{\"name\":\"dsds\"}}]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.d"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsds"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[0].binaryValue());

            // test14: {"field":{"name": [{"d":{"name":"dsds"}}, null]}}
            json = "{\"field\":{\"name\": [{\"d\":{\"name\":\"dsds\"}}, null]}}";
            doc = mapper.parse(source(json));
            IndexableField[] fields1 = doc.rootDoc().getFields("field");
            assertEquals(fields1.length, fields.length);
            for (int i = 0; i < fields1.length; i++) {
                assertEquals(fields[i].toString(), fields1[i].toString());
            }
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.d"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsds"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[0].binaryValue());

            // test15: {"field":{"name": [{"name":"age1"}, null, {"d":{"name":"dsds"}}]}}
            json = "{\"field\":{\"name\": [{\"name\":\"age1\"}, null, {\"d\":{\"name\":\"dsds\"}}]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.d"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("dsds"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("age1"), fieldValues[2].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.name=age1"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[2].binaryValue());

            // test16: {"field":{"name": {"name1": [null,"dsdsdsd"]}}}
            json = "{\"field\":{\"name\": {\"name1\": [null,\"dsdsdsd\"]}}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name1"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsdsdsd"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.name1=dsdsdsd"), fieldValueAndPaths[0].binaryValue());

            // test17: {"field":{"name": {"name1": [[],["dsdsdsd", null]]}}}
            json = "{\"field\":{\"name\": {\"name1\": [[],[\"dsdsdsd\", null]]}}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name1"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsdsdsd"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.name1=dsdsdsd"), fieldValueAndPaths[0].binaryValue());
        }
    }

    public void testInfiniteLoopWithNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        // test2: {"field":{"name": null,"age":3}}
        String json = "{\"field\":{\"name\": null,\"age\":3}}";
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValueAndPaths.length);
        assertEquals(new BytesRef("field.age=3"), fieldValueAndPaths[0].binaryValue());
    }

    // test deduplicationValue of keyList, valueList, valueAndPathList
    public void testDeduplicationValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // test: {"field":{"age": 3,"labels": [null,"3"], "abc":{"abc":{"labels":"n"}}}}
        String json = "{\"field\":{\"age\": 3,\"labels\": [null,\"3\"], \"abc\":{\"abc\":{\"labels\":\"n\"}}}}";
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(6, fields.length);
        assertEquals(new BytesRef("field.abc"), fields[0].binaryValue());
        assertEquals(new BytesRef("field.age"), fields[2].binaryValue());
        assertEquals(new BytesRef("field.labels"), fields[4].binaryValue());
        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(4, fieldValues.length);
        assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
        assertEquals(new BytesRef("n"), fieldValues[2].binaryValue());
        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(6, fieldValueAndPaths.length);
        assertEquals(new BytesRef("field.abc.abc.labels=n"), fieldValueAndPaths[0].binaryValue());
        assertEquals(new BytesRef("field.age=3"), fieldValueAndPaths[2].binaryValue());
        assertEquals(new BytesRef("field.labels=3"), fieldValueAndPaths[4].binaryValue());
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // In the future we will want to make sure parameter updates are covered.
    }

}
