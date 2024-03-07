/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.script.Script;

import java.io.IOException;

public class DerivedFieldMapperTests extends MapperTestCase {

    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    // Overriding fieldMapping to make it create derived mappings by default.
    // This way, the parent tests are checking the right behavior for this Mapper.
    @Override
    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return fieldMapping(buildField, true);
    }

    // Overriding this to do nothing since derived fields are not written in the documents
    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {}

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "keyword");
    }

    protected void registerParameters(ParameterChecker checker) throws IOException {
        // TODO Any conflicts or updates to check for here? Parameters index, store, doc_values and boost are not
        //  supported for DerivedFieldMapper (we explicitly set these values on initialization)
    }

    // TESTCASE: testDefaults() (no script provided and assuming field is not in the source either)
    //  TODO: Might not need this (no default that really deviates it from testSerialization()

    public void testSerialization() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        assertTrue(mapper instanceof DerivedFieldMapper);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"keyword\"}}", builder.toString());
    }

    public void testParsesScript() throws IOException {

        String scriptString = "{\"source\": \"doc['test'].value\"}";
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("script", scriptString);
        }));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        assertTrue(mapper instanceof DerivedFieldMapper);
        DerivedFieldMapper derivedFieldMapper = (DerivedFieldMapper) mapper;
        assertEquals(Script.parse(scriptString), derivedFieldMapper.getScript());

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"type\": \"keyword\",\"script\": {\"source\": \"emit(doc['test'].value)\"}}", builder.toString());
    }

    //  TODO: Is there a case where we want to allow the field to be defined in both 'derived' and 'properties'?
    //    If the user wants to move a field they were testing as derived to be indexed they should be able to update
    //    the mappings in index template to move the field from 'derived' to 'properties' and it should take affect
    //    during the next index rollover (so even in this case, the field is only defined in one or the other).
    public void testFieldInDerivedAndProperties() throws IOException {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(topMapping(b -> {
                b.startObject("derived");
                b.startObject("field");
                b.field("type", "keyword");
                b.endObject();
                b.endObject();
                b.startObject("properties");
                b.startObject("field");
                b.field("type", "keyword");
                b.endObject();
                b.endObject();
            }))
        );
        assertEquals(
            "Field [field] is defined more than once",
            ex.getMessage()
        );
    }

    // TODO TESTCASE: testWithFieldInSource() (derived field with that field present in source)
    //  This is more checking search behavior so may need to revisit this after query implementation

}
