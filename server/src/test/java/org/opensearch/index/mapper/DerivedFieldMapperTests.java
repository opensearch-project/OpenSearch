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

    protected boolean supportsMeta() {
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
        // supported for DerivedFieldMapper (we explicitly set these values on initialization)
    }

    // TODO: Can update this once the query implementation is completed
    // This is also being left blank because the super assertExistsQuery is trying to parse
    // an empty source and fails.
    @Override
    protected void assertExistsQuery(MapperService mapperService) {}

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

        String scriptString = "doc['test'].value";
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("script");
            b.field("source", scriptString);
            b.endObject();
        }));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        assertTrue(mapper instanceof DerivedFieldMapper);
        DerivedFieldMapper derivedFieldMapper = (DerivedFieldMapper) mapper;
        assertEquals(Script.parse(scriptString), derivedFieldMapper.getScript());

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"field\":{\"type\":\"keyword\",\"script\":{\"source\":\"doc['test'].value\",\"lang\":\"painless\"}}}",
            builder.toString()
        );
    }

    // TODO: Is there a case where we want to allow the field to be defined in both 'derived' and 'properties'?
    // If the user wants to move a field they were testing as derived to be indexed, they should be able to update
    // the mappings in index template to move the field from 'derived' to 'properties' and it should take affect
    // during the next index rollover (so even in this case, the field is only defined in one or the other).
    public void testFieldInDerivedAndProperties() throws IOException {
        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> createDocumentMapper(topMapping(b -> {
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
        })));
        // TODO: Do we want to handle this as a different error? As it stands, it fails as a merge conflict which makes sense.
        // If it didn't fail here, it would hit the MapperParsingException for the field being defined more than once
        // when MappingLookup is initialized
        assertEquals("Failed to parse mapping [_doc]: mapper [field] cannot be changed from type [derived] to [keyword]", ex.getMessage());
    }

    // TODO TESTCASE: testWithFieldInSource() (derived field with that field present in source)
    // This is more checking search behavior so may need to revisit this after query implementation

}
