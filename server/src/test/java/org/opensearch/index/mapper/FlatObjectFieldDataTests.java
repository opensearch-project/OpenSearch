/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fielddata.AbstractFieldDataTestCase;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;

import java.util.List;

public class FlatObjectFieldDataTests extends AbstractFieldDataTestCase {
    private String FIELD_TYPE = "flat_object";

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    public void testDocValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("test")
            .startObject("properties")
            .startObject("field")
            .field("type", FIELD_TYPE)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        final DocumentMapper mapper = mapperService.documentMapperParser().parse("test", new CompressedXContent(mapping));

        XContentBuilder json = XContentFactory.jsonBuilder().startObject().startObject("field").field("foo", "bar").endObject().endObject();
        ParsedDocument d = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(json), MediaTypeRegistry.JSON));
        writer.addDocument(d.rootDoc());
        writer.commit();

        IndexFieldData<?> fieldData = getForField("field");
        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());

        IndexFieldData<?> valueFieldData = getForField("field._value");
        List<LeafReaderContext> valueReaders = refreshReader();
        assertEquals(1, valueReaders.size());
    }

    public void testLongFieldNameWithHashArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("test")
            .startObject("properties")
            .startObject("field")
            .field("type", FIELD_TYPE)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        final DocumentMapper mapper = mapperService.documentMapperParser().parse("test", new CompressedXContent(mapping));

        XContentBuilder json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .startObject("detail")
            .startArray("fooooooooooo")
            .startObject()
            .field("name", "baz")
            .endObject()
            .startObject()
            .field("name", "baz")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        ParsedDocument d = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(json), MediaTypeRegistry.JSON));
        writer.addDocument(d.rootDoc());
        writer.commit();

        IndexFieldData<?> fieldData = getForField("field");
        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());

        IndexFieldData<?> valueFieldData = getForField("field._value");
        List<LeafReaderContext> valueReaders = refreshReader();
        assertEquals(1, valueReaders.size());
    }

    public void testSubfieldDocValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("test")
            .startObject("properties")
            .startObject("field")
            .field("type", FIELD_TYPE)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        final DocumentMapper mapper = mapperService.documentMapperParser().parse("test", new CompressedXContent(mapping));

        XContentBuilder json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .startObject("detail")
            .field("name", "foo")
            .field("age", 25)
            .endObject()
            .field("other", "bar")
            .endObject()
            .endObject();

        ParsedDocument d = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(json), MediaTypeRegistry.JSON));
        writer.addDocument(d.rootDoc());
        writer.commit();

        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());

        IndexFieldData<?> detailNameFieldData = getForField("field.detail.name");
        LeafOrdinalsFieldData detailNameLeafData = (LeafOrdinalsFieldData) detailNameFieldData.load(readers.get(0));

        ScriptDocValues<?> scriptValues = detailNameLeafData.getScriptValues();
        scriptValues.setNextDocId(0);

        assertEquals(1, scriptValues.size());
        assertEquals("foo", scriptValues.get(0));

        IndexFieldData<?> detailAgeFieldData = getForField("field.detail.age");
        LeafOrdinalsFieldData detailAgeLeafData = (LeafOrdinalsFieldData) detailAgeFieldData.load(readers.get(0));

        scriptValues = detailAgeLeafData.getScriptValues();
        scriptValues.setNextDocId(0);

        assertEquals(1, scriptValues.size());
        assertEquals("25", scriptValues.get(0));
    }

    @Override
    protected String getFieldDataType() {
        return FIELD_TYPE;
    }
}
