/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.apache.lucene.index.IndexableField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ExtraFieldValuesIngestTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new ExtraFieldValuesMapperPlugin());
    }

    public void testExtraFieldValues_bytes_rootField() throws Exception {
        DocumentMapper dm = createDocumentMapper(fieldMapping(b -> b.field("type", ExtraFieldValuesMapperPlugin.EXTRA_FIELDS_TEST)));

        // _source does NOT contain "field"
        var source = source(b -> b.field("other", "x"));

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("field", new BytesValue(new BytesArray(new byte[] { 1, 2, 3, 4 }))));

        SourceToParse stp = new SourceToParse(
            "test",
            "1",
            source.source(),
            MediaType.fromMediaType(source.getMediaType().mediaType()),
            null,
            efv
        );

        ParsedDocument doc = dm.parse(stp);

        assertThat(doc.rootDoc().getField("field_type").binaryValue().utf8ToString(), is("BYTES"));

        IndexableField f = doc.rootDoc().getField("field");
        assertThat(f, notNullValue());
        assertThat(f.binaryValue().length, is(4));

        assertThat(doc.rootDoc().getField("field_len").numericValue().intValue(), is(4));
    }

    public void testExtraFieldValues_floatArray_nestedObjectPath() throws Exception {
        DocumentMapper dm = createDocumentMapper(mapping(b -> {
            b.startObject("obj");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("vec");
                    {
                        b.field("type", ExtraFieldValuesMapperPlugin.EXTRA_FIELDS_TEST);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        var source = source(b -> b.field("other", "x"));

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("obj.vec", new PrimitiveFloatArray(new float[] { 10.5f, 20.25f })));

        SourceToParse stp = new SourceToParse(
            "test",
            "1",
            source.source(),
            MediaType.fromMediaType(source.getMediaType().mediaType()),
            null,
            efv
        );

        ParsedDocument doc = dm.parse(stp);

        assertThat(doc.rootDoc().getField("obj.vec_type").binaryValue().utf8ToString(), is("FLOAT_ARRAY"));
        assertThat(doc.rootDoc().getField("obj.vec_dim").numericValue().intValue(), is(2));
        assertThat(doc.rootDoc().getField("obj.vec_f0").numericValue().floatValue(), is(10.5f));
    }

    public void testExtraFieldValues_throwsIfNoMapper() throws Exception {
        DocumentMapper dm = createDocumentMapper(mapping(b -> {
            b.startObject("obj");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("x");
                    {
                        b.field("type", "keyword");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        var source = source(b -> b.field("other", "x"));

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("wrong_field", new BytesValue(new BytesArray(new byte[] { 1 }))));

        SourceToParse stp = new SourceToParse(
            "test",
            "1",
            source.source(),
            MediaType.fromMediaType(source.getMediaType().mediaType()),
            null,
            efv
        );

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> dm.parse(stp));
        assertThat(e.getMessage(), containsString("No mapper found for extra field [wrong_field]"));
    }
}
