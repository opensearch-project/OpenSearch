/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.junit.Before;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.index.IndexService;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;

public class ConstantKeywordFieldMapperTests extends OpenSearchSingleNodeTestCase {

    private IndexService indexService;
    private DocumentMapperParser parser;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testDefaultDisabledIndexMapper() throws Exception {


        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "constant_keyword")
            .field("value", "default_value").endObject()
            .startObject("field2")
            .field("type", "keyword").endObject();
        mapping = mapping.endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping.toString()));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> {
                b.field("field", "sdf");
                b.field("field2", "szdfvsddf");
            }))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [constant_keyword] in document with id '1'. Preview of field's value: 'sdf'"));

        final ParsedDocument doc = mapper.parse(source(b -> {
            b.field("field", "default_value");
            b.field("field2", "field_2_value");
        }));

        final IndexableField field = doc.rootDoc().getField("field");

        // constantKeywordField should not be stored
        assertNull(field);
    }

    private final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", "1", BytesReference.bytes(builder), MediaTypeRegistry.JSON);
    }
}
