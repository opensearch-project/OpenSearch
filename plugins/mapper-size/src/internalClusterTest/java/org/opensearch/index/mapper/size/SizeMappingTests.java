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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.mapper.size;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.plugin.mapper.MapperSizePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SizeMappingTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperSizePlugin.class, InternalSettingsPlugin.class);
    }

    public void testSizeEnabled() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        boolean stored = false;
        boolean points = false;
        for (IndexableField field : doc.rootDoc().getFields("_size")) {
            stored |= field.fieldType().stored();
            points |= field.fieldType().pointIndexDimensionCount() > 0;
        }
        assertTrue(stored);
        assertTrue(points);
    }

    public void testSizeDisabled() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=false");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY);
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();
        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(true));

        String disabledMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_size")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        docMapper = service.mapperService()
            .merge("type", new CompressedXContent(disabledMapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(false));
    }

}
