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

package org.opensearch.indices.mapping;

import org.opensearch.action.admin.indices.mapping.get.GetFieldAliasesMappingsResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.not;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;

public class SimpleGetFieldAliasesMappingsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetFieldAliasesMappingsResponse response = client().admin().indices().prepareGetFieldAliasesMappings().get();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("index").size(), equalTo(0));

        assertThat(response.fieldMappings("index", "field"), nullValue());
    }

    /**
     * <pre>
     *     {
     *         "properties": {
     *              "field1":{
     *                  "type":"text"
     *              },
     *              "alias":{
     *                  "type":"alias",
     *                  "path":"field1"
     *              },
     *              "aliasTwo":{
     *                  "type":"alias",
     *                  "path":"field1"
     *              },
     *              "subFieldAlias":{
     *                  "type":"alias",
     *                  "path":"obj.subfield"
     *              },
     *              "obj":{
     *                  "properties":{
     *                      "subfield":{
     *                          "type":"keyword"
     *                      }
     *                  "subFieldInnerAlias":{
     *                      "type":"alias",
     *                      "path":"obj.subfield"
     *                      }
     *                  }
     *              }
     *
     *         }
     *     }
     * </pre>
     *
     * @return
     * @throws IOException
     */
    private XContentBuilder getMappingForType() throws IOException {
        return jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("aliasTwo")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("subFieldAlias")
            .field("type", "alias")
            .field("path", "obj.subfield")
            .endObject()
            .startObject("obj")
            .startObject("properties")
            .startObject("subfield")
            .field("type", "keyword")
            .endObject()
            .startObject("subFieldInnerAlias")
            .field("type", "alias")
            .field("path", "obj.subfield")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    @SuppressWarnings("unchecked")
    public void testSimpleGetFieldAliasesMappingsWithoutAliases() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));
        client().admin().indices().preparePutMapping("test").setSource("num", "type=long").get();
        client().admin().indices().preparePutMapping("test").setSource("field2", "type=text,index=false").get();

        GetFieldAliasesMappingsResponse response = client().admin()
            .indices()
            .prepareGetFieldAliasesMappings()
            .setFields("num", "field2")
            .includeDefaults(true)
            .get();

        assertNull(response.fieldMappings("test", "num"));
        assertNull(response.fieldMappings("test", "field2"));
    }

    @SuppressWarnings("unchecked")
    public void testGetFieldMappingsWithFieldAlias() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));
        GetFieldAliasesMappingsResponse response = client().admin()
            .indices()
            .prepareGetFieldAliasesMappings("test")
            .setFields("field1", "obj.subfield")
            .get();
        assertThat(response.fieldMappings("test", "alias").fullName(), equalTo("alias"));
        assertThat(response.fieldMappings("test", "aliasTwo").fullName(), equalTo("aliasTwo"));
    }

    @SuppressWarnings("unchecked")
    public void testGetFieldInternalObjectMappingsWithFieldAlias() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));
        GetFieldAliasesMappingsResponse response = client().admin()
            .indices()
            .prepareGetFieldAliasesMappings("test")
            .setFields("obj.subfield")
            .get();
        assertThat(response.fieldMappings("test", "subFieldAlias").fullName(), equalTo("subFieldAlias"));
        assertThat(response.fieldMappings("test", "obj.subFieldInnerAlias").fullName(), equalTo("obj.subFieldInnerAlias"));
    }

    // fix #6552
    public void testSimpleGetFieldAliasesMappingsWithPretty() throws Exception {
        assertAcked(prepareCreate("index").setMapping(getMappingForType()));
        Map<String, String> params = new HashMap<>();
        params.put("pretty", "true");
        GetFieldAliasesMappingsResponse response = client().admin()
            .indices()
            .prepareGetFieldAliasesMappings("index")
            .setFields("field1", "obj.subfield")
            .get();
        XContentBuilder responseBuilder = XContentFactory.jsonBuilder().prettyPrint();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        String responseStrings = responseBuilder.toString();

        XContentBuilder prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        prettyJsonBuilder.copyCurrentStructure(createParser(JsonXContent.jsonXContent, responseStrings));
        assertThat(responseStrings, equalTo(prettyJsonBuilder.toString()));

        params.put("pretty", "false");

        response = client().admin().indices().prepareGetFieldAliasesMappings("index").setFields("field1", "obj.subfield").get();
        responseBuilder = XContentFactory.jsonBuilder().prettyPrint().lfAtEnd();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        responseStrings = responseBuilder.toString();

        prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        prettyJsonBuilder.copyCurrentStructure(createParser(JsonXContent.jsonXContent, responseStrings));
        assertThat(responseStrings, not(equalTo(prettyJsonBuilder.toString())));

    }

    public void testGetFieldMappingsWithBlocks() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetFieldAliasesMappingsResponse response = client().admin()
                    .indices()
                    .prepareGetFieldAliasesMappings("test")
                    .setFields("field1", "obj.subfield")
                    .get();
                assertThat(response.fieldMappings("test", "alias").fullName(), equalTo("alias"));
                assertThat(response.fieldMappings("test", "aliasTwo").fullName(), equalTo("aliasTwo"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetMappings(), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
