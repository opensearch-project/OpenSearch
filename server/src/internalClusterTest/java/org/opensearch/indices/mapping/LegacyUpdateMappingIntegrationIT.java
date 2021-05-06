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

import org.opensearch.LegacyESVersion;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class LegacyUpdateMappingIntegrationIT extends OpenSearchIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDefaultMappingSettings() throws Exception {
        logger.info("Creating index with _default_ mappings");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.field("date_detection", false);
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            client()
                    .admin()
                    .indices()
                    .prepareCreate("test")
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0).build())
                    .addMapping(MapperService.DEFAULT_MAPPING, defaultMapping)
                    .get();
        }

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            assertThat(defaultMapping, hasKey("date_detection"));
        }

        logger.info("Emptying _default_ mappings");
        // now remove it
        try (XContentBuilder mappingBuilder =
                     JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING).endObject().endObject()) {
            final AcknowledgedResponse putResponse =
                    client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING)
                            .setSource(mappingBuilder)
                            .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }
        logger.info("Done Emptying _default_ mappings");

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            assertThat(defaultMapping, not(hasKey("date_detection")));
        }

        // now test you can change stuff that are normally unchangeable
        logger.info("Creating _default_ mappings with an analyzed field");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {

            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.startObject("properties");
                    {
                        defaultMapping.startObject("f");
                        {
                            defaultMapping.field("type", "text");
                            defaultMapping.field("index", true);
                        }
                        defaultMapping.endObject();
                    }
                    defaultMapping.endObject();
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();

            final AcknowledgedResponse putResponse =
                    client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING).setSource(defaultMapping)
                            .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }

        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        {
            try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {
                mappingBuilder.startObject();
                {
                    mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                    {
                        mappingBuilder.startObject("properties");
                        {
                            mappingBuilder.startObject("f");
                            {
                                mappingBuilder.field("type", "keyword");
                            }
                            mappingBuilder.endObject();
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();

                final AcknowledgedResponse putResponse =
                        client()
                                .admin()
                                .indices()
                                .preparePutMapping("test")
                                .setType(MapperService.DEFAULT_MAPPING)
                                .setSource(mappingBuilder)
                                .get();
                assertThat(putResponse.isAcknowledged(), equalTo(true));
            }
        }
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            final Map<String, Object> fieldSettings = (Map<String, Object>) ((Map) defaultMapping.get("properties")).get("f");
            assertThat(fieldSettings, hasEntry("type", "keyword"));
        }

        // but we still validate the _default_ type
        logger.info("Confirming _default_ mappings validation");
        try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {

            mappingBuilder.startObject();
            {
                mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    mappingBuilder.startObject("properites");
                    {
                        mappingBuilder.startObject("f");
                        {
                            mappingBuilder.field("type", "non-existent");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();

            expectThrows(
                    MapperParsingException.class,
                    () -> client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING)
                            .setSource(mappingBuilder)
                            .get());
        }

    }

}
