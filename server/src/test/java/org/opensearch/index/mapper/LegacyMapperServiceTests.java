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

import org.opensearch.LegacyESVersion;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.IndexService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

public class LegacyMapperServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexMetadataUpdateDoesNotLoseDefaultMapper() throws IOException {
        final IndexService indexService =
                createIndex("test", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0).build());
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    builder.field("date_detection", false);
                }
                builder.endObject();
            }
            builder.endObject();
            final PutMappingRequest putMappingRequest = new PutMappingRequest();
            putMappingRequest.indices("test");
            putMappingRequest.type(MapperService.DEFAULT_MAPPING);
            putMappingRequest.source(builder);
            client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(builder).get();
        }
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
        final Settings zeroReplicasSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(zeroReplicasSettings).get();
        /*
         * This assertion is a guard against a previous bug that would lose the default mapper when applying a metadata update that did not
         * update the default mapping.
         */
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
    }

    public void testDefaultMappingIsDeprecatedOn6() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0).build();
        final String mapping;
        try (XContentBuilder defaultMapping = XContentFactory.jsonBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject("_default_");
                {

                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            mapping = Strings.toString(defaultMapping);
        }
        final MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge("_default_", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertWarnings(MapperService.DEFAULT_MAPPING_ERROR_MESSAGE);
    }

}
