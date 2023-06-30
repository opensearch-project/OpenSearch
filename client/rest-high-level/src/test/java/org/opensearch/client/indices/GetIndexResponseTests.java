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

package org.opensearch.client.indices;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.client.AbstractResponseTestCase;
import org.opensearch.client.GetAliasesResponseTests;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.RandomCreateIndexGenerator;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class GetIndexResponseTests extends AbstractResponseTestCase<
    org.opensearch.action.admin.indices.get.GetIndexResponse,
    GetIndexResponse> {

    @Override
    protected org.opensearch.action.admin.indices.get.GetIndexResponse createServerTestInstance(XContentType xContentType) {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        final Map<String, MappingMetadata> mappings = new HashMap<>();
        final Map<String, List<AliasMetadata>> aliases = new HashMap<>();
        final Map<String, Settings> settings = new HashMap<>();
        final Map<String, Settings> defaultSettings = new HashMap<>();
        final Map<String, String> dataStreams = new HashMap<>();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index : indices) {
            mappings.put(index, createMappingsForIndex());

            List<AliasMetadata> aliasMetadataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i = 0; i < aliasesNum; i++) {
                aliasMetadataList.add(GetAliasesResponseTests.createAliasMetadata());
            }
            CollectionUtil.timSort(aliasMetadataList, Comparator.comparing(AliasMetadata::alias));
            aliases.put(index, Collections.unmodifiableList(aliasMetadataList));

            Settings.Builder builder = Settings.builder();
            builder.put(RandomCreateIndexGenerator.randomIndexSettings());
            settings.put(index, builder.build());

            if (includeDefaults) {
                defaultSettings.put(index, indexScopedSettings.diff(settings.get(index), Settings.EMPTY));
            }

            if (randomBoolean()) {
                dataStreams.put(index, randomAlphaOfLength(5).toLowerCase(Locale.ROOT));
            }
        }
        return new org.opensearch.action.admin.indices.get.GetIndexResponse(
            indices,
            mappings,
            aliases,
            settings,
            defaultSettings,
            dataStreams
        );
    }

    @Override
    protected GetIndexResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetIndexResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.opensearch.action.admin.indices.get.GetIndexResponse serverTestInstance,
        GetIndexResponse clientInstance
    ) {
        assertArrayEquals(serverTestInstance.getIndices(), clientInstance.getIndices());
        assertEquals(serverTestInstance.getMappings(), clientInstance.getMappings());
        assertEquals(serverTestInstance.getSettings(), clientInstance.getSettings());
        assertEquals(serverTestInstance.defaultSettings(), clientInstance.getDefaultSettings());
        assertEquals(serverTestInstance.getAliases(), clientInstance.getAliases());
    }

    private static MappingMetadata createMappingsForIndex() {
        int typeCount = rarely() ? 0 : 1;
        MappingMetadata mmd = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap());
        for (int i = 0; i < typeCount; i++) {
            if (rarely() == false) { // rarely have no fields
                Map<String, Object> mappings = new HashMap<>();
                mappings.put("field-" + i, randomFieldMapping());
                if (randomBoolean()) {
                    mappings.put("field2-" + i, randomFieldMapping());
                }

                String typeName = MapperService.SINGLE_MAPPING_NAME;
                mmd = new MappingMetadata(typeName, mappings);
            }
        }
        return mmd;
    }

    // Not meant to be exhaustive
    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomBoolean() ? "text" : "keyword");
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else if (randomBoolean()) {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        } else if (randomBoolean()) {
            mappings.put("type", "object");
            mappings.put("dynamic", "strict");
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> props1 = new HashMap<>();
            props1.put("type", randomFrom("text", "keyword"));
            props1.put("analyzer", "keyword");
            properties.put("subtext", props1);
            Map<String, Object> props2 = new HashMap<>();
            props2.put("type", "object");
            Map<String, Object> prop2properties = new HashMap<>();
            Map<String, Object> props3 = new HashMap<>();
            props3.put("type", "integer");
            props3.put("index", "false");
            prop2properties.put("subsubfield", props3);
            props2.put("properties", prop2properties);
            mappings.put("properties", properties);
        } else {
            mappings.put("type", "keyword");
        }
        return mappings;
    }
}
