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

package org.opensearch.action.admin.indices.get;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponseTests;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponseTests;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.RandomCreateIndexGenerator;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Locale;

public class GetIndexResponseTests extends AbstractWireSerializingTestCase<GetIndexResponse> {

    @Override
    protected Writeable.Reader<GetIndexResponse> instanceReader() {
        return GetIndexResponse::new;
    }

    @Override
    protected GetIndexResponse createTestInstance() {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        final Map<String, MappingMetadata> mappings = new HashMap<>();
        final Map<String, List<AliasMetadata>> aliases = new HashMap<>();
        Map<String, Settings> settings = new HashMap<>();
        Map<String, Settings> defaultSettings = new HashMap<>();
        Map<String, String> dataStreams = new HashMap<>();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index : indices) {
            mappings.put(index, GetMappingsResponseTests.createMappingsForIndex());

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
        return new GetIndexResponse(indices, mappings, aliases, settings, defaultSettings, dataStreams);
    }
}
