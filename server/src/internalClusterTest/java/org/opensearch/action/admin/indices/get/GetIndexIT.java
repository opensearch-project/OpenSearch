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

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GetIndexIT extends OpenSearchIntegTestCase {
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addAlias(new Alias("alias_idx")).setSettings(Settings.builder().put("number_of_shards", 1)).get());
        ensureSearchable("idx");
        createIndex("empty_idx");
        ensureSearchable("idx", "empty_idx");
    }

    public void testSimple() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertMappings(response, "idx");
        assertSettings(response, "idx");
    }

    public void testSimpleUnknownIndex() {
        try {
            client().admin().indices().prepareGetIndex().addIndices("missing_idx").get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [missing_idx]"));
        }
    }

    public void testUnknownIndexWithAllowNoIndices() {
        GetIndexResponse response = client().admin()
            .indices()
            .prepareGetIndex()
            .addIndices("missing_idx")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get();
        assertThat(response.indices(), notNullValue());
        assertThat(response.indices().length, equalTo(0));
        assertThat(response.mappings(), notNullValue());
        assertThat(response.mappings().size(), equalTo(0));
    }

    public void testEmpty() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("empty_idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        assertNonEmptySettings(response, "empty_idx");
    }

    public void testSimpleMapping() {
        GetIndexResponse response = runWithRandomFeatureMethod(
            client().admin().indices().prepareGetIndex().addIndices("idx"),
            Feature.MAPPINGS
        );
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertMappings(response, "idx");
        assertEmptyAliases(response);
        assertEmptySettings(response);
    }

    public void testSimpleAlias() {
        GetIndexResponse response = runWithRandomFeatureMethod(
            client().admin().indices().prepareGetIndex().addIndices("idx"),
            Feature.ALIASES
        );
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertEmptyMappings(response);
        assertEmptySettings(response);
    }

    public void testSimpleSettings() {
        GetIndexResponse response = runWithRandomFeatureMethod(
            client().admin().indices().prepareGetIndex().addIndices("idx"),
            Feature.SETTINGS
        );
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertSettings(response, "idx");
        assertEmptyAliases(response);
        assertEmptyMappings(response);
    }

    public void testSimpleMixedFeatures() {
        int numFeatures = randomIntBetween(1, Feature.values().length);
        List<Feature> features = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(
            client().admin().indices().prepareGetIndex().addIndices("idx"),
            features.toArray(new Feature[0])
        );
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        if (features.contains(Feature.ALIASES)) {
            assertAliases(response, "idx");
        } else {
            assertEmptyAliases(response);
        }
        if (features.contains(Feature.MAPPINGS)) {
            assertMappings(response, "idx");
        } else {
            assertEmptyMappings(response);
        }
        if (features.contains(Feature.SETTINGS)) {
            assertSettings(response, "idx");
        } else {
            assertEmptySettings(response);
        }
    }

    public void testEmptyMixedFeatures() {
        int numFeatures = randomIntBetween(1, Feature.values().length);
        List<Feature> features = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(
            client().admin().indices().prepareGetIndex().addIndices("empty_idx"),
            features.toArray(new Feature[0])
        );
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        if (features.contains(Feature.MAPPINGS)) {
            assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        } else {
            assertEmptyMappings(response);
        }
        if (features.contains(Feature.SETTINGS)) {
            assertNonEmptySettings(response, "empty_idx");
        } else {
            assertEmptySettings(response);
        }
    }

    public void testGetIndexWithBlocks() {
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("idx", block);
                GetIndexResponse response = client().admin()
                    .indices()
                    .prepareGetIndex()
                    .addIndices("idx")
                    .addFeatures(Feature.MAPPINGS, Feature.ALIASES)
                    .get();
                String[] indices = response.indices();
                assertThat(indices, notNullValue());
                assertThat(indices.length, equalTo(1));
                assertThat(indices[0], equalTo("idx"));
                assertMappings(response, "idx");
                assertAliases(response, "idx");
            } finally {
                disableIndexBlock("idx", block);
            }
        }

        try {
            enableIndexBlock("idx", SETTING_BLOCKS_METADATA);
            assertBlocked(
                client().admin().indices().prepareGetIndex().addIndices("idx").addFeatures(Feature.MAPPINGS, Feature.ALIASES),
                INDEX_METADATA_BLOCK
            );
        } finally {
            disableIndexBlock("idx", SETTING_BLOCKS_METADATA);
        }
    }

    private GetIndexResponse runWithRandomFeatureMethod(GetIndexRequestBuilder requestBuilder, Feature... features) {
        if (randomBoolean()) {
            return requestBuilder.addFeatures(features).get();
        } else {
            return requestBuilder.setFeatures(features).get();
        }
    }

    private void assertSettings(GetIndexResponse response, String indexName) {
        final Map<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
        assertThat(indexSettings.get("index.number_of_shards"), equalTo("1"));
    }

    private void assertNonEmptySettings(GetIndexResponse response, String indexName) {
        final Map<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
    }

    private void assertMappings(GetIndexResponse response, String indexName) {
        final Map<String, MappingMetadata> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        MappingMetadata indexMappings = mappings.get(indexName);
        assertThat(indexMappings, notNullValue());
    }

    private void assertEmptyOrOnlyDefaultMappings(GetIndexResponse response, String indexName) {
        final Map<String, MappingMetadata> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        MappingMetadata indexMappings = mappings.get(indexName);
        assertEquals(indexMappings, MappingMetadata.EMPTY_MAPPINGS);
    }

    private void assertAliases(GetIndexResponse response, String indexName) {
        final Map<String, List<AliasMetadata>> aliases = response.aliases();
        assertThat(aliases, notNullValue());
        assertThat(aliases.size(), equalTo(1));
        List<AliasMetadata> indexAliases = aliases.get(indexName);
        assertThat(indexAliases, notNullValue());
        assertThat(indexAliases.size(), equalTo(1));
        AliasMetadata alias = indexAliases.get(0);
        assertThat(alias, notNullValue());
        assertThat(alias.alias(), equalTo("alias_idx"));
    }

    private void assertEmptySettings(GetIndexResponse response) {
        assertThat(response.settings(), notNullValue());
        assertThat(response.settings().isEmpty(), equalTo(true));
    }

    private void assertEmptyMappings(GetIndexResponse response) {
        assertThat(response.mappings(), notNullValue());
        assertThat(response.mappings().isEmpty(), equalTo(true));
    }

    private void assertEmptyAliases(GetIndexResponse response) {
        assertThat(response.aliases(), notNullValue());
        for (final List<AliasMetadata> entry : response.getAliases().values()) {
            assertTrue(entry.isEmpty());
        }
    }
}
