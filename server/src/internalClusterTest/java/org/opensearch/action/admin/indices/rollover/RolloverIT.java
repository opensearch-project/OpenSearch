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

package org.opensearch.action.admin.indices.rollover;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RolloverIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testRolloverOnEmptyIndex() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-1").addAlias(testAlias).get());
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().containsKey("test_alias"));
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRollover() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias")).get());
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    @SuppressWarnings("unchecked")
    public void testCopyMappingsFromWriteIndex() throws Exception {
        final String aliasName = "copy-mappings-write";
        final String sourceIndexName = "copy-mappings-000001";
        final String sourceOnlyAlias = "source-only-alias";
        assertAcked(
            prepareCreate(sourceIndexName).setSettings(Settings.builder().put("index.refresh_interval", "42s"))
                .addAlias(new Alias(aliasName).writeIndex(true))
                .addAlias(new Alias(sourceOnlyAlias))
                .get()
        );
        index(aliasName, SINGLE_MAPPING_NAME, "1", "dynamic_keyword", "value", "dynamic_long", 1L);

        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex(aliasName)
            .copyMappings(true)
            .simpleMapping("explicit_field", "type=keyword")
            .get();

        assertTrue(response.isRolledOver());
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
        assertThat(properties.keySet(), containsInAnyOrder("dynamic_keyword", "dynamic_long", "explicit_field"));
        assertFalse(targetIndex.getAliases().containsKey(sourceOnlyAlias));
        assertNull(targetIndex.getSettings().get("index.refresh_interval"));
    }

    public void testRequestOverridesDynamicClusterMappingCopySetting() throws Exception {
        final String settingKey = MetadataRolloverService.CLUSTER_ROLLOVER_FORCE_COPY_MAPPINGS_SETTING.getKey();
        final String aliasName = "force-copy-mappings-write";
        assertAcked(prepareCreate("force-copy-mappings-000001").addAlias(new Alias(aliasName).writeIndex(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "dynamic_field", "value");

        try {
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(settingKey, "enabled"))
            );
            final RolloverResponse enabledResponse = client().admin().indices().prepareRolloverIndex(aliasName).get();
            assertRolloverContainsDynamicField(enabledResponse, true);

            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(settingKey, "disabled"))
            );
            final RolloverResponse transientOverrideResponse = client().admin().indices().prepareRolloverIndex(aliasName).get();
            assertRolloverContainsDynamicField(transientOverrideResponse, false);

            index(aliasName, SINGLE_MAPPING_NAME, "2", "dynamic_field", "value");
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull(settingKey)));
            final RolloverResponse optOutResponse = client().admin().indices().prepareRolloverIndex(aliasName).copyMappings(false).get();
            assertRolloverContainsDynamicField(optOutResponse, false);

            index(aliasName, SINGLE_MAPPING_NAME, "3", "dynamic_field", "value");
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(settingKey, "disabled"))
            );
            final RolloverResponse optInResponse = client().admin().indices().prepareRolloverIndex(aliasName).copyMappings(true).get();
            assertRolloverContainsDynamicField(optInResponse, true);
            final RolloverResponse disabledResponse = client().admin().indices().prepareRolloverIndex(aliasName).get();
            assertRolloverContainsDynamicField(disabledResponse, false);

            index(aliasName, SINGLE_MAPPING_NAME, "4", "dynamic_field", "value");
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(settingKey, "auto"))
            );
            assertRolloverContainsDynamicField(client().admin().indices().prepareRolloverIndex(aliasName).get(), false);

            index(aliasName, SINGLE_MAPPING_NAME, "5", "dynamic_field", "value");
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(settingKey, "enabled"))
            );
            assertRolloverContainsDynamicField(client().admin().indices().prepareRolloverIndex(aliasName).get(), true);

            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(settingKey)));
            assertRolloverContainsDynamicField(client().admin().indices().prepareRolloverIndex(aliasName).get(), false);
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull(settingKey))
                    .setTransientSettings(Settings.builder().putNull(settingKey))
            );
        }
    }

    @SuppressWarnings("unchecked")
    private void assertRolloverContainsDynamicField(RolloverResponse response, boolean expectedCopy) {
        assertTrue(response.isRolledOver());
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> properties = targetIndex.mapping() == null
            ? Collections.emptyMap()
            : (Map<String, Object>) targetIndex.mapping().sourceAsMap().getOrDefault("properties", Collections.emptyMap());
        assertEquals(expectedCopy, properties.containsKey("dynamic_field"));
    }

    public void testCopyMappingsPreservesLegacyTemplateRequestPrecedence() throws Exception {
        assertCopyMappingsPreservesTemplateRequestPrecedence(false);
    }

    public void testCopyMappingsPreservesComposableTemplateRequestPrecedence() throws Exception {
        assertCopyMappingsPreservesTemplateRequestPrecedence(true);
    }

    @SuppressWarnings("unchecked")
    public void testExplicitFieldAliasCanReferToCopiedField() throws Exception {
        final String aliasName = "copy-field-alias-write";
        assertAcked(
            prepareCreate("copy-field-alias-000001").setMapping("source_field", "type=keyword")
                .addAlias(new Alias(aliasName).writeIndex(true))
                .get()
        );

        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex(aliasName)
            .copyMappings(true)
            .simpleMapping("source_field_alias", "type=alias,path=source_field")
            .get();

        assertTrue(response.isRolledOver());
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
        assertThat(((Map<String, Object>) properties.get("source_field")).get("type"), equalTo("keyword"));
        assertThat(((Map<String, Object>) properties.get("source_field_alias")).get("type"), equalTo("alias"));
        assertThat(((Map<String, Object>) properties.get("source_field_alias")).get("path"), equalTo("source_field"));
    }

    @SuppressWarnings("unchecked")
    public void testCopyRoutingRequirementWithExplicitNewField() throws Exception {
        final String aliasName = "copy-required-routing-write";
        assertAcked(prepareCreate("copy-required-routing-000001").setMapping("""
            {
              "_routing": { "required": true },
              "properties": { "source_field": { "type": "keyword" } }
            }
            """).addAlias(new Alias(aliasName).writeIndex(true)).get());

        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex(aliasName)
            .copyMappings(true)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, 2)
                    .build()
            )
            .simpleMapping("new_field", "type=long")
            .get();

        assertTrue(response.isRolledOver());
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> mapping = targetIndex.mapping().sourceAsMap();
        assertThat(((Map<String, Object>) mapping.get("_routing")).get("required"), equalTo(true));
        final Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertThat(((Map<String, Object>) properties.get("source_field")).get("type"), equalTo("keyword"));
        assertThat(((Map<String, Object>) properties.get("new_field")).get("type"), equalTo("long"));
        assertThat(targetIndex.getRoutingPartitionSize(), equalTo(2));
    }

    @SuppressWarnings("unchecked")
    private void assertCopyMappingsPreservesTemplateRequestPrecedence(boolean composableTemplate) throws Exception {
        final String templateType = composableTemplate ? "v2" : "v1";
        final String indexPrefix = "copy-request-precedence-" + templateType;
        final String aliasName = indexPrefix + "-write";
        final String sourceIndexName = indexPrefix + "-000001";
        final String templateName = indexPrefix + "-template";
        assertAcked(prepareCreate(sourceIndexName).addAlias(new Alias(aliasName).writeIndex(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "source_only_field", 1L);

        if (composableTemplate) {
            final ComposableIndexTemplate template = new ComposableIndexTemplate(
                Collections.singletonList(indexPrefix + "-*"),
                new Template(null, new CompressedXContent("""
                    {
                      "properties": {
                        "overridden_field": {
                          "type": "keyword"
                        }
                      }
                    }
                    """), null),
                null,
                200L,
                null,
                null,
                null
            );
            assertAcked(
                client().execute(
                    PutComposableIndexTemplateAction.INSTANCE,
                    new PutComposableIndexTemplateAction.Request(templateName).indexTemplate(template)
                ).get()
            );
        } else {
            assertAcked(
                client().admin()
                    .indices()
                    .preparePutTemplate(templateName)
                    .setPatterns(Collections.singletonList(indexPrefix + "-*"))
                    .setMapping("overridden_field", "type=keyword")
                    .get()
            );
        }

        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex(aliasName)
            .copyMappings(true)
            .simpleMapping("overridden_field", "type=text")
            .get();

        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
        assertThat(((Map<String, Object>) properties.get("source_only_field")).get("type"), equalTo("long"));
        assertThat(((Map<String, Object>) properties.get("overridden_field")).get("type"), equalTo("text"));
    }

    @SuppressWarnings("unchecked")
    public void testMappingsAreNotCopiedByDefaultForStandardIndex() throws Exception {
        final String aliasName = "default-mappings-write";
        assertAcked(prepareCreate("default-mappings-000001").addAlias(new Alias(aliasName).writeIndex(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "dynamic_field", "value");

        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();

        assertTrue(response.isRolledOver());
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        if (targetIndex.mapping() != null) {
            final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
            assertTrue(properties == null || properties.containsKey("dynamic_field") == false);
        }
    }

    public void testCopyMappingsRejectsLegacyTemplateConflict() throws Exception {
        final String aliasName = "copy-conflict-write";
        final String sourceIndexName = "copy-conflict-000001";
        assertAcked(prepareCreate(sourceIndexName).addAlias(new Alias(aliasName).writeIndex(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "conflicting_field", 1L);
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("copy-conflict-template")
                .setPatterns(Collections.singletonList("copy-conflict-*"))
                .setMapping("conflicting_field", "type=keyword")
                .get()
        );

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex(aliasName).copyMappings(true).get()
        );
        assertThat(exception.getMessage(), containsString("cannot copy mappings from source index [" + sourceIndexName + "]"));
        assertThat(exception.getMessage(), containsString("rollover index [copy-conflict-000002]"));
        assertThat(exception.getMessage(), containsString("copy-conflict-template"));
    }

    public void testCopyMappingsRejectsExplicitMappingConflict() throws Exception {
        final String aliasName = "explicit-conflict-write";
        final String sourceIndexName = "explicit-conflict-000001";
        assertAcked(prepareCreate(sourceIndexName).addAlias(new Alias(aliasName).writeIndex(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "conflicting_field", 1L);

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareRolloverIndex(aliasName)
                .copyMappings(true)
                .simpleMapping("conflicting_field", "type=keyword")
                .get()
        );
        assertThat(exception.getMessage(), containsString("explicit mappings for rollover index [explicit-conflict-000002]"));
        assertThat(exception.getMessage(), containsString("copied mappings from source index [" + sourceIndexName + "]"));
    }

    public void testRolloverWithExplicitWriteIndex() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias").writeIndex(true)).get());
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    public void testRolloverWithNoWriteIndex() {
        Boolean firstIsWriteIndex = randomFrom(false, null);
        assertAcked(prepareCreate("index1").addAlias(new Alias("alias").writeIndex(firstIsWriteIndex)).get());
        if (firstIsWriteIndex == null) {
            assertAcked(prepareCreate("index2").addAlias(new Alias("alias").writeIndex(randomFrom(false, null))).get());
        }
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("alias").dryRun(randomBoolean()).get()
        );
        assertThat(exception.getMessage(), equalTo("rollover target [alias] does not point to a write index"));
    }

    public void testRolloverWithIndexSettings() throws Exception {

        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(settings)
            .alias(new Alias("extra_alias"))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverWithIndexSettingsBalancedReplica() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        manageReplicaBalanceSetting(true);
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        final IllegalArgumentException restoreError = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("test_alias").settings(settings).alias(new Alias("extra_alias")).get()
        );

        assertThat(
            restoreError.getMessage(),
            containsString("expected total copies needs to be a multiple of total awareness attributes [2]")
        );

        client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
            )
            .alias(new Alias("extra_alias"))
            .waitForActiveShards(0)
            .get();

        client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .build()
            )
            .alias(new Alias("extra_alias"))
            .waitForActiveShards(0)
            .get();

        client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                    .build()
            )
            .alias(new Alias("extra_alias"))
            .waitForActiveShards(0)
            .get();

        final IllegalArgumentException restoreError2 = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-0")
                        .build()
                )
                .alias(new Alias("extra_alias"))
                .get()
        );

        assertThat(
            restoreError2.getMessage(),
            containsString("expected max cap on auto expand to be a multiple of total awareness attributes [2]")
        );

        manageReplicaBalanceSetting(false);
    }

    public void testRolloverWithIndexSettingsBalancedWithUseZoneForReplicaDefaultCount() throws Exception {
        DeleteIndexTemplateRequestBuilder deleteTemplate = client().admin().indices().prepareDeleteTemplate("random_index_template");
        assertAcked(deleteTemplate.execute().actionGet());

        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        manageReplicaSettingForDefaultReplica(true);
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");

        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).build();
        client().admin().indices().prepareRolloverIndex("test_alias").settings(settings).alias(new Alias("extra_alias")).get();

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(3));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(2));
        manageReplicaSettingForDefaultReplica(false);
        randomIndexTemplate();
    }

    public void testRolloverWithIndexSettingsWithoutPrefix() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        index("test_index-2", "_doc", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(settings)
            .alias(new Alias("extra_alias"))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverDryRun() throws Exception {
        if (randomBoolean()) {
            PutIndexTemplateRequestBuilder putTemplate = client().admin()
                .indices()
                .preparePutTemplate("test_index")
                .setPatterns(Collections.singletonList("test_index-*"))
                .setOrder(-1)
                .setSettings(Settings.builder().put(AutoExpandReplicas.SETTING.getKey(), "0-all"));
            assertAcked(putTemplate.get());
        }
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        index("test_index-1", "type1", "1", "field", "value");
        flush("test_index-1");
        ensureGreen();
        Logger allocationServiceLogger = LogManager.getLogger(AllocationService.class);

        final RolloverResponse response;
        try (MockLogAppender appender = MockLogAppender.createForLoggers(allocationServiceLogger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no related message logged on dry run",
                    AllocationService.class.getName(),
                    Level.INFO,
                    "*test_index*"
                )
            );

            response = client().admin().indices().prepareRolloverIndex("test_alias").dryRun(true).get();

            appender.assertAllExpectationsMatched();
        }

        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertNull(newIndex);
    }

    public void testRolloverConditionsNotMet() throws Exception {
        boolean explicitWriteIndex = randomBoolean();
        Alias testAlias = new Alias("test_alias");
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-0").addAlias(testAlias).get());
        index("test_index-0", "type1", "1", "field", "value");
        flush("test_index-0");
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .addMaxIndexSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB))
            .addMaxIndexAgeCondition(TimeValue.timeValueHours(4))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-0"));
        assertThat(response.getNewIndex(), equalTo("test_index-000001"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(2));
        assertThat(response.getConditionStatus().values(), everyItem(is(false)));
        Set<String> conditions = response.getConditionStatus().keySet();
        assertThat(
            conditions,
            containsInAnyOrder(
                new MaxSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB)).toString(),
                new MaxAgeCondition(TimeValue.timeValueHours(4)).toString()
            )
        );

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-0");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertNull(oldIndex.getAliases().get("test_alias").writeIndex());
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000001");
        assertNull(newIndex);
    }

    public void testRolloverWithNewIndexName() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index").addAlias(testAlias).get());
        index("test_index", "type1", "1", "field", "value");
        flush("test_index");
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .setNewIndexName("test_new_index")
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index"));
        assertThat(response.getNewIndex(), equalTo("test_new_index"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index");
        final IndexMetadata newIndex = state.metadata().index("test_new_index");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverOnExistingIndex() throws Exception {
        assertAcked(prepareCreate("test_index-0").addAlias(new Alias("test_alias")).get());
        index("test_index-0", "type1", "1", "field", "value");
        assertAcked(prepareCreate("test_index-000001").get());
        index("test_index-000001", "type1", "1", "field", "value");
        flush("test_index-0", "test_index-000001");
        try {
            client().admin().indices().prepareRolloverIndex("test_alias").get();
            fail("expected failure due to existing rollover index");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getIndex().getName(), equalTo("test_index-000001"));
        }
    }

    public void testRolloverWithDateMath() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        assumeTrue("only works on the same day", now.plusMinutes(5).getDayOfYear() == now.getDayOfYear());
        String index = "test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-1";
        String dateMathExp = "<test-{now/d}-1>";
        assertAcked(prepareCreate(dateMathExp).addAlias(new Alias("test_alias")).get());
        ensureGreen(index);
        // now we modify the provided name such that we can test that the pattern is carried on
        client().admin().indices().prepareClose(index).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(index)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, "<test-{now/M{yyyy.MM}}-1>"))
            .get();

        client().admin().indices().prepareOpen(index).get();
        ensureGreen(index);
        RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo(index));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        GetSettingsResponse getSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(response.getOldIndex(), response.getNewIndex())
            .get();
        assertEquals(
            "<test-{now/M{yyyy.MM}}-000002>",
            getSettingsResponse.getSetting(response.getOldIndex(), IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );
        assertEquals(
            "<test-{now/M{yyyy.MM}}-000003>",
            getSettingsResponse.getSetting(response.getNewIndex(), IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );

        response = client().admin().indices().prepareRolloverIndex("test_alias").setNewIndexName("<test-{now/d}-000004>").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-000004"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
    }

    public void testRolloverMaxSize() throws Exception {
        assertAcked(prepareCreate("test-1").addAlias(new Alias("test_alias")).get());
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            index("test-1", "doc", Integer.toString(i), "field", "foo-" + i);
        }
        flush("test-1");
        refresh("test_alias");

        // A large max_size
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomIntBetween(100, 50 * 1024), ByteSizeUnit.MB))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("No rollover with a large max_size condition", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }

        // A small max_size
        {
            ByteSizeValue maxSizeValue = new ByteSizeValue(randomIntBetween(1, 20), ByteSizeUnit.BYTES);
            long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(maxSizeValue)
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_size condition", response.isRolledOver(), equalTo(true));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            List<Condition<?>> metConditions = oldIndex.getRolloverInfos().get("test_alias").getMetConditions();
            assertThat(metConditions.size(), equalTo(1));
            assertThat(metConditions.get(0).toString(), equalTo(new MaxSizeCondition(maxSizeValue).toString()));
            assertThat(
                oldIndex.getRolloverInfos().get("test_alias").getTime(),
                is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
            );
        }

        // An empty index
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-000002"));
            assertThat(response.getNewIndex(), equalTo("test-000003"));
            assertThat("No rollover with an empty index", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-000002");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }
    }

    public void testRejectIfAliasFoundInTemplate() throws Exception {
        client().admin()
            .indices()
            .preparePutTemplate("logs")
            .setPatterns(Collections.singletonList("logs-*"))
            .addAlias(new Alias("logs-write"))
            .get();
        assertAcked(client().admin().indices().prepareCreate("logs-000001").get());
        ensureYellow("logs-write");
        final IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("logs-write").addMaxIndexSizeCondition(new ByteSizeValue(1)).get()
        );
        assertThat(
            error.getMessage(),
            equalTo(
                "Rollover alias [logs-write] can point to multiple indices, found duplicated alias [[logs-write]] in index template [logs]"
            )
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/64921")
    public void testRolloverWithClosedIndexInAlias() throws Exception {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());

        index(closedIndex, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        index(aliasName, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        index(aliasName, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).get());

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName).addMaxIndexDocsCondition(1).get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithClosedWriteIndex() throws Exception {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());

        index(closedIndex, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        index(aliasName, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        index(aliasName, SINGLE_MAPPING_NAME, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).get());
        assertAcked(client().admin().indices().prepareClose(writeIndexPrefix + "000001").get());
        ensureGreen(aliasName);

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName).addMaxIndexDocsCondition(1).get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithHiddenAliasesAndExplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).writeIndex(true).isHidden(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertTrue(oldIndex.getAliases().containsKey(aliasName));
        assertTrue(oldIndex.getAliases().get(aliasName).isHidden());
        assertFalse(oldIndex.getAliases().get(aliasName).writeIndex());
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertTrue(newIndex.getAliases().get(aliasName).writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    public void testRolloverWithHiddenAliasesAndImplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).isHidden(true)).get());
        index(aliasName, SINGLE_MAPPING_NAME, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertFalse(oldIndex.getAliases().containsKey(aliasName));
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertThat(newIndex.getAliases().get(aliasName).writeIndex(), nullValue());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }
}
