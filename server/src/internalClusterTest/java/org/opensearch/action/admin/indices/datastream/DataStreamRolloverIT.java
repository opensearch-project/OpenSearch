/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.rollover.MetadataRolloverService;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamRolloverIT extends DataStreamTestCase {

    public void testDataStreamRollover() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");

        DataStream dataStream;
        GetDataStreamAction.Response.DataStreamInfo dataStreamInfo;
        GetDataStreamAction.Response response;

        // Data stream before a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001")
        );

        // Perform a rollover.
        RolloverResponse rolloverResponse = rolloverDataStream("logs-demo");
        assertThat(rolloverResponse.getOldIndex(), equalTo(".ds-logs-demo-000001"));
        assertThat(rolloverResponse.getNewIndex(), equalTo(".ds-logs-demo-000002"));

        // Data stream after a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(2L));
        assertThat(dataStream.getIndices().size(), equalTo(2));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001", ".ds-logs-demo-000002")
        );
    }

    public void testDataStreamRolloverCopiesMappings() throws Exception {
        assertDataStreamRolloverMappingCopy(true, true);
    }

    public void testStandardDataStreamDoesNotCopyMappingsByDefault() throws Exception {
        assertDataStreamRolloverMappingCopy(null, false);
    }

    public void testStandardDataStreamCanExplicitlyDisableMappingCopy() throws Exception {
        assertDataStreamRolloverMappingCopy(false, false);
    }

    public void testClusterSettingEnablesDataStreamMappingCopyByDefault() throws Exception {
        assertClusterSettingDataStreamMappingCopy("enabled", null, true);
    }

    public void testDisabledModeSkipsDataStreamMappingCopy() throws Exception {
        assertClusterSettingDataStreamMappingCopy("disabled", null, false);
    }

    public void testAutoModePreservesStandardDataStreamDefault() throws Exception {
        assertClusterSettingDataStreamMappingCopy("auto", null, false);
    }

    public void testDataStreamRequestOptOutOverridesEnabledClusterSetting() throws Exception {
        assertClusterSettingDataStreamMappingCopy("enabled", false, false);
    }

    public void testDataStreamRequestOptInOverridesDisabledClusterSetting() throws Exception {
        assertClusterSettingDataStreamMappingCopy("disabled", true, true);
    }

    private void assertClusterSettingDataStreamMappingCopy(String clusterCopyMappings, Boolean copyMappings, boolean expectedCopy)
        throws Exception {
        final String settingKey = MetadataRolloverService.CLUSTER_ROLLOVER_FORCE_COPY_MAPPINGS_SETTING.getKey();
        try {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(settingKey, clusterCopyMappings))
            );
            assertDataStreamRolloverMappingCopy(copyMappings, expectedCopy);
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(settingKey)));
        }
    }

    private void assertDataStreamRolloverMappingCopy(Boolean copyMappings, boolean expectedCopy) throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");
        client().index(
            new IndexRequest("logs-demo").id("doc-1")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("@timestamp", "2020-12-06T11:04:05.000Z")
                        .field("dynamic_field", "value")
                        .endObject()
                )
        ).get();

        final RolloverResponse response = copyMappings == null
            ? rolloverDataStream("logs-demo")
            : rolloverDataStream("logs-demo", copyMappings);

        assertDataStreamRolloverContainsDynamicField(response, expectedCopy);
    }

    @SuppressWarnings("unchecked")
    private void assertDataStreamRolloverContainsDynamicField(RolloverResponse response, boolean expectedCopy) {
        final IndexMetadata targetIndex = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(response.getNewIndex());
        final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
        assertEquals(expectedCopy, properties.containsKey("dynamic_field"));
    }

    public void testMappingCopyModesApplyAcrossDataStreamRollovers() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");
        final String key = MetadataRolloverService.CLUSTER_ROLLOVER_FORCE_COPY_MAPPINGS_SETTING.getKey();
        try {
            for (String mode : Arrays.asList("enabled", "enabled", "disabled", "auto", "enabled", null)) {
                client().index(
                    new IndexRequest("logs-demo").opType(DocWriteRequest.OpType.CREATE)
                        .source(Map.of("@timestamp", "2020-12-06T11:04:05.000Z", "dynamic_field", "value"))
                ).get();
                final Settings.Builder settings = Settings.builder();
                if (mode == null) {
                    settings.putNull(key);
                } else {
                    settings.put(key, mode);
                }
                assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
                assertDataStreamRolloverContainsDynamicField(rolloverDataStream("logs-demo"), "enabled".equals(mode));
            }
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(key)));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataStreamRequestOptOutAllowsObjectEnabledTemplateChange() throws Exception {
        final String template = """
            {
              "index_patterns": ["logs-*"],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "attributes": { "type": "object", "enabled": %s }
                  }
                }
              }
            }
            """;
        createIndexTemplate("demo-template", String.format(Locale.ROOT, template, true));
        createDataStream("logs-demo");
        client().index(
            new IndexRequest("logs-demo").id("doc-1")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(Map.of("@timestamp", "2020-12-06T11:04:05.000Z", "attributes", Map.of("key", "value"), "source_only", "value"))
        ).get();
        createIndexTemplate("demo-template", String.format(Locale.ROOT, template, false));

        final String settingKey = MetadataRolloverService.CLUSTER_ROLLOVER_FORCE_COPY_MAPPINGS_SETTING.getKey();
        try {
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(settingKey, "enabled"))
            );
            final ExecutionException exception = expectThrows(ExecutionException.class, () -> rolloverDataStream("logs-demo"));
            assertThat(exception.getMessage(), containsString("cannot copy mappings from source index [.ds-logs-demo-000001]"));
            final Throwable mappingConflict = ExceptionsHelper.unwrap(exception, MapperException.class);
            assertNotNull(mappingConflict);
            assertThat(
                mappingConflict.getMessage(),
                containsString("the [enabled] parameter can't be updated for the object mapping [attributes]")
            );

            final RolloverResponse optOutResponse = rolloverDataStream("logs-demo", false);
            assertEquals(".ds-logs-demo-000002", optOutResponse.getNewIndex());
            final IndexMetadata targetIndex = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index(optOutResponse.getNewIndex());
            final Map<String, Object> properties = (Map<String, Object>) targetIndex.mapping().sourceAsMap().get("properties");
            assertEquals(false, ((Map<String, Object>) properties.get("attributes")).get("enabled"));
            assertFalse(properties.containsKey("source_only"));

            client().index(
                new IndexRequest("logs-demo").id("doc-2")
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("@timestamp", "2020-12-06T11:04:06.000Z", "dynamic_field", "value"))
            ).get();
            final RolloverResponse defaultResponse = rolloverDataStream("logs-demo");
            final IndexMetadata nextIndex = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index(defaultResponse.getNewIndex());
            final Map<String, Object> nextProperties = (Map<String, Object>) nextIndex.mapping().sourceAsMap().get("properties");
            assertTrue(nextProperties.containsKey("dynamic_field"));
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(settingKey)));
        }
    }

    public void testDataStreamRolloverRejectsTemplateConflict() throws Exception {
        createIndexTemplate("demo-template", """
            {
              "index_patterns": ["logs-*"],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" }
                  }
                }
              }
            }
            """);
        createDataStream("logs-demo");
        client().index(
            new IndexRequest("logs-demo").id("doc-1")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("@timestamp", "2020-12-06T11:04:05.000Z")
                        .field("conflicting_field", 1L)
                        .endObject()
                )
        ).get();
        createIndexTemplate("demo-template", """
            {
              "index_patterns": ["logs-*"],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "conflicting_field": { "type": "keyword" }
                  }
                }
              }
            }
            """);

        final RolloverRequest request = new RolloverRequest("logs-demo", null);
        request.copyMappings(true);
        final ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> client().admin().indices().rolloverIndex(request).get()
        );
        assertThat(exception.getMessage(), containsString("cannot copy mappings from source index [.ds-logs-demo-000001]"));
        assertThat(exception.getMessage(), containsString("rollover index [.ds-logs-demo-000002]"));
        assertThat(exception.getMessage(), containsString("demo-template"));
    }

}
