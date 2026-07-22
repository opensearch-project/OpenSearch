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

package org.opensearch.rest.action.cat;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.pagination.PageToken;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.Table;
import org.opensearch.common.UUIDs;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.SystemIndices;
import org.opensearch.rest.action.list.RestIndicesListAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestIndicesActionTests extends OpenSearchTestCase {

    private static final String SYSTEM_INDEX_NAME = ".system-index";
    private static final String SYSTEM_INDEX_DESCRIPTION = "Example of a system index";

    final Map<String, Settings> indicesSettings = new LinkedHashMap<>();
    final Map<String, IndexMetadata> indicesMetadatas = new LinkedHashMap<>();
    final Map<String, ClusterIndexHealth> indicesHealths = new LinkedHashMap<>();
    final Map<String, IndexStats> indicesStats = new LinkedHashMap<>();

    @Before
    public void setup() {
        final int numIndices = randomIntBetween(3, 20);
        List<String> indexNames = new ArrayList<>();
        indexNames.add(SYSTEM_INDEX_NAME);
        for (int i = 0; i < numIndices; i++) {
            indexNames.add("index-" + i);
        }

        for (String indexName : indexNames) {
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), randomBoolean())
                .build();
            indicesSettings.put(indexName, indexSettings);

            IndexMetadata.State indexState = randomBoolean() ? IndexMetadata.State.OPEN : IndexMetadata.State.CLOSE;
            if (frequently() || SYSTEM_INDEX_NAME.equals(indexName)) {
                ClusterHealthStatus healthStatus = randomFrom(ClusterHealthStatus.values());
                int numberOfShards = randomIntBetween(1, 3);
                int numberOfReplicas = healthStatus == ClusterHealthStatus.YELLOW ? 1 : randomInt(1);
                IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                    .settings(indexSettings)
                    .creationDate(System.currentTimeMillis())
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .state(indexState)
                    .system(SYSTEM_INDEX_NAME.equals(indexName))
                    .build();
                indicesMetadatas.put(indexName, indexMetadata);

                if (frequently()) {
                    Index index = indexMetadata.getIndex();
                    IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
                    switch (randomFrom(ClusterHealthStatus.values())) {
                        case GREEN:
                            IntStream.range(0, numberOfShards)
                                .mapToObj(n -> new ShardId(index, n))
                                .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeA", true, ShardRoutingState.STARTED))
                                .forEach(indexRoutingTable::addShard);
                            if (numberOfReplicas > 0) {
                                IntStream.range(0, numberOfShards)
                                    .mapToObj(n -> new ShardId(index, n))
                                    .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeB", false, ShardRoutingState.STARTED))
                                    .forEach(indexRoutingTable::addShard);
                            }
                            break;
                        case YELLOW:
                            IntStream.range(0, numberOfShards)
                                .mapToObj(n -> new ShardId(index, n))
                                .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeA", true, ShardRoutingState.STARTED))
                                .forEach(indexRoutingTable::addShard);
                            if (numberOfReplicas > 0) {
                                IntStream.range(0, numberOfShards)
                                    .mapToObj(n -> new ShardId(index, n))
                                    .map(shardId -> TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED))
                                    .forEach(indexRoutingTable::addShard);
                            }
                            break;
                        case RED:
                            break;
                    }
                    indicesHealths.put(indexName, new ClusterIndexHealth(indexMetadata, indexRoutingTable.build()));

                    if (frequently()) {
                        IndexStats indexStats = mock(IndexStats.class);
                        when(indexStats.getPrimaries()).thenReturn(new CommonStats());
                        when(indexStats.getTotal()).thenReturn(new CommonStats());
                        indicesStats.put(indexName, indexStats);
                    }
                }
            }
        }
    }

    public void testBuildTable() {
        final RestIndicesAction action = newAction(systemIndices());
        final Table table = action.buildTable(
            new FakeRestRequest(),
            indicesSettings,
            indicesHealths,
            indicesStats,
            indicesMetadatas,
            action.getTableIterator(new String[0], indicesSettings),
            null
        );

        assertNotNull(table);
        assertTableHeaders(table);
        assertThat(table.getRows().size(), equalTo(indicesMetadatas.size()));
        assertTableRows(table);
    }

    public void testBuildPaginatedTable() {
        final RestIndicesAction action = newAction(systemIndices());
        final RestIndicesListAction indicesListAction = new RestIndicesListAction(responseLimitSettings(), systemIndices());
        List<String> indicesList = new ArrayList<>(indicesMetadatas.keySet());
        String[] indicesToBeQueried = indicesList.subList(0, indicesMetadatas.size() / 2).toArray(new String[0]);
        PageToken pageToken = new PageToken("foo", "indices");
        final Table table = action.buildTable(
            new FakeRestRequest(),
            indicesSettings,
            indicesHealths,
            indicesStats,
            indicesMetadatas,
            indicesListAction.getTableIterator(indicesToBeQueried, indicesSettings),
            pageToken
        );

        assertNotNull(table);
        assertTableHeaders(table);
        assertNotNull(table.getPageToken());
        assertEquals(pageToken.getNextToken(), table.getPageToken().getNextToken());
        assertEquals(pageToken.getPaginatedEntity(), table.getPageToken().getPaginatedEntity());

        assertThat(table.getRows().size(), equalTo(indicesMetadatas.size() / 2));
        assertTableRows(table);
    }

    public void testBuildTableWithSystemParam() {
        final RestIndicesAction action = newAction(systemIndices());
        assertTrue(action.responseParams().contains("system"));
        final Table table = action.buildTable(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("system", "true")).build(),
            indicesSettings,
            indicesHealths,
            indicesStats,
            indicesMetadatas,
            action.getTableIterator(new String[0], indicesSettings),
            null
        );

        assertTableHeaders(table);
        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0).get(2).value, equalTo(SYSTEM_INDEX_NAME));
        assertThat(rows.get(0).get(headerIndex(table, "system")).value, equalTo(true));
        assertThat(rows.get(0).get(headerIndex(table, "system.description")).value, equalTo(SYSTEM_INDEX_DESCRIPTION));
    }

    public void testBuildTableWithSystemFalseParam() {
        final RestIndicesAction action = newAction(systemIndices());
        final Table table = action.buildTable(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("system", "false")).build(),
            indicesSettings,
            indicesHealths,
            indicesStats,
            indicesMetadatas,
            action.getTableIterator(new String[0], indicesSettings),
            null
        );

        assertTableHeaders(table);
        final int systemColumn = headerIndex(table, "system");
        final int descriptionColumn = headerIndex(table, "system.description");
        assertThat(table.getRows().size(), equalTo(indicesMetadatas.size() - 1));
        for (List<Table.Cell> row : table.getRows()) {
            assertThat(row.get(systemColumn).value, equalTo(false));
            assertNull(row.get(descriptionColumn).value);
        }
    }

    private void assertTableHeaders(Table table) {
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(0).value, equalTo("health"));
        assertThat(headers.get(1).value, equalTo("status"));
        assertThat(headers.get(2).value, equalTo("index"));
        assertThat(headers.get(3).value, equalTo("uuid"));
        assertThat(headers.get(4).value, equalTo("pri"));
        assertThat(headers.get(5).value, equalTo("rep"));
        assertTrue(headerIndex(table, "system") > 5);
        assertTrue(headerIndex(table, "system.description") > 5);
        boolean foundRaw = false;
        boolean foundString = false;
        for (Table.Cell cell : headers) {
            if ("last_index_request_timestamp".equals(cell.value)) foundRaw = true;
            if ("last_index_request_timestamp_string".equals(cell.value)) foundString = true;
        }
        assertTrue(foundRaw);
        assertTrue(foundString);
    }

    private void assertTableRows(Table table) {
        final List<List<Table.Cell>> rows = table.getRows();

        for (final List<Table.Cell> row : rows) {
            final String indexName = (String) row.get(2).value;

            ClusterIndexHealth indexHealth = indicesHealths.get(indexName);
            IndexStats indexStats = indicesStats.get(indexName);
            IndexMetadata indexMetadata = indicesMetadatas.get(indexName);

            if (indexHealth != null) {
                assertThat(row.get(0).value, equalTo(indexHealth.getStatus().toString().toLowerCase(Locale.ROOT)));
            } else if (indexStats != null) {
                assertThat(row.get(0).value, equalTo("red*"));
            } else {
                assertThat(row.get(0).value, equalTo(""));
            }

            assertThat(row.get(1).value, equalTo(indexMetadata.getState().toString().toLowerCase(Locale.ROOT)));
            assertThat(row.get(2).value, equalTo(indexName));
            assertThat(row.get(3).value, equalTo(indexMetadata.getIndexUUID()));
            if (indexHealth != null) {
                assertThat(row.get(4).value, equalTo(indexMetadata.getNumberOfShards()));
                assertThat(row.get(5).value, equalTo(indexMetadata.getNumberOfReplicas()));
            } else {
                assertThat(row.get(4).value, nullValue());
                assertThat(row.get(5).value, nullValue());
            }
        }
    }

    private int headerIndex(Table table, String name) {
        for (int i = 0; i < table.getHeaders().size(); i++) {
            if (name.equals(table.getHeaders().get(i).value)) {
                return i;
            }
        }
        fail("missing table header [" + name + "]");
        return -1;
    }

    public void testLastIndexRequestTimestampColumns() {
        final RestIndicesAction action = newAction(emptySystemIndices());
        long knownTs = 1710000000000L;
        IndexStats indexStats = mock(IndexStats.class);
        CommonStats commonStats = mock(CommonStats.class);
        org.opensearch.index.shard.IndexingStats indexingStats = mock(org.opensearch.index.shard.IndexingStats.class);
        org.opensearch.index.shard.IndexingStats.Stats stats = mock(org.opensearch.index.shard.IndexingStats.Stats.class);
        when(indexStats.getTotal()).thenReturn(commonStats);
        when(indexStats.getPrimaries()).thenReturn(commonStats);
        when(commonStats.getIndexing()).thenReturn(indexingStats);
        when(indexingStats.getTotal()).thenReturn(stats);
        when(stats.getMaxLastIndexRequestTimestamp()).thenReturn(knownTs);
        Map<String, IndexStats> testStats = new LinkedHashMap<>();
        String testIndex = "test-index";
        testStats.put(testIndex, indexStats);
        Map<String, Settings> testSettings = new LinkedHashMap<>();
        testSettings.put(testIndex, Settings.EMPTY);
        Map<String, IndexMetadata> testMetadatas = new LinkedHashMap<>();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        testMetadatas.put(
            testIndex,
            IndexMetadata.builder(testIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build()
        );
        Map<String, ClusterIndexHealth> testHealths = new LinkedHashMap<>();
        Table table = action.buildTable(
            new FakeRestRequest(),
            testSettings,
            testHealths,
            testStats,
            testMetadatas,
            action.getTableIterator(new String[] { testIndex }, testSettings),
            null
        );

        List<Table.Cell> header = table.getHeaders();
        int rawIdx = -1;
        int strIdx = -1;
        for (int i = 0; i < header.size(); i++) {
            if ("last_index_request_timestamp".equals(header.get(i).value)) rawIdx = i;
            if ("last_index_request_timestamp_string".equals(header.get(i).value)) strIdx = i;
        }
        assertTrue(rawIdx != -1);
        assertTrue(strIdx != -1);
        List<List<Table.Cell>> rows = table.getRows();
        assertEquals(1, rows.size());
        List<Table.Cell> row = rows.get(0);
        assertEquals(String.valueOf(knownTs), row.get(rawIdx).value.toString());
        String timestampString = row.get(strIdx).value.toString();
        try {
            Instant parsed = Instant.parse(timestampString);
            assertEquals(knownTs, parsed.toEpochMilli());
        } catch (DateTimeParseException e) {
            fail("Timestamp string is not a valid ISO-8601 date: " + timestampString);
        }
    }

    private RestIndicesAction newAction(SystemIndices systemIndices) {
        return new RestIndicesAction(responseLimitSettings(), systemIndices);
    }

    private ResponseLimitSettings responseLimitSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ResponseLimitSettings(clusterSettings, Settings.EMPTY);
    }

    private SystemIndices systemIndices() {
        return new SystemIndices(Map.of("testplugin", List.of(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, SYSTEM_INDEX_DESCRIPTION))));
    }

    private SystemIndices emptySystemIndices() {
        return new SystemIndices(Collections.emptyMap());
    }
}
