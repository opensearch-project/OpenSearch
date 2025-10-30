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

package org.opensearch.action.admin.indices.resolve;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction.TransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexAbstractionResolver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.core.IsNull.notNullValue;

public class ResolveIndexTests extends OpenSearchTestCase {

    private final Object[][] indices = new Object[][] {
        // name, isClosed, isHidden, isFrozen, dataStream, aliases
        { "logs-pgsql-prod-20200101", false, false, true, null, new String[] { "logs-pgsql-prod" } },
        { "logs-pgsql-prod-20200102", false, false, true, null, new String[] { "logs-pgsql-prod", "one-off-alias" } },
        { "logs-pgsql-prod-20200103", false, false, false, null, new String[] { "logs-pgsql-prod" } },
        { "logs-pgsql-test-20200101", true, false, false, null, new String[] { "logs-pgsql-test" } },
        { "logs-pgsql-test-20200102", false, false, false, null, new String[] { "logs-pgsql-test" } },
        { "logs-pgsql-test-20200103", false, false, false, null, new String[] { "logs-pgsql-test" } } };

    private final Object[][] dataStreams = new Object[][] {
        // name, timestampField, numBackingIndices
        { "logs-mysql-prod", "@timestamp", 4 },
        { "logs-mysql-test", "@timestamp", 2 } };

    private Metadata metadata = buildMetadata(dataStreams, indices);
    private IndexAbstractionResolver resolver = new IndexAbstractionResolver(
        new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
    );

    public void testResolveStarWithDefaultOptions() {
        String[] names = new String[] { "*" };
        IndicesOptions indicesOptions = Request.DEFAULT_INDICES_OPTIONS;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, metadata, resolver, indices, aliases, dataStreams, true);

        validateIndices(
            indices,
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test", "one-off-alias");

        validateDataStreams(dataStreams, "logs-mysql-prod", "logs-mysql-test");
    }

    public void testResolveStarWithAllOptions() {
        String[] names = new String[] { "*" };
        IndicesOptions indicesOptions = IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, metadata, resolver, indices, aliases, dataStreams, true);
        validateIndices(
            indices,
            ".ds-logs-mysql-prod-000001",
            ".ds-logs-mysql-prod-000002",
            ".ds-logs-mysql-prod-000003",
            ".ds-logs-mysql-prod-000004",
            ".ds-logs-mysql-test-000001",
            ".ds-logs-mysql-test-000002",
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200101",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test", "one-off-alias");

        validateDataStreams(dataStreams, "logs-mysql-prod", "logs-mysql-test");
    }

    public void testResolveWithPattern() {
        String[] names = new String[] { "logs-pgsql*" };
        IndicesOptions indicesOptions = Request.DEFAULT_INDICES_OPTIONS;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, metadata, resolver, indices, aliases, dataStreams, true);

        validateIndices(
            indices,
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test");

        validateDataStreams(dataStreams, Strings.EMPTY_ARRAY);
    }

    public void testResolveWithMultipleNames() {
        String[] names = new String[] { ".ds-logs-mysql-prod-000003", "logs-pgsql-test-20200102", "one-off-alias", "logs-mysql-test" };
        IndicesOptions indicesOptions = IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, metadata, resolver, indices, aliases, dataStreams, true);
        validateIndices(indices, ".ds-logs-mysql-prod-000003", "logs-pgsql-test-20200102");
        validateAliases(aliases, "one-off-alias");
        validateDataStreams(dataStreams, "logs-mysql-test");
    }

    public void testResolveHiddenProperlyWithDateMath() {
        // set up with today's index and following day's index to avoid test failures due to execution time
        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu.MM.dd");
        Instant now = Instant.now(Clock.systemUTC());
        String todaySuffix = dateFormatter.format(now);
        String tomorrowSuffix = dateFormatter.format(now.plus(Duration.ofDays(1L)));
        Object[][] indices = new Object[][] {
            // name, isClosed, isHidden, isFrozen, dataStream, aliases
            { "logs-pgsql-prod-" + todaySuffix, false, true, false, null, Strings.EMPTY_ARRAY },
            { "logs-pgsql-prod-" + tomorrowSuffix, false, true, false, null, Strings.EMPTY_ARRAY } };
        Metadata metadata = buildMetadata(new Object[][] {}, indices);

        String requestedIndex = "<logs-pgsql-prod-{now/d}>";
        List<String> resolvedIndices = resolver.resolveIndexAbstractions(
            singletonList(requestedIndex),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            metadata,
            asList("logs-pgsql-prod-" + todaySuffix, "logs-pgsql-prod-" + tomorrowSuffix),
            randomBoolean(),
            randomBoolean(),
            true
        );
        assertThat(resolvedIndices.size(), is(1));
        assertThat(resolvedIndices.get(0), oneOf("logs-pgsql-prod-" + todaySuffix, "logs-pgsql-prod-" + tomorrowSuffix));
    }

    private void validateIndices(List<ResolvedIndex> resolvedIndices, String... expectedIndices) {
        assertThat(resolvedIndices.size(), equalTo(expectedIndices.length));
        for (int k = 0; k < resolvedIndices.size(); k++) {
            ResolvedIndex resolvedIndex = resolvedIndices.get(k);
            Object[] indexInfo = findInfo(indices, expectedIndices[k]);
            if (indexInfo == null) {
                indexInfo = findBackingIndexInfo(dataStreams, expectedIndices[k]);
            }
            assertThat(indexInfo, notNullValue());
            assertThat(resolvedIndex.getName(), equalTo((String) indexInfo[0]));
            assertThat(resolvedIndex.getAliases(), is(((String[]) indexInfo[5])));
            assertThat(resolvedIndex.getAttributes(), is(flagsToAttributes(indexInfo)));
            assertThat(resolvedIndex.getDataStream(), equalTo((String) indexInfo[4]));
        }
    }

    private void validateAliases(List<ResolvedAlias> resolvedAliases, String... expectedAliases) {
        assertThat(resolvedAliases.size(), equalTo(expectedAliases.length));

        Map<String, Set<String>> aliasToIndicesMap = new HashMap<>();
        for (Object[] indexInfo : indices) {
            String[] aliases = (String[]) indexInfo[5];
            for (String alias : aliases) {
                Set<String> indicesSet = aliasToIndicesMap.get(alias);
                if (indicesSet == null) {
                    indicesSet = new HashSet<>();
                    aliasToIndicesMap.put(alias, indicesSet);
                }
                indicesSet.add((String) indexInfo[0]);
            }
        }
        for (int k = 0; k < resolvedAliases.size(); k++) {
            ResolvedAlias resolvedAlias = resolvedAliases.get(k);
            assertThat(resolvedAlias.getName(), equalTo(expectedAliases[k]));
            Set<String> aliasIndices = aliasToIndicesMap.get(resolvedAlias.getName());
            assertThat(aliasIndices, notNullValue());
            String[] expectedIndices = aliasIndices.toArray(Strings.EMPTY_ARRAY);
            Arrays.sort(expectedIndices);
            assertThat(resolvedAlias.getIndices(), is(expectedIndices));
        }
    }

    private void validateDataStreams(List<ResolvedDataStream> resolvedDataStreams, String... expectedDataStreams) {
        assertThat(resolvedDataStreams.size(), equalTo(expectedDataStreams.length));
        for (int k = 0; k < resolvedDataStreams.size(); k++) {
            ResolvedDataStream resolvedDataStream = resolvedDataStreams.get(k);
            Object[] dataStreamInfo = findInfo(dataStreams, expectedDataStreams[k]);
            assertThat(dataStreamInfo, notNullValue());
            assertThat(resolvedDataStream.getName(), equalTo((String) dataStreamInfo[0]));
            assertThat(resolvedDataStream.getTimestampField(), equalTo((String) dataStreamInfo[1]));
            int numBackingIndices = (int) dataStreamInfo[2];
            List<String> expectedBackingIndices = new ArrayList<>();
            for (int m = 1; m <= numBackingIndices; m++) {
                expectedBackingIndices.add(DataStream.getDefaultBackingIndexName(resolvedDataStream.getName(), m));
            }
            assertThat(resolvedDataStream.getBackingIndices(), is((expectedBackingIndices.toArray(Strings.EMPTY_ARRAY))));
        }
    }

    static Metadata buildMetadata(Object[][] dataStreams, Object[][] indices) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Object[] dsInfo : dataStreams) {
            String dataStreamName = (String) dsInfo[0];
            String timestampField = (String) dsInfo[1];
            int numBackingIndices = (int) dsInfo[2];
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
                backingIndices.add(createIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber), true));
            }
            allIndices.addAll(backingIndices);

            DataStream ds = new DataStream(
                dataStreamName,
                createTimestampField(timestampField),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
            );
            builder.put(ds);
        }

        for (Object[] indexInfo : indices) {
            String indexName = (String) indexInfo[0];
            String[] aliases = (String[]) indexInfo[5];
            boolean closed = (boolean) indexInfo[1];
            boolean hidden = (boolean) indexInfo[2];
            boolean frozen = (boolean) indexInfo[3];
            allIndices.add(createIndexMetadata(indexName, aliases, closed, hidden, frozen));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return builder.build();
    }

    private static IndexMetadata createIndexMetadata(String name, String[] aliases, boolean closed, boolean hidden, boolean frozen) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.hidden", hidden)
            .put("index.frozen", frozen);

        IndexMetadata.Builder indexBuilder = IndexMetadata.builder(name)
            .settings(settingsBuilder)
            .state(closed ? IndexMetadata.State.CLOSE : IndexMetadata.State.OPEN)
            .numberOfShards(1)
            .numberOfReplicas(1);

        for (String alias : aliases) {
            indexBuilder.putAlias(AliasMetadata.builder(alias).build());
        }

        return indexBuilder.build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden) {
        return createIndexMetadata(name, Strings.EMPTY_ARRAY, false, true, false);
    }

    private static Object[] findInfo(Object[][] indexSource, String indexName) {
        for (Object[] info : indexSource) {
            if (info[0].equals(indexName)) {
                return info;
            }
        }
        return null;
    }

    private static Object[] findBackingIndexInfo(Object[][] dataStreamSource, String indexName) {
        for (Object[] info : dataStreamSource) {
            String dataStreamName = (String) info[0];
            int generations = (int) info[2];
            for (int k = 1; k <= generations; k++) {
                if (DataStream.getDefaultBackingIndexName(dataStreamName, k).equals(indexName)) {
                    return new Object[] {
                        DataStream.getDefaultBackingIndexName(dataStreamName, k),
                        false,
                        true,
                        false,
                        dataStreamName,
                        Strings.EMPTY_ARRAY };
                }
            }
        }
        return null;
    }

    private static String[] flagsToAttributes(Object[] indexInfo) {
        List<String> attributes = new ArrayList<>();
        attributes.add((boolean) indexInfo[1] ? "closed" : "open");
        if ((boolean) indexInfo[2]) {
            attributes.add("hidden");
        }
        if ((boolean) indexInfo[3]) {
            attributes.add("frozen");
        }
        attributes.sort(String::compareTo);
        return attributes.toArray(Strings.EMPTY_ARRAY);
    }
}
