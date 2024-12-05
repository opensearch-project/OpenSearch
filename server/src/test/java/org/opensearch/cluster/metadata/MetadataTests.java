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

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.DataStreamTestHelper;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.opensearch.cluster.DataStreamTestHelper.createBackingIndex;
import static org.opensearch.cluster.DataStreamTestHelper.createFirstBackingIndex;
import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.opensearch.cluster.metadata.Metadata.Builder.validateDataStreams;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MetadataTests extends OpenSearchTestCase {

    public void testFindAliases() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias1").build())
                    .putAlias(AliasMetadata.builder("alias2").build())
            )
            .build();

        {
            final Map<String, List<AliasMetadata>> aliases = metadata.findAliases(new GetAliasesRequest(), Strings.EMPTY_ARRAY);
            assertThat(aliases.size(), equalTo(0));
        }
        {
            final GetAliasesRequest request;
            if (randomBoolean()) {
                request = new GetAliasesRequest();
            } else {
                request = new GetAliasesRequest(randomFrom("alias1", "alias2"));
                // replacing with empty aliases behaves as if aliases were unspecified at request building
                request.replaceAliases(Strings.EMPTY_ARRAY);
            }
            final Map<String, List<AliasMetadata>> aliases = metadata.findAliases(new GetAliasesRequest(), new String[] { "index" });
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList.size(), equalTo(2));
            assertThat(aliasMetadataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetadataList.get(1).alias(), equalTo("alias2"));
        }
        {
            final Map<String, List<AliasMetadata>> aliases = metadata.findAliases(
                new GetAliasesRequest("alias*"),
                new String[] { "index" }
            );
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList.size(), equalTo(2));
            assertThat(aliasMetadataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetadataList.get(1).alias(), equalTo("alias2"));
        }
        {
            final Map<String, List<AliasMetadata>> aliases = metadata.findAliases(
                new GetAliasesRequest("alias1"),
                new String[] { "index" }
            );
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList.size(), equalTo(1));
            assertThat(aliasMetadataList.get(0).alias(), equalTo("alias1"));
        }
        {
            final Map<String, List<AliasMetadata>> aliases = metadata.findAllAliases(new String[] { "index" });
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList.size(), equalTo(2));
            assertThat(aliasMetadataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetadataList.get(1).alias(), equalTo("alias2"));
        }
        {
            final Map<String, List<AliasMetadata>> aliases = metadata.findAllAliases(Strings.EMPTY_ARRAY);
            assertThat(aliases.size(), equalTo(0));
        }
    }

    public void testFindAliasWithExclusion() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias1").build())
                    .putAlias(AliasMetadata.builder("alias2").build())
            )
            .build();
        List<AliasMetadata> aliases = metadata.findAliases(new GetAliasesRequest().aliases("*", "-alias1"), new String[] { "index" })
            .get("index");
        assertThat(aliases.size(), equalTo(1));
        assertThat(aliases.get(0).alias(), equalTo("alias2"));
    }

    public void testFindDataStreams() {
        final int numIndices = randomIntBetween(2, 5);
        final int numBackingIndices = randomIntBetween(2, 5);
        final String dataStreamName = "my-data-stream";
        CreateIndexResult result = createIndices(numIndices, numBackingIndices, dataStreamName);

        List<Index> allIndices = new ArrayList<>(result.indices);
        allIndices.addAll(result.backingIndices);
        String[] concreteIndices = allIndices.stream().map(Index::getName).collect(Collectors.toList()).toArray(new String[] {});
        final Map<String, IndexAbstraction.DataStream> dataStreams = result.metadata.findDataStreams(concreteIndices);
        assertThat(dataStreams.size(), equalTo(numBackingIndices));
        for (Index backingIndex : result.backingIndices) {
            assertThat(dataStreams.containsKey(backingIndex.getName()), is(true));
            assertThat(dataStreams.get(backingIndex.getName()).getName(), equalTo(dataStreamName));
        }
    }

    public void testFindAliasWithExclusionAndOverride() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("aa").build())
                    .putAlias(AliasMetadata.builder("ab").build())
                    .putAlias(AliasMetadata.builder("bb").build())
            )
            .build();
        List<AliasMetadata> aliases = metadata.findAliases(new GetAliasesRequest().aliases("a*", "-*b", "b*"), new String[] { "index" })
            .get("index");
        assertThat(aliases.size(), equalTo(2));
        assertThat(aliases.get(0).alias(), equalTo("aa"));
        assertThat(aliases.get(1).alias(), equalTo("bb"));
    }

    public void testIndexAndAliasWithSameName() {
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("index").build());
        try {
            Metadata.builder().put(builder).build();
            fail("exception should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(
                e.getMessage(),
                equalTo(
                    "index, alias, and data stream names need to be unique, but the following duplicates were found [index (alias "
                        + "of [index]) conflicts with index]"
                )
            );
        }
    }

    public void testAliasCollidingWithAnExistingIndex() {
        int indexCount = randomIntBetween(10, 100);
        Set<String> indices = new HashSet<>(indexCount);
        for (int i = 0; i < indexCount; i++) {
            indices.add(randomAlphaOfLength(10));
        }
        Map<String, Set<String>> aliasToIndices = new HashMap<>();
        for (String alias : randomSubsetOf(randomIntBetween(1, 10), indices)) {
            aliasToIndices.put(alias, new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }
        int properAliases = randomIntBetween(0, 3);
        for (int i = 0; i < properAliases; i++) {
            aliasToIndices.put(randomAlphaOfLength(5), new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (String index : indices) {
            IndexMetadata.Builder indexBuilder = IndexMetadata.builder(index)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0);
            aliasToIndices.forEach((key, value) -> {
                if (value.contains(index)) {
                    indexBuilder.putAlias(AliasMetadata.builder(key).build());
                }
            });
            metadataBuilder.put(indexBuilder);
        }
        try {
            metadataBuilder.build();
            fail("exception should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), startsWith("index, alias, and data stream names need to be unique"));
        }
    }

    public void testValidateAliasWriteOnly() {
        String alias = randomAlphaOfLength(5);
        String indexA = randomAlphaOfLength(6);
        String indexB = randomAlphaOfLength(7);
        Boolean aWriteIndex = randomBoolean() ? null : randomBoolean();
        Boolean bWriteIndex;
        if (Boolean.TRUE.equals(aWriteIndex)) {
            bWriteIndex = randomFrom(Boolean.FALSE, null);
        } else {
            bWriteIndex = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        }
        // when only one index/alias pair exist
        Metadata metadata = Metadata.builder().put(buildIndexMetadata(indexA, alias, aWriteIndex)).build();

        // when alias points to two indices, but valid
        // one of the following combinations: [(null, null), (null, true), (null, false), (false, false)]
        Metadata.builder(metadata).put(buildIndexMetadata(indexB, alias, bWriteIndex)).build();

        // when too many write indices
        Exception exception = expectThrows(IllegalStateException.class, () -> {
            IndexMetadata.Builder metaA = buildIndexMetadata(indexA, alias, true);
            IndexMetadata.Builder metaB = buildIndexMetadata(indexB, alias, true);
            Metadata.builder().put(metaA).put(metaB).build();
        });
        assertThat(exception.getMessage(), startsWith("alias [" + alias + "] has more than one write index ["));
    }

    public void testValidateHiddenAliasConsistency() {
        String alias = randomAlphaOfLength(5);
        String indexA = randomAlphaOfLength(6);
        String indexB = randomAlphaOfLength(7);

        {
            Exception ex = expectThrows(
                IllegalStateException.class,
                () -> buildMetadataWithHiddenIndexMix(alias, indexA, true, indexB, randomFrom(false, null)).build()
            );
            assertThat(ex.getMessage(), containsString("has is_hidden set to true on indices"));
        }

        {
            Exception ex = expectThrows(
                IllegalStateException.class,
                () -> buildMetadataWithHiddenIndexMix(alias, indexA, randomFrom(false, null), indexB, true).build()
            );
            assertThat(ex.getMessage(), containsString("has is_hidden set to true on indices"));
        }
    }

    private Metadata.Builder buildMetadataWithHiddenIndexMix(
        String aliasName,
        String indexAName,
        Boolean indexAHidden,
        String indexBName,
        Boolean indexBHidden
    ) {
        IndexMetadata.Builder indexAMeta = IndexMetadata.builder(indexAName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).isHidden(indexAHidden).build());
        IndexMetadata.Builder indexBMeta = IndexMetadata.builder(indexBName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).isHidden(indexBHidden).build());
        return Metadata.builder().put(indexAMeta).put(indexBMeta);
    }

    public void testResolveIndexRouting() {
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build())
            .putAlias(AliasMetadata.builder("alias1").routing("1").build())
            .putAlias(AliasMetadata.builder("alias2").routing("1,2").build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // no alias, no index
        assertEquals(metadata.resolveIndexRouting(null, null), null);
        assertEquals(metadata.resolveIndexRouting("0", null), "0");

        // index, no alias
        assertEquals(metadata.resolveIndexRouting(null, "index"), null);
        assertEquals(metadata.resolveIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertEquals(metadata.resolveIndexRouting(null, "alias0"), null);
        assertEquals(metadata.resolveIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metadata.resolveIndexRouting(null, "alias1"), "1");
        try {
            metadata.resolveIndexRouting("0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(
                ex.getMessage(),
                is(
                    "Alias [alias1] has index routing associated with it [1], "
                        + "and was provided with routing value [0], rejecting operation"
                )
            );
        }

        // alias with invalid index routing.
        try {
            metadata.resolveIndexRouting(null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(
                ex.getMessage(),
                is(
                    "index/alias [alias2] provided with routing value [1,2] that"
                        + " resolved to several routing values, rejecting operation"
                )
            );
        }

        try {
            metadata.resolveIndexRouting("1", "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(
                ex.getMessage(),
                is(
                    "index/alias [alias2] provided with routing value [1,2] that"
                        + " resolved to several routing values, rejecting operation"
                )
            );
        }

        IndexMetadata.Builder builder2 = IndexMetadata.builder("index2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build());
        Metadata metadataTwoIndices = Metadata.builder(metadata).put(builder2).build();

        // alias with multiple indices
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> metadataTwoIndices.resolveIndexRouting("1", "alias0")
        );
        assertThat(exception.getMessage(), startsWith("Alias [alias0] has more than one index associated with it"));
    }

    public void testResolveWriteIndexRouting() {
        AliasMetadata.Builder aliasZeroBuilder = AliasMetadata.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(true);
        }
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilder.build())
            .putAlias(AliasMetadata.builder("alias1").routing("1").build())
            .putAlias(AliasMetadata.builder("alias2").routing("1,2").build())
            .putAlias(AliasMetadata.builder("alias3").writeIndex(false).build())
            .putAlias(AliasMetadata.builder("alias4").routing("1,2").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // no alias, no index
        assertEquals(metadata.resolveWriteIndexRouting(null, null), null);
        assertEquals(metadata.resolveWriteIndexRouting("0", null), "0");

        // index, no alias
        assertEquals(metadata.resolveWriteIndexRouting(null, "index"), null);
        assertEquals(metadata.resolveWriteIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertEquals(metadata.resolveWriteIndexRouting(null, "alias0"), null);
        assertEquals(metadata.resolveWriteIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metadata.resolveWriteIndexRouting(null, "alias1"), "1");
        Exception exception = expectThrows(IllegalArgumentException.class, () -> metadata.resolveWriteIndexRouting("0", "alias1"));
        assertThat(
            exception.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation")
        );

        // alias with invalid index routing.
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.resolveWriteIndexRouting(null, "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.resolveWriteIndexRouting("1", "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.resolveWriteIndexRouting(randomFrom("1", null), "alias4"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias4] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        // alias with no write index
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.resolveWriteIndexRouting("1", "alias3"));
        assertThat(exception.getMessage(), is("alias [alias3] does not have a write index"));

        // aliases with multiple indices
        AliasMetadata.Builder aliasZeroBuilderTwo = AliasMetadata.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(false);
        }
        IndexMetadata.Builder builder2 = IndexMetadata.builder("index2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilderTwo.build())
            .putAlias(AliasMetadata.builder("alias1").routing("0").writeIndex(true).build())
            .putAlias(AliasMetadata.builder("alias2").writeIndex(true).build());
        Metadata metadataTwoIndices = Metadata.builder(metadata).put(builder2).build();

        // verify that new write index is used
        assertThat("0", equalTo(metadataTwoIndices.resolveWriteIndexRouting("0", "alias1")));
    }

    public void testUnknownFieldClusterMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("meta-data").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            Metadata.Builder.fromXContent(parser);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testUnknownFieldIndexMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("index_name").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            IndexMetadata.Builder.fromXContent(parser);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testMetadataGlobalStateChangesOnIndexDeletions() {
        IndexGraveyard.Builder builder = IndexGraveyard.builder();
        builder.addTombstone(new Index("idx1", UUIDs.randomBase64UUID()));
        final Metadata metadata1 = Metadata.builder().indexGraveyard(builder.build()).build();
        builder = IndexGraveyard.builder(metadata1.indexGraveyard());
        builder.addTombstone(new Index("idx2", UUIDs.randomBase64UUID()));
        final Metadata metadata2 = Metadata.builder(metadata1).indexGraveyard(builder.build()).build();
        assertFalse("metadata not equal after adding index deletions", Metadata.isGlobalStateEquals(metadata1, metadata2));
        final Metadata metadata3 = Metadata.builder(metadata2).build();
        assertTrue("metadata equal when not adding index deletions", Metadata.isGlobalStateEquals(metadata2, metadata3));
    }

    public void testXContentWithIndexGraveyard() throws IOException {
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().indexGraveyard(graveyard).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, originalMeta);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(fromXContentMeta.indexGraveyard(), equalTo(originalMeta.indexGraveyard()));
        }
    }

    public void testXContentClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, originalMeta);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(fromXContentMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
            assertThat(fromXContentMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
        }
    }

    public void testSerializationClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
        assertThat(fromStreamMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
    }

    public void testMetadataGlobalStateChangesOnClusterUUIDChanges() {
        final Metadata metadata1 = Metadata.builder().clusterUUID(UUIDs.randomBase64UUID()).clusterUUIDCommitted(randomBoolean()).build();
        final Metadata metadata2 = Metadata.builder(metadata1).clusterUUID(UUIDs.randomBase64UUID()).build();
        final Metadata metadata3 = Metadata.builder(metadata1).clusterUUIDCommitted(!metadata1.clusterUUIDCommitted()).build();
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata3));
        final Metadata metadata4 = Metadata.builder(metadata2).clusterUUID(metadata1.clusterUUID()).build();
        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata4));
    }

    private static CoordinationMetadata.VotingConfiguration randomVotingConfig() {
        return new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    private Set<VotingConfigExclusion> randomVotingConfigExclusions() {
        final int size = randomIntBetween(0, 10);
        final Set<VotingConfigExclusion> nodes = new HashSet<>(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
    }

    public void testXContentWithCoordinationMetadata() throws IOException {
        CoordinationMetadata originalMeta = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );

        Metadata metadata = Metadata.builder().coordinationMetadata(originalMeta).build();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetadata fromXContentMeta = Metadata.fromXContent(parser).coordinationMetadata();
            assertThat(fromXContentMeta, equalTo(originalMeta));
        }
    }

    public void testGlobalStateEqualsCoordinationMetadata() {
        CoordinationMetadata coordinationMetadata1 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata1 = Metadata.builder().coordinationMetadata(coordinationMetadata1).build();
        CoordinationMetadata coordinationMetadata2 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata2 = Metadata.builder().coordinationMetadata(coordinationMetadata2).build();

        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata1));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
    }

    public void testGlobalResourcesStateEqualsCoordinationMetadata() {
        CoordinationMetadata coordinationMetadata1 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata1 = Metadata.builder()
            .coordinationMetadata(coordinationMetadata1)
            .clusterUUID(randomAlphaOfLength(10))
            .clusterUUIDCommitted(false)
            .hashesOfConsistentSettings(Map.of("a", "b"))
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();
        CoordinationMetadata coordinationMetadata2 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata2 = Metadata.builder()
            .coordinationMetadata(coordinationMetadata2)
            .clusterUUIDCommitted(true)
            .clusterUUID(randomAlphaOfLength(11))
            .hashesOfConsistentSettings(Map.of("b", "a"))
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();

        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata1));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertTrue(Metadata.isGlobalResourcesMetadataEquals(metadata1, metadata2));
    }

    public void testSerializationWithIndexGraveyard() throws IOException {
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().indexGraveyard(graveyard).build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.indexGraveyard(), equalTo(fromStreamMeta.indexGraveyard()));
    }

    public void testFindMappings() throws IOException {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index1")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(FIND_MAPPINGS_TEST_ITEM)
            )
            .put(
                IndexMetadata.builder("index2")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(FIND_MAPPINGS_TEST_ITEM)
            )
            .build();

        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(Strings.EMPTY_ARRAY, MapperPlugin.NOOP_FIELD_FILTER);
            assertEquals(0, mappings.size());
        }
        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(new String[] { "index1" }, MapperPlugin.NOOP_FIELD_FILTER);
            assertEquals(1, mappings.size());
            assertIndexMappingsNotFiltered(mappings, "index1");
        }
        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(
                new String[] { "index1", "index2" },
                MapperPlugin.NOOP_FIELD_FILTER
            );
            assertEquals(2, mappings.size());
            assertIndexMappingsNotFiltered(mappings, "index1");
            assertIndexMappingsNotFiltered(mappings, "index2");
        }
    }

    public void testFindMappingsNoOpFilters() throws IOException {
        MappingMetadata originalMappingMetadata = new MappingMetadata(
            "_doc",
            XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, true)
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index1")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(originalMappingMetadata)
            )
            .build();

        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(new String[] { "index1" }, MapperPlugin.NOOP_FIELD_FILTER);
            MappingMetadata mappingMetadata = mappings.get("index1");
            assertSame(originalMappingMetadata, mappingMetadata);
        }
        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(
                new String[] { "index1" },
                index -> field -> randomBoolean()
            );
            MappingMetadata mappingMetadata = mappings.get("index1");
            assertNotSame(originalMappingMetadata, mappingMetadata);
        }
    }

    @SuppressWarnings("unchecked")
    public void testFindMappingsWithFilters() throws IOException {
        String mapping = FIND_MAPPINGS_TEST_ITEM;
        if (randomBoolean()) {
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, false);
            Map<String, Object> doc = (Map<String, Object>) stringObjectMap.get("_doc");
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(doc);
                mapping = builder.toString();
            }
        }

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index1")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(mapping)
            )
            .put(
                IndexMetadata.builder("index2")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(mapping)
            )
            .put(
                IndexMetadata.builder("index3")
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .putMapping(mapping)
            )
            .build();

        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(new String[] { "index1", "index2", "index3" }, index -> {
                if (index.equals("index1")) {
                    return field -> field.startsWith("name.") == false
                        && field.startsWith("properties.key.") == false
                        && field.equals("age") == false
                        && field.equals("address.location") == false;
                }
                if (index.equals("index2")) {
                    return field -> false;
                }
                return MapperPlugin.NOOP_FIELD_PREDICATE;
            });

            assertIndexMappingsNoFields(mappings, "index2");
            assertIndexMappingsNotFiltered(mappings, "index3");

            MappingMetadata docMapping = mappings.get("index1");
            assertNotNull(docMapping);

            Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
            assertEquals(3, sourceAsMap.size());
            assertTrue(sourceAsMap.containsKey("_routing"));
            assertTrue(sourceAsMap.containsKey("_source"));

            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(6, typeProperties.size());
            assertTrue(typeProperties.containsKey("birth"));
            assertTrue(typeProperties.containsKey("ip"));
            assertTrue(typeProperties.containsKey("suggest"));

            Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
            assertNotNull(name);
            assertEquals(1, name.size());
            Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
            assertNotNull(nameProperties);
            assertEquals(0, nameProperties.size());

            Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
            assertNotNull(address);
            assertEquals(2, address.size());
            assertTrue(address.containsKey("type"));
            Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
            assertNotNull(addressProperties);
            assertEquals(2, addressProperties.size());
            assertLeafs(addressProperties, "street", "area");

            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertNotNull(properties);
            assertEquals(2, properties.size());
            assertTrue(properties.containsKey("type"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertNotNull(propertiesProperties);
            assertEquals(2, propertiesProperties.size());
            assertLeafs(propertiesProperties, "key");
            assertMultiField(propertiesProperties, "value", "keyword");
        }

        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(
                new String[] { "index1", "index2", "index3" },
                index -> field -> (index.equals("index3") && field.endsWith("keyword"))
            );

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index2");
            MappingMetadata mappingMetadata = mappings.get("index3");
            Map<String, Object> sourceAsMap = mappingMetadata.getSourceAsMap();
            assertEquals(3, sourceAsMap.size());
            assertTrue(sourceAsMap.containsKey("_routing"));
            assertTrue(sourceAsMap.containsKey("_source"));
            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertNotNull(typeProperties);
            assertEquals(1, typeProperties.size());
            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertNotNull(properties);
            assertEquals(2, properties.size());
            assertTrue(properties.containsKey("type"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertNotNull(propertiesProperties);
            assertEquals(2, propertiesProperties.size());
            Map<String, Object> key = (Map<String, Object>) propertiesProperties.get("key");
            assertEquals(1, key.size());
            Map<String, Object> keyProperties = (Map<String, Object>) key.get("properties");
            assertEquals(1, keyProperties.size());
            assertLeafs(keyProperties, "keyword");
            Map<String, Object> value = (Map<String, Object>) propertiesProperties.get("value");
            assertEquals(1, value.size());
            Map<String, Object> valueProperties = (Map<String, Object>) value.get("properties");
            assertEquals(1, valueProperties.size());
            assertLeafs(valueProperties, "keyword");
        }

        {
            final Map<String, MappingMetadata> mappings = metadata.findMappings(
                new String[] { "index1", "index2", "index3" },
                index -> field -> (index.equals("index2"))
            );

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index3");
            assertIndexMappingsNotFiltered(mappings, "index2");
        }
    }

    private static IndexMetadata.Builder buildIndexMetadata(String name, String alias, Boolean writeIndex) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT))
            .creationDate(randomNonNegativeLong())
            .putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex))
            .numberOfShards(1)
            .numberOfReplicas(0);
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNoFields(final Map<String, MappingMetadata> mappings, String index) {
        MappingMetadata docMapping = mappings.get(index);
        assertNotNull(docMapping);
        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertEquals(3, sourceAsMap.size());
        assertTrue(sourceAsMap.containsKey("_routing"));
        assertTrue(sourceAsMap.containsKey("_source"));
        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertEquals(0, typeProperties.size());
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNotFiltered(final Map<String, MappingMetadata> mappings, String index) {
        MappingMetadata docMapping = mappings.get(index);
        assertNotNull(docMapping);

        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertEquals(3, sourceAsMap.size());
        assertTrue(sourceAsMap.containsKey("_routing"));
        assertTrue(sourceAsMap.containsKey("_source"));

        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertEquals(7, typeProperties.size());
        assertTrue(typeProperties.containsKey("birth"));
        assertTrue(typeProperties.containsKey("age"));
        assertTrue(typeProperties.containsKey("ip"));
        assertTrue(typeProperties.containsKey("suggest"));

        Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
        assertNotNull(name);
        assertEquals(1, name.size());
        Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
        assertNotNull(nameProperties);
        assertEquals(2, nameProperties.size());
        assertLeafs(nameProperties, "first", "last");

        Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
        assertNotNull(address);
        assertEquals(2, address.size());
        assertTrue(address.containsKey("type"));
        Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
        assertNotNull(addressProperties);
        assertEquals(3, addressProperties.size());
        assertLeafs(addressProperties, "street", "location", "area");

        Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());
        assertTrue(properties.containsKey("type"));
        Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
        assertNotNull(propertiesProperties);
        assertEquals(2, propertiesProperties.size());
        assertMultiField(propertiesProperties, "key", "keyword");
        assertMultiField(propertiesProperties, "value", "keyword");
    }

    @SuppressWarnings("unchecked")
    public static void assertLeafs(Map<String, Object> properties, String... fields) {
        for (String field : fields) {
            assertTrue(properties.containsKey(field));
            Map<String, Object> fieldProp = (Map<String, Object>) properties.get(field);
            assertNotNull(fieldProp);
            assertFalse(fieldProp.containsKey("properties"));
            assertFalse(fieldProp.containsKey("fields"));
        }
    }

    public static void assertMultiField(Map<String, Object> properties, String field, String... subFields) {
        assertTrue(properties.containsKey(field));
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldProp = (Map<String, Object>) properties.get(field);
        assertNotNull(fieldProp);
        assertTrue(fieldProp.containsKey("fields"));
        @SuppressWarnings("unchecked")
        Map<String, Object> subFieldsDef = (Map<String, Object>) fieldProp.get("fields");
        assertLeafs(subFieldsDef, subFields);
    }

    private static final String FIND_MAPPINGS_TEST_ITEM = "{\n"
        + "  \"_doc\": {\n"
        + "      \"_routing\": {\n"
        + "        \"required\":true\n"
        + "      },"
        + "      \"_source\": {\n"
        + "        \"enabled\":false\n"
        + "      },"
        + "      \"properties\": {\n"
        + "        \"name\": {\n"
        + "          \"properties\": {\n"
        + "            \"first\": {\n"
        + "              \"type\": \"keyword\"\n"
        + "            },\n"
        + "            \"last\": {\n"
        + "              \"type\": \"keyword\"\n"
        + "            }\n"
        + "          }\n"
        + "        },\n"
        + "        \"birth\": {\n"
        + "          \"type\": \"date\"\n"
        + "        },\n"
        + "        \"age\": {\n"
        + "          \"type\": \"integer\"\n"
        + "        },\n"
        + "        \"ip\": {\n"
        + "          \"type\": \"ip\"\n"
        + "        },\n"
        + "        \"suggest\" : {\n"
        + "          \"type\": \"completion\"\n"
        + "        },\n"
        + "        \"address\": {\n"
        + "          \"type\": \"object\",\n"
        + "          \"properties\": {\n"
        + "            \"street\": {\n"
        + "              \"type\": \"keyword\"\n"
        + "            },\n"
        + "            \"location\": {\n"
        + "              \"type\": \"geo_point\"\n"
        + "            },\n"
        + "            \"area\": {\n"
        + "              \"type\": \"geo_shape\",  \n"
        + "              \"tree\": \"quadtree\",\n"
        + "              \"precision\": \"1m\"\n"
        + "            }\n"
        + "          }\n"
        + "        },\n"
        + "        \"properties\": {\n"
        + "          \"type\": \"nested\",\n"
        + "          \"properties\": {\n"
        + "            \"key\" : {\n"
        + "              \"type\": \"text\",\n"
        + "              \"fields\": {\n"
        + "                \"keyword\" : {\n"
        + "                  \"type\" : \"keyword\"\n"
        + "                }\n"
        + "              }\n"
        + "            },\n"
        + "            \"value\" : {\n"
        + "              \"type\": \"text\",\n"
        + "              \"fields\": {\n"
        + "                \"keyword\" : {\n"
        + "                  \"type\" : \"keyword\"\n"
        + "                }\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";

    public void testTransientSettingsOverridePersistentSettings() {
        final Setting setting = Setting.simpleString("key");
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(setting.getKey(), "persistent-value").build())
            .transientSettings(Settings.builder().put(setting.getKey(), "transient-value").build())
            .build();
        assertThat(setting.get(metadata.settings()), equalTo("transient-value"));
    }

    public void testBuilderRejectsNullCustom() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        final Map<String, Metadata.Custom> mapBuilder = new HashMap<>();
        mapBuilder.put(key, null);
        assertThat(expectThrows(NullPointerException.class, () -> builder.customs(mapBuilder)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsDataStreamThatConflictsWithIndex() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        Metadata.Builder b = Metadata.builder()
            .put(idx, false)
            .put(
                IndexMetadata.builder(dataStreamName).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build(),
                false
            )
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "index, alias, and data stream names need to be unique, but the following duplicates were found [data "
                    + "stream ["
                    + dataStreamName
                    + "] conflicts with index]"
            )
        );
    }

    public void testBuilderRejectsDataStreamThatConflictsWithAlias() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).putAlias(AliasMetadata.builder(dataStreamName).build()).build();
        Metadata.Builder b = Metadata.builder()
            .put(idx, false)
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "index, alias, and data stream names need to be unique, but the following duplicates were found ["
                    + dataStreamName
                    + " (alias of ["
                    + DataStream.getDefaultBackingIndexName(dataStreamName, 1)
                    + "]) conflicts with data stream]"
            )
        );
    }

    public void testBuilderRejectsDataStreamWithConflictingBackingIndices() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata validIdx = createFirstBackingIndex(dataStreamName).build();
        final String conflictingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata invalidIdx = createBackingIndex(dataStreamName, 2).build();
        Metadata.Builder b = Metadata.builder()
            .put(validIdx, false)
            .put(invalidIdx, false)
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(validIdx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "data stream ["
                    + dataStreamName
                    + "] could create backing indices that conflict with 1 existing index(s) or alias(s) including '"
                    + conflictingIndex
                    + "'"
            )
        );
    }

    public void testBuilderRejectsDataStreamWithConflictingBackingAlias() {
        final String dataStreamName = "my-data-stream";
        final String conflictingName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).putAlias(new AliasMetadata.Builder(conflictingName)).build();
        Metadata.Builder b = Metadata.builder()
            .put(idx, false)
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "data stream ["
                    + dataStreamName
                    + "] could create backing indices that conflict with 1 existing index(s) or alias(s) including '"
                    + conflictingName
                    + "'"
            )
        );
    }

    public void testBuilderForDataStreamWithRandomlyNumberedBackingIndices() {
        final String dataStreamName = "my-data-stream";
        final List<Index> backingIndices = new ArrayList<>();
        final int numBackingIndices = randomIntBetween(2, 5);
        int lastBackingIndexNum = 0;
        Metadata.Builder b = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            lastBackingIndexNum = randomIntBetween(lastBackingIndexNum + 1, lastBackingIndexNum + 50);
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, lastBackingIndexNum))
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            backingIndices.add(im.getIndex());
        }

        b.put(new DataStream(dataStreamName, createTimestampField("@timestamp"), backingIndices, lastBackingIndexNum));
        Metadata metadata = b.build();
        assertThat(metadata.dataStreams().size(), equalTo(1));
        assertThat(metadata.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
    }

    public void testBuildIndicesLookupForDataStreams() {
        Metadata.Builder b = Metadata.builder();
        int numDataStreams = randomIntBetween(2, 8);
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            int numBackingIndices = randomIntBetween(1, 4);
            List<Index> indices = new ArrayList<>(numBackingIndices);
            for (int j = 1; j <= numBackingIndices; j++) {
                IndexMetadata idx = createBackingIndex(name, j).build();
                indices.add(idx.getIndex());
                b.put(idx, true);
            }
            b.put(new DataStream(name, createTimestampField("@timestamp"), indices, indices.size()));
        }

        Metadata metadata = b.build();
        assertThat(metadata.dataStreams().size(), equalTo(numDataStreams));
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            IndexAbstraction value = metadata.getIndicesLookup().get(name);
            assertThat(value, notNullValue());
            DataStream ds = metadata.dataStreams().get(name);
            assertThat(ds, notNullValue());

            assertThat(value.isHidden(), is(false));
            assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(value.getIndices().size(), equalTo(ds.getIndices().size()));
            assertThat(
                value.getWriteIndex().getIndex().getName(),
                equalTo(DataStream.getDefaultBackingIndexName(name, ds.getGeneration()))
            );
        }
    }

    public void testIndicesLookupRecordsDataStreamForBackingIndices() {
        final int numIndices = randomIntBetween(2, 5);
        final int numBackingIndices = randomIntBetween(2, 5);
        final String dataStreamName = "my-data-stream";
        CreateIndexResult result = createIndices(numIndices, numBackingIndices, dataStreamName);

        SortedMap<String, IndexAbstraction> indicesLookup = result.metadata.getIndicesLookup();
        assertThat(indicesLookup.size(), equalTo(result.indices.size() + result.backingIndices.size() + 1));
        for (Index index : result.indices) {
            assertTrue(indicesLookup.containsKey(index.getName()));
            assertNull(indicesLookup.get(index.getName()).getParentDataStream());
        }
        for (Index index : result.backingIndices) {
            assertTrue(indicesLookup.containsKey(index.getName()));
            assertNotNull(indicesLookup.get(index.getName()).getParentDataStream());
            assertThat(indicesLookup.get(index.getName()).getParentDataStream().getName(), equalTo(dataStreamName));
        }
    }

    public void testSerialization() throws IOException {
        final Metadata orig = randomMetadata();
        final BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertTrue(Metadata.isGlobalStateEquals(orig, fromStreamMeta));
    }

    public void testValidateDataStreamsNoConflicts() {
        Metadata metadata = createIndices(5, 10, "foo-datastream").metadata;
        // don't expect any exception when validating a system without indices that would conflict with future backing indices
        validateDataStreams(metadata.getIndicesLookup(), (DataStreamMetadata) metadata.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsThrowsExceptionOnConflict() {
        String dataStreamName = "foo-datastream";
        int generations = 10;
        List<IndexMetadata> backingIndices = new ArrayList<>(generations);
        for (int i = 1; i <= generations; i++) {
            IndexMetadata idx = createBackingIndex(dataStreamName, i).build();
            backingIndices.add(idx);
        }
        DataStream dataStream = new DataStream(
            dataStreamName,
            createTimestampField("@timestamp"),
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
            backingIndices.size()
        );

        IndexAbstraction.DataStream dataStreamAbstraction = new IndexAbstraction.DataStream(dataStream, backingIndices);
        // manually building the indices lookup as going through Metadata.Builder#build would trigger the validate method and would fail
        SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
        for (IndexMetadata indexMeta : backingIndices) {
            indicesLookup.put(indexMeta.getIndex().getName(), new IndexAbstraction.Index(indexMeta, dataStreamAbstraction));
        }

        // add the offending index to the indices lookup
        IndexMetadata standaloneIndexConflictingWithBackingIndices = createBackingIndex(dataStreamName, 2 * generations).build();
        Index index = standaloneIndexConflictingWithBackingIndices.getIndex();
        indicesLookup.put(index.getName(), new IndexAbstraction.Index(standaloneIndexConflictingWithBackingIndices, null));

        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(Map.of(dataStreamName, dataStream));

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> validateDataStreams(indicesLookup, dataStreamMetadata)
        );
        assertThat(
            illegalStateException.getMessage(),
            is(
                "data stream [foo-datastream] could create backing indices that conflict with 1 existing index(s) or alias(s) "
                    + "including '"
                    + index.getName()
                    + "'"
            )
        );
    }

    public void testValidateDataStreamsIgnoresIndicesWithoutCounter() {
        String dataStreamName = "foo-datastream";
        Metadata metadata = Metadata.builder(createIndices(10, 10, dataStreamName).metadata)
            .put(
                new IndexMetadata.Builder(dataStreamName + "-index-without-counter").settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(
                new IndexMetadata.Builder(dataStreamName + randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)

            )
            .put(
                new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)

            )
            .build();
        // don't expect any exception when validating against non-backing indices that don't conform to the backing indices naming
        // convention
        validateDataStreams(metadata.getIndicesLookup(), (DataStreamMetadata) metadata.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsAllowsNamesThatStartsWithPrefix() {
        String dataStreamName = "foo-datastream";
        Metadata metadata = Metadata.builder(createIndices(10, 10, dataStreamName).metadata)
            .put(
                new IndexMetadata.Builder(DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-something-100012").settings(
                    settings(Version.CURRENT)
                ).numberOfShards(1).numberOfReplicas(1)
            )
            .build();
        // don't expect any exception when validating against (potentially backing) indices that can't create conflict because of
        // additional text before number
        validateDataStreams(metadata.getIndicesLookup(), (DataStreamMetadata) metadata.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsAllowsPrefixedBackingIndices() {
        String dataStreamName = "foo-datastream";
        int generations = 10;
        List<IndexMetadata> backingIndices = new ArrayList<>(generations);
        for (int i = 1; i <= generations; i++) {
            IndexMetadata idx;
            if (i % 2 == 0 && i < generations) {
                idx = IndexMetadata.builder("shrink-" + DataStream.getDefaultBackingIndexName(dataStreamName, i))
                    .settings(OpenSearchTestCase.settings(Version.CURRENT).put("index.hidden", true))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .build();
            } else {
                idx = createBackingIndex(dataStreamName, i).build();
            }
            backingIndices.add(idx);
        }
        DataStream dataStream = new DataStream(
            dataStreamName,
            createTimestampField("@timestamp"),
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
            backingIndices.size()
        );

        IndexAbstraction.DataStream dataStreamAbstraction = new IndexAbstraction.DataStream(dataStream, backingIndices);
        // manually building the indices lookup as going through Metadata.Builder#build would trigger the validate method already
        SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
        for (IndexMetadata indexMeta : backingIndices) {
            indicesLookup.put(indexMeta.getIndex().getName(), new IndexAbstraction.Index(indexMeta, dataStreamAbstraction));
        }

        for (int i = 1; i <= generations; i++) {
            // for the indices that we added in the data stream with a "shrink-" prefix, add the non-prefixed indices to the lookup
            if (i % 2 == 0 && i < generations) {
                IndexMetadata indexMeta = createBackingIndex(dataStreamName, i).build();
                indicesLookup.put(indexMeta.getIndex().getName(), new IndexAbstraction.Index(indexMeta, dataStreamAbstraction));
            }
        }
        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(Map.of(dataStreamName, dataStream));

        // prefixed indices with a lower generation than the data stream's generation are allowed even if the non-prefixed, matching the
        // data stream backing indices naming pattern, indices are already in the system
        validateDataStreams(indicesLookup, dataStreamMetadata);
    }

    public void testValidateDataStreamsForNullDataStreamMetadata() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("foo-index").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        try {
            validateDataStreams(metadata.getIndicesLookup(), null);
        } catch (Exception e) {
            fail("did not expect exception when validating a system without any data streams but got " + e.getMessage());
        }
    }

    public void testMetadataBuildInvocations() {
        final Metadata previousMetadata = randomMetadata();
        Metadata builtMetadata;
        Metadata.Builder spyBuilder;

        // previous Metadata state was not provided to Builder during assignment - indices lookups should get re-computed
        spyBuilder = spy(Metadata.builder());
        builtMetadata = spyBuilder.build();
        verify(spyBuilder, times(1)).buildMetadataWithRecomputedIndicesLookups();
        verify(spyBuilder, times(0)).buildMetadataWithPreviousIndicesLookups();
        compareMetadata(Metadata.EMPTY_METADATA, builtMetadata, true, true, false);

        // no changes in builder method after initialization from previous Metadata - indices lookups should not be re-computed
        spyBuilder = spy(Metadata.builder(previousMetadata));
        builtMetadata = spyBuilder.version(previousMetadata.version() + 1).build();
        verify(spyBuilder, times(0)).buildMetadataWithRecomputedIndicesLookups();
        verify(spyBuilder, times(1)).buildMetadataWithPreviousIndicesLookups();
        compareMetadata(previousMetadata, builtMetadata, true, true, true);
        reset(spyBuilder);

        // adding new index - all indices lookups should get re-computed
        spyBuilder = spy(Metadata.builder(previousMetadata));
        String index = "new_index_" + randomAlphaOfLength(3);
        builtMetadata = spyBuilder.indices(
            Collections.singletonMap(
                index,
                IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build()
            )
        ).build();
        verify(spyBuilder, times(1)).buildMetadataWithRecomputedIndicesLookups();
        verify(spyBuilder, times(0)).buildMetadataWithPreviousIndicesLookups();
        compareMetadata(previousMetadata, builtMetadata, false, true, false);
        reset(spyBuilder);

        // adding new templates - indices lookups should not get recomputed
        spyBuilder = spy(Metadata.builder(previousMetadata));
        builtMetadata = spyBuilder.put("component_template_new_" + randomAlphaOfLength(3), ComponentTemplateTests.randomInstance())
            .put("index_template_v2_new_" + randomAlphaOfLength(3), ComposableIndexTemplateTests.randomInstance())
            .build();
        verify(spyBuilder, times(0)).buildMetadataWithRecomputedIndicesLookups();
        verify(spyBuilder, times(1)).buildMetadataWithPreviousIndicesLookups();
        compareMetadata(previousMetadata, builtMetadata, true, false, false);
        reset(spyBuilder);

        // adding new data stream - indices lookups should get re-computed
        spyBuilder = spy(Metadata.builder(previousMetadata));
        DataStream dataStream = DataStreamTests.randomInstance();
        for (Index backingIndex : dataStream.getIndices()) {
            spyBuilder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(backingIndex));
        }
        builtMetadata = spyBuilder.put(dataStream).version(previousMetadata.version() + 1).build();
        verify(spyBuilder, times(1)).buildMetadataWithRecomputedIndicesLookups();
        verify(spyBuilder, times(0)).buildMetadataWithPreviousIndicesLookups();
        compareMetadata(previousMetadata, builtMetadata, false, true, true);
    }

    public void testIsSegmentReplicationEnabled() {
        final String indexName = "test";
        Settings.Builder builder = settings(Version.CURRENT).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(builder)
            .numberOfShards(1)
            .numberOfReplicas(1);
        Metadata.Builder metadataBuilder = Metadata.builder().put(indexMetadataBuilder);
        Metadata metadata = metadataBuilder.build();
        assertTrue(metadata.isSegmentReplicationEnabled(indexName));
    }

    public void testIsSegmentReplicationDisabled() {
        final String indexName = "test";
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        Metadata.Builder metadataBuilder = Metadata.builder().put(indexMetadataBuilder);
        Metadata metadata = metadataBuilder.build();
        assertFalse(metadata.isSegmentReplicationEnabled(indexName));
    }

    public void testTemplatesMetadata() {
        TemplatesMetadata templatesMetadata1 = TemplatesMetadata.builder()
            .put(
                IndexTemplateMetadata.builder("template_1")
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                    .build()
            )
            .build();
        Metadata metadata1 = Metadata.builder().templates(templatesMetadata1).build();
        assertThat(metadata1.templates(), is(templatesMetadata1.getTemplates()));

        TemplatesMetadata templatesMetadata2 = TemplatesMetadata.builder()
            .put(
                IndexTemplateMetadata.builder("template_2")
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                    .build()
            )
            .build();

        Metadata metadata2 = Metadata.builder(metadata1).templates(templatesMetadata2).build();

        assertThat(metadata2.templates(), is(templatesMetadata2.getTemplates()));

    }

    public static Metadata randomMetadata() {
        Metadata.Builder md = Metadata.builder()
            .put(buildIndexMetadata("index", "alias", randomBoolean() ? null : randomBoolean()).build(), randomBoolean())
            .put(
                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                    .build()
            )
            .persistentSettings(Settings.builder().put("setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .transientSettings(Settings.builder().put("other_setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .clusterUUID("uuid" + randomAlphaOfLength(3))
            .clusterUUIDCommitted(randomBoolean())
            .indexGraveyard(IndexGraveyardTests.createRandom())
            .version(randomNonNegativeLong())
            .put("component_template_" + randomAlphaOfLength(3), ComponentTemplateTests.randomInstance())
            .put("index_template_v2_" + randomAlphaOfLength(3), ComposableIndexTemplateTests.randomInstance());

        DataStream randomDataStream = DataStreamTests.randomInstance();
        for (Index index : randomDataStream.getIndices()) {
            md.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        md.put(randomDataStream);

        return md.build();
    }

    public void testXContentWithTemplateMetadata() throws IOException {
        final TemplatesMetadata templatesMetadata = getTemplatesMetadata(0);
        verifyTemplatesMetadata(templatesMetadata);
        final TemplatesMetadata templatesMetadata2 = getTemplatesMetadata(2);
        verifyTemplatesMetadata(templatesMetadata2);
    }

    private void verifyTemplatesMetadata(TemplatesMetadata templatesMetadata) throws IOException {
        final Metadata originalMeta = Metadata.builder().templates(templatesMetadata).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, originalMeta);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(fromXContentMeta.templates(), equalTo(originalMeta.templates()));
        }
    }

    private static TemplatesMetadata getTemplatesMetadata(int numberOfTemplates) {
        TemplatesMetadata.Builder builder = TemplatesMetadata.builder();
        for (int i = 0; i < numberOfTemplates; i++) {
            builder.put(
                IndexTemplateMetadata.builder("template" + i)
                    .order(1234)
                    .patterns(Arrays.asList(randomAlphaOfLength(3) + "-*"))
                    .settings(
                        Settings.builder().put("index.random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build()
                    )
                    .build()
            );
        }
        return builder.build();
    }

    private static CreateIndexResult createIndices(int numIndices, int numBackingIndices, String dataStreamName) {
        // create some indices that do not back a data stream
        final List<Index> indices = new ArrayList<>();
        int lastIndexNum = randomIntBetween(9, 50);
        Metadata.Builder b = Metadata.builder();
        for (int k = 1; k <= numIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("index", lastIndexNum))
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            indices.add(im.getIndex());
            lastIndexNum = randomIntBetween(lastIndexNum + 1, lastIndexNum + 50);
        }

        // create some backing indices for a data stream
        final List<Index> backingIndices = new ArrayList<>();
        int lastBackingIndexNum = 0;
        for (int k = 1; k <= numBackingIndices; k++) {
            lastBackingIndexNum = randomIntBetween(lastBackingIndexNum + 1, lastBackingIndexNum + 50);
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, lastBackingIndexNum))
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            backingIndices.add(im.getIndex());
        }
        b.put(new DataStream(dataStreamName, createTimestampField("@timestamp"), backingIndices, lastBackingIndexNum));
        return new CreateIndexResult(indices, backingIndices, b.build());
    }

    private static class CreateIndexResult {
        final List<Index> indices;
        final List<Index> backingIndices;
        final Metadata metadata;

        CreateIndexResult(List<Index> indices, List<Index> backingIndices, Metadata metadata) {
            this.indices = indices;
            this.backingIndices = backingIndices;
            this.metadata = metadata;
        }
    }

    private static void compareMetadata(
        final Metadata previousMetadata,
        final Metadata newMetadata,
        final boolean compareIndicesLookups,
        final boolean compareTemplates,
        final boolean checkVersionIncrement
    ) {
        assertEquals(previousMetadata.clusterUUID(), newMetadata.clusterUUID());
        assertEquals(previousMetadata.clusterUUIDCommitted(), newMetadata.clusterUUIDCommitted());
        assertEquals(previousMetadata.coordinationMetadata(), newMetadata.coordinationMetadata());
        assertEquals(previousMetadata.settings(), newMetadata.settings());
        assertEquals(previousMetadata.transientSettings(), newMetadata.transientSettings());
        assertEquals(previousMetadata.persistentSettings(), newMetadata.persistentSettings());
        assertEquals(previousMetadata.hashesOfConsistentSettings(), newMetadata.hashesOfConsistentSettings());

        if (compareIndicesLookups == true) {
            assertEquals(previousMetadata.indices(), newMetadata.indices());
            assertEquals(previousMetadata.getConcreteAllIndices(), newMetadata.getConcreteAllIndices());
            assertEquals(previousMetadata.getConcreteAllClosedIndices(), newMetadata.getConcreteAllClosedIndices());
            assertEquals(previousMetadata.getConcreteAllOpenIndices(), newMetadata.getConcreteAllOpenIndices());
            assertEquals(previousMetadata.getConcreteVisibleIndices(), newMetadata.getConcreteVisibleIndices());
            assertEquals(previousMetadata.getConcreteVisibleClosedIndices(), newMetadata.getConcreteVisibleClosedIndices());
            assertEquals(previousMetadata.getConcreteVisibleOpenIndices(), newMetadata.getConcreteVisibleOpenIndices());
            assertEquals(previousMetadata.getIndicesLookup(), newMetadata.getIndicesLookup());
            assertEquals(previousMetadata.getCustoms().get(DataStreamMetadata.TYPE), newMetadata.getCustoms().get(DataStreamMetadata.TYPE));
        }

        if (compareTemplates == true) {
            assertEquals(previousMetadata.templates(), newMetadata.templates());
            assertEquals(previousMetadata.templatesV2(), newMetadata.templatesV2());
            assertEquals(previousMetadata.componentTemplates(), newMetadata.componentTemplates());
        }

        if (compareIndicesLookups == true && compareTemplates == true) {
            assertEquals(previousMetadata.getCustoms(), newMetadata.getCustoms());
        }

        if (checkVersionIncrement == true) {
            assertEquals(previousMetadata.version() + 1, newMetadata.version());
        }
    }
}
