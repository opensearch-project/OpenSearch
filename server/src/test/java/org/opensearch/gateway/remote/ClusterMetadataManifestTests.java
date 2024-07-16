/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V0;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V1;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;

public class ClusterMetadataManifestTests extends OpenSearchTestCase {

    public void testClusterMetadataManifestXContentV0() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path", CODEC_V0);
        ClusterMetadataManifest originalManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .opensearchVersion(Version.CURRENT)
            .nodeId("test-node-id")
            .committed(false)
            .codecVersion(CODEC_V0)
            .indices(Collections.singletonList(uploadedIndexMetadata))
            .previousClusterUUID("prev-cluster-uuid")
            .clusterUUIDCommitted(true)
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContentV0(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestXContentV1() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path", CODEC_V1);
        ClusterMetadataManifest originalManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .opensearchVersion(Version.CURRENT)
            .nodeId("test-node-id")
            .committed(false)
            .codecVersion(CODEC_V1)
            .globalMetadataFileName("test-global-metadata-file")
            .indices(Collections.singletonList(uploadedIndexMetadata))
            .previousClusterUUID("prev-cluster-uuid")
            .clusterUUIDCommitted(true)
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContentV1(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestXContent() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        ClusterMetadataManifest originalManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .opensearchVersion(Version.CURRENT)
            .nodeId("test-node-id")
            .committed(false)
            .codecVersion(ClusterMetadataManifest.CODEC_V2)
            .indices(Collections.singletonList(uploadedIndexMetadata))
            .previousClusterUUID("prev-cluster-uuid")
            .clusterUUIDCommitted(true)
            .coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination-file"))
            .settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, "setting-file"))
            .templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, "templates-file"))
            .customMetadataMap(
                Collections.unmodifiableList(
                    Arrays.asList(
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + RepositoriesMetadata.TYPE,
                            "custom--repositories-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + IndexGraveyard.TYPE,
                            "custom--index_graveyard-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + WeightedRoutingMetadata.TYPE,
                            "custom--weighted_routing_netadata-file"
                        )
                    )
                ).stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()))
            )
            .routingTableVersion(1L)
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContent(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestSerializationEqualsHashCode() {
        ClusterMetadataManifest initialManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1337L)
            .stateVersion(7L)
            .clusterUUID("HrYF3kP5SmSPWtKlWhnNSA")
            .stateUUID("6By9p9G0Rv2MmFYJcPAOgA")
            .opensearchVersion(Version.CURRENT)
            .nodeId("B10RX1f5RJenMQvYccCgSQ")
            .committed(true)
            .codecVersion(ClusterMetadataManifest.CODEC_V2)
            .indices(randomUploadedIndexMetadataList())
            .previousClusterUUID("yfObdx8KSMKKrXf8UyHhM")
            .clusterUUIDCommitted(true)
            .coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination-file"))
            .settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, "setting-file"))
            .templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, "templates-file"))
            .customMetadataMap(
                Collections.unmodifiableList(
                    Arrays.asList(
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + RepositoriesMetadata.TYPE,
                            "custom--repositories-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + IndexGraveyard.TYPE,
                            "custom--index_graveyard-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + WeightedRoutingMetadata.TYPE,
                            "custom--weighted_routing_netadata-file"
                        )
                    )
                ).stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()))
            )
            .routingTableVersion(1L)
            .discoveryNodesMetadata(new UploadedMetadataAttribute(DISCOVERY_NODES, "discovery-nodes-file"))
            .clusterBlocksMetadata(new UploadedMetadataAttribute(CLUSTER_BLOCKS, "cluster-block-file"))
            .transientSettingsMetadata(new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, "transient-settings-file"))
            .hashesOfConsistentSettings(new UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, "hashes-of-consistent-settings-file"))
            .clusterStateCustomMetadataMap(Collections.emptyMap())
            .diffManifest(
                new ClusterStateDiffManifest(
                    RemoteClusterStateServiceTests.generateClusterStateWithOneIndex().build(),
                    ClusterState.EMPTY_STATE
                )
            )
            .build();
        {  // Mutate Cluster Term
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterTerm(1338L);
                    return builder.build();
                }
            );
        }
        {  // Mutate State Version
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.stateVersion(8L);
                    return builder.build();
                }
            );
        }
        {  // Mutate Cluster UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterUUID("efOkMiPbQZCUQQgtFWdbPw");
                    return builder.build();
                }
            );
        }
        {  // Mutate State UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.stateUUID("efOkMiPbQZCUQQgtFWdbPw");
                    return builder.build();
                }
            );
        }
        {  // Mutate OpenSearch Version
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.opensearchVersion(Version.V_EMPTY);
                    return builder.build();
                }
            );
        }
        {  // Mutate Committed State
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.committed(false);
                    return builder.build();
                }
            );
        }
        {  // Mutate Indices
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.indices(randomUploadedIndexMetadataList());
                    return builder.build();
                }
            );
        }
        { // Mutate Previous cluster UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.previousClusterUUID("vZX62DCQEOzGXlxXCrEu");
                    return builder.build();
                }
            );

        }
        { // Mutate cluster uuid committed
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterUUIDCommitted(false);
                    return builder.build();
                }
            );
        }
        {
            // Mutate Coordination metadata
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.coordinationMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate setting metadata
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.settingMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate template metadata
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.templatesMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate custom metadata
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.customMetadataMap(Collections.emptyMap());
                    return builder.build();
                }
            );
        }
        {
            // Mutate discovery nodes
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.discoveryNodesMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate cluster blocks
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterBlocksMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate transient settings metadata
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.transientSettingsMetadata(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
        {
            // Mutate diff manifest
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.diffManifest(null);
                    return builder.build();
                }
            );
        }
        {
            // Mutate hashes of consistent settings
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.hashesOfConsistentSettings(randomUploadedMetadataAttribute());
                    return builder.build();
                }
            );
        }
    }

    public void testClusterMetadataManifestXContentV2() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        UploadedMetadataAttribute uploadedMetadataAttribute = new UploadedMetadataAttribute("attribute_name", "testing_attribute");
        ClusterMetadataManifest originalManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .opensearchVersion(Version.CURRENT)
            .nodeId("test-node-id")
            .committed(false)
            .codecVersion(ClusterMetadataManifest.CODEC_V2)
            .indices(Collections.singletonList(uploadedIndexMetadata))
            .previousClusterUUID("prev-cluster-uuid")
            .clusterUUIDCommitted(true)
            .coordinationMetadata(uploadedMetadataAttribute)
            .settingMetadata(uploadedMetadataAttribute)
            .templatesMetadata(uploadedMetadataAttribute)
            .customMetadataMap(
                Collections.unmodifiableList(
                    Arrays.asList(
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + RepositoriesMetadata.TYPE,
                            "custom--repositories-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + IndexGraveyard.TYPE,
                            "custom--index_graveyard-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + WeightedRoutingMetadata.TYPE,
                            "custom--weighted_routing_netadata-file"
                        )
                    )
                ).stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()))
            )
            .routingTableVersion(1L)
            .indicesRouting(Collections.singletonList(uploadedIndexMetadata))
            .discoveryNodesMetadata(uploadedMetadataAttribute)
            .clusterBlocksMetadata(uploadedMetadataAttribute)
            .transientSettingsMetadata(uploadedMetadataAttribute)
            .hashesOfConsistentSettings(uploadedMetadataAttribute)
            .clusterStateCustomMetadataMap(Collections.emptyMap())
            .diffManifest(
                new ClusterStateDiffManifest(
                    RemoteClusterStateServiceTests.generateClusterStateWithOneIndex().build(),
                    ClusterState.EMPTY_STATE
                )
            )
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContent(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestXContentV2WithoutEphemeral() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        UploadedMetadataAttribute uploadedMetadataAttribute = new UploadedMetadataAttribute("attribute_name", "testing_attribute");
        UploadedIndexMetadata uploadedIndexRoutingMetadata = new UploadedIndexMetadata(
            "test-index",
            "test-uuid",
            "routing-path",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        ClusterMetadataManifest originalManifest = ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .opensearchVersion(Version.CURRENT)
            .nodeId("test-node-id")
            .committed(false)
            .codecVersion(ClusterMetadataManifest.CODEC_V2)
            .indices(Collections.singletonList(uploadedIndexMetadata))
            .previousClusterUUID("prev-cluster-uuid")
            .clusterUUIDCommitted(true)
            .coordinationMetadata(uploadedMetadataAttribute)
            .settingMetadata(uploadedMetadataAttribute)
            .templatesMetadata(uploadedMetadataAttribute)
            .customMetadataMap(
                Collections.unmodifiableList(
                    Arrays.asList(
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + RepositoriesMetadata.TYPE,
                            "custom--repositories-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + IndexGraveyard.TYPE,
                            "custom--index_graveyard-file"
                        ),
                        new UploadedMetadataAttribute(
                            CUSTOM_METADATA + CUSTOM_DELIMITER + WeightedRoutingMetadata.TYPE,
                            "custom--weighted_routing_netadata-file"
                        )
                    )
                ).stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()))
            )
            .indicesRouting(Collections.singletonList(uploadedIndexRoutingMetadata))
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContent(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public static List<UploadedIndexMetadata> randomUploadedIndexMetadataList() {
        final int size = randomIntBetween(1, 10);
        final List<UploadedIndexMetadata> uploadedIndexMetadataList = new ArrayList<>(size);
        while (uploadedIndexMetadataList.size() < size) {
            assertTrue(uploadedIndexMetadataList.add(randomUploadedIndexMetadata()));
        }
        return uploadedIndexMetadataList;
    }

    private static UploadedIndexMetadata randomUploadedIndexMetadata() {
        return new UploadedIndexMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    private UploadedMetadataAttribute randomUploadedMetadataAttribute() {
        return new UploadedMetadataAttribute("attribute_name", "testing_attribute");
    }

    public void testUploadedIndexMetadataSerializationEqualsHashCode() {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            uploadedIndexMetadata,
            orig -> OpenSearchTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), UploadedIndexMetadata::new),
            metadata -> randomlyChangingUploadedIndexMetadata(uploadedIndexMetadata)
        );
    }

    public void testUploadedIndexMetadataWithoutComponentPrefix() throws IOException {
        final UploadedIndexMetadata originalUploadedIndexMetadata = new UploadedIndexMetadata(
            "test-index",
            "test-index-uuid",
            "test_file_name",
            CODEC_V1
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalUploadedIndexMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final UploadedIndexMetadata fromXContentUploadedIndexMetadata = UploadedIndexMetadata.fromXContent(parser, 1L);
            assertEquals(originalUploadedIndexMetadata, fromXContentUploadedIndexMetadata);
        }
    }

    private UploadedIndexMetadata randomlyChangingUploadedIndexMetadata(UploadedIndexMetadata uploadedIndexMetadata) {
        switch (randomInt(2)) {
            case 0:
                return new UploadedIndexMetadata(
                    randomAlphaOfLength(10),
                    uploadedIndexMetadata.getIndexUUID(),
                    uploadedIndexMetadata.getUploadedFilename()
                );
            case 1:
                return new UploadedIndexMetadata(
                    uploadedIndexMetadata.getIndexName(),
                    randomAlphaOfLength(10),
                    uploadedIndexMetadata.getUploadedFilename()
                );
            case 2:
                return new UploadedIndexMetadata(
                    uploadedIndexMetadata.getIndexName(),
                    uploadedIndexMetadata.getIndexUUID(),
                    randomAlphaOfLength(10)
                );
        }
        return uploadedIndexMetadata;
    }

}
