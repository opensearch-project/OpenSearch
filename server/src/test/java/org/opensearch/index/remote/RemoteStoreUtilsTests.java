/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_CUSTOM_KEY;
import static org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdaterTests.createIndexMetadataWithDocrepSettings;
import static org.opensearch.index.remote.RemoteStoreUtils.URL_BASE64_CHARSET;
import static org.opensearch.index.remote.RemoteStoreUtils.determineTranslogMetadataEnabled;
import static org.opensearch.index.remote.RemoteStoreUtils.finalizeMigration;
import static org.opensearch.index.remote.RemoteStoreUtils.isSwitchToStrictCompatibilityMode;
import static org.opensearch.index.remote.RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding;
import static org.opensearch.index.remote.RemoteStoreUtils.longToUrlBase64;
import static org.opensearch.index.remote.RemoteStoreUtils.urlBase64ToLong;
import static org.opensearch.index.remote.RemoteStoreUtils.verifyNoMultipleWriters;
import static org.opensearch.index.shard.IndexShardTestUtils.MOCK_SEGMENT_REPO_NAME;
import static org.opensearch.index.shard.IndexShardTestUtils.MOCK_TLOG_REPO_NAME;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils.SEPARATOR;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;

public class RemoteStoreUtilsTests extends OpenSearchTestCase {

    private static Map<Character, Integer> BASE64_CHARSET_IDX_MAP;
    private static String index = "test-index";

    static {
        Map<Character, Integer> charToIndexMap = new HashMap<>();
        for (int i = 0; i < URL_BASE64_CHARSET.length; i++) {
            charToIndexMap.put(URL_BASE64_CHARSET[i], i);
        }
        BASE64_CHARSET_IDX_MAP = Collections.unmodifiableMap(charToIndexMap);
    }

    private final String metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        1,
        1,
        "node-1"
    );

    private final String metadataFilenameDup = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        2,
        1,
        "node-2"
    );
    private final String metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        13,
        34,
        1,
        1,
        "node-1"
    );

    private final String oldMetadataFilename = getOldSegmentMetadataFilename(12, 23, 34, 1, 1);

    /*
    Gives segment metadata filename for <2.11 version
     */
    public static String getOldSegmentMetadataFilename(
        long primaryTerm,
        long generation,
        long translogGeneration,
        long uploadCounter,
        int metadataVersion
    ) {
        return String.join(
            SEPARATOR,
            METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(translogGeneration),
            RemoteStoreUtils.invertLong(uploadCounter),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(metadataVersion)
        );
    }

    public static String getOldTranslogMetadataFilename(long primaryTerm, long generation, int metadataVersion) {
        return String.join(
            METADATA_SEPARATOR,
            METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(metadataVersion)
        );
    }

    public void testInvertToStrInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong(-1));
    }

    public void testInvertToStrValid() {
        assertEquals("9223372036854774573", RemoteStoreUtils.invertLong(1234));
        assertEquals("0000000000000001234", RemoteStoreUtils.invertLong(9223372036854774573L));
    }

    public void testInvertToLongInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong("-5"));
    }

    public void testInvertToLongValid() {
        assertEquals(1234, RemoteStoreUtils.invertLong("9223372036854774573"));
        assertEquals(9223372036854774573L, RemoteStoreUtils.invertLong("0000000000000001234"));
    }

    public void testinvert() {
        assertEquals(0, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(0)));
        assertEquals(Long.MAX_VALUE, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(Long.MAX_VALUE)));
        for (int i = 0; i < 10; i++) {
            long num = randomLongBetween(1, Long.MAX_VALUE);
            assertEquals(num, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(num)));
        }
    }

    public void testGetSegmentNameForCfeFile() {
        assertEquals("_foo", RemoteStoreUtils.getSegmentName("_foo.cfe"));
    }

    public void testGetSegmentNameForDvmFile() {
        assertEquals("_bar", RemoteStoreUtils.getSegmentName("_bar_1_Lucene90_0.dvm"));
    }

    public void testGetSegmentNameWeirdSegmentNameOnlyUnderscore() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("_.dvm"));
    }

    public void testGetSegmentNameUnderscoreDelimiterOverrides() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("___.dvm"));
    }

    public void testGetSegmentNameException() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.getSegmentName("dvd"));
    }

    public void testVerifyMultipleWriters_Segment() {
        List<String> mdFiles = new ArrayList<>();
        mdFiles.add(metadataFilename);
        mdFiles.add(metadataFilename2);
        mdFiles.add(oldMetadataFilename);
        verifyNoMultipleWriters(mdFiles, RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        mdFiles.add(metadataFilenameDup);
        assertThrows(
            IllegalStateException.class,
            () -> verifyNoMultipleWriters(mdFiles, RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen)
        );
    }

    public void testVerifyMultipleWriters_Translog() throws InterruptedException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename = tm.getFileName();
        Thread.sleep(1);
        TranslogTransferMetadata tm2 = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename2 = tm2.getFileName();
        List<BlobMetadata> bmList = new LinkedList<>();
        bmList.add(new PlainBlobMetadata(mdFilename, 1));
        bmList.add(new PlainBlobMetadata(mdFilename2, 1));
        bmList.add(new PlainBlobMetadata(getOldTranslogMetadataFilename(1, 1, 1), 1));
        RemoteStoreUtils.verifyNoMultipleWriters(
            bmList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
            TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
        );

        bmList = new LinkedList<>();
        bmList.add(new PlainBlobMetadata(mdFilename, 1));
        TranslogTransferMetadata tm3 = new TranslogTransferMetadata(1, 1, 1, 2, "node--2");
        bmList.add(new PlainBlobMetadata(tm3.getFileName(), 1));
        List<BlobMetadata> finalBmList = bmList;
        assertThrows(
            IllegalStateException.class,
            () -> RemoteStoreUtils.verifyNoMultipleWriters(
                finalBmList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
                TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
            )
        );
    }

    public void testLongToBase64() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "syVHd0gGq0w",
            -5878421770170594047L,
            "rmumi5UPDQE",
            -5147010836697060622L,
            "uJIk6f-V6vI",
            937096430362711837L,
            "DQE8PQwOVx0",
            8422273604115462710L,
            "dOHtOEZzejY",
            -2528761975013221124L,
            "3OgIYbXSXPw",
            -5512387536280560513L,
            "s4AQvdu03H8",
            -5749656451579835857L,
            "sDUd65cNCi8",
            5569654857969679538L,
            "TUtjlYLPvLI",
            -1563884000447039930L,
            "6kv3yZNv9kY"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            String base64Str = longToUrlBase64(entry.getKey());
            assertEquals(entry.getValue(), base64Str);
            assertEquals(11, entry.getValue().length());
            assertEquals((long) entry.getKey(), urlBase64ToLong(base64Str));
        }

        int iters = randomInt(100);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            String base64Str = longToUrlBase64(value);
            assertEquals(value, urlBase64ToLong(base64Str));
        }
    }

    public void testLongToCompositeUrlBase64AndBinaryEncodingUsing20Bits() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "s11001001010100",
            -5878421770170594047L,
            "r10011010111010",
            -5147010836697060622L,
            "u00100100100010",
            937096430362711837L,
            "D01000000010011",
            8422273604115462710L,
            "d00111000011110",
            -2528761975013221124L,
            "300111010000000",
            -5512387536280560513L,
            "s11100000000001",
            -5749656451579835857L,
            "s00001101010001",
            5569654857969679538L,
            "T01010010110110",
            -1563884000447039930L,
            "610010010111111"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            String base64Str = RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(entry.getKey(), 20);
            assertEquals(entry.getValue(), base64Str);
            assertEquals(15, entry.getValue().length());
            assertEquals(longToUrlBase64(entry.getKey()).charAt(0), base64Str.charAt(0));
        }

        int iters = randomInt(1000);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            assertEquals(RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(value, 20).charAt(0), longToUrlBase64(value).charAt(0));
        }
    }

    public void testLongToCompositeUrlBase64AndBinaryEncoding() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "s1100100101010001110111011101001000000001101010101101001100",
            -5878421770170594047L,
            "r1001101011101001101000101110010101000011110000110100000001",
            -5147010836697060622L,
            "u0010010010001001001110100111111111100101011110101011110010",
            937096430362711837L,
            "D0100000001001111000011110100001100000011100101011100011101",
            8422273604115462710L,
            "d0011100001111011010011100001000110011100110111101000110110",
            -2528761975013221124L,
            "30011101000000010000110000110110101110100100101110011111100",
            -5512387536280560513L,
            "s1110000000000100001011110111011011101101001101110001111111",
            -5749656451579835857L,
            "s0000110101000111011110101110010111000011010000101000101111",
            5569654857969679538L,
            "T0101001011011000111001010110000010110011111011110010110010",
            -1563884000447039930L,
            "61001001011111101111100100110010011011011111111011001000110"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            Long hashValue = entry.getKey();
            String expectedCompositeEncoding = entry.getValue();
            String actualCompositeEncoding = longToCompositeBase64AndBinaryEncoding(hashValue, 64);
            assertEquals(expectedCompositeEncoding, actualCompositeEncoding);
            assertEquals(59, expectedCompositeEncoding.length());
            assertEquals(longToUrlBase64(entry.getKey()).charAt(0), actualCompositeEncoding.charAt(0));
            assertEquals(RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(hashValue, 20), actualCompositeEncoding.substring(0, 15));

            Long computedHashValue = compositeUrlBase64BinaryEncodingToLong(actualCompositeEncoding);
            assertEquals(hashValue, computedHashValue);
        }

        int iters = randomInt(1000);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            String compositeEncoding = longToCompositeBase64AndBinaryEncoding(value, 64);
            assertEquals(value, compositeUrlBase64BinaryEncodingToLong(compositeEncoding));
        }
    }

    public void testGetRemoteStoreRepoNameWithRemoteNodes() {
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode("1")).build();
        Map<String, String> expected = new HashMap<>();
        expected.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, MOCK_SEGMENT_REPO_NAME);
        expected.put(RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, MOCK_TLOG_REPO_NAME);
        assertEquals(expected, RemoteStoreUtils.getRemoteStoreRepoName(discoveryNodes));
    }

    public void testGetRemoteStoreRepoNameWithDocrepNdoes() {
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeDiscoNode("1")).build();
        assertTrue(RemoteStoreUtils.getRemoteStoreRepoName(discoveryNodes).isEmpty());
    }

    static long compositeUrlBase64BinaryEncodingToLong(String encodedValue) {
        char ch = encodedValue.charAt(0);
        int base64BitsIntValue = BASE64_CHARSET_IDX_MAP.get(ch);
        String base64PartBinary = Integer.toBinaryString(base64BitsIntValue);
        String binaryString = base64PartBinary + encodedValue.substring(1);
        return new BigInteger(binaryString, 2).longValue();
    }

    public void testDeterdetermineTranslogMetadataEnabledWhenTrue() {
        Metadata metadata = createIndexMetadataWithRemoteStoreSettings(index, 1);
        IndexMetadata indexMetadata = metadata.index(index);
        assertTrue(determineTranslogMetadataEnabled(indexMetadata));
    }

    public void testDeterdetermineTranslogMetadataEnabledWhenFalse() {
        Metadata metadata = createIndexMetadataWithRemoteStoreSettings(index, 0);
        IndexMetadata indexMetadata = metadata.index(index);
        assertFalse(determineTranslogMetadataEnabled(indexMetadata));
    }

    public void testDeterdetermineTranslogMetadataEnabledWhenKeyNotFound() {
        Metadata metadata = createIndexMetadataWithRemoteStoreSettings(index, 2);
        IndexMetadata indexMetadata = metadata.index(index);
        assertThrows(AssertionError.class, () -> determineTranslogMetadataEnabled(indexMetadata));
    }

    private static Metadata createIndexMetadataWithRemoteStoreSettings(String indexName, int option) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_15_0)
                .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                .put(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "dummy-tlog-repo")
                .put(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.getKey(), "dummy-segment-repo")
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT")
                .build()
        ).putCustom(REMOTE_STORE_CUSTOM_KEY, getCustomDataMap(option)).build();
        return Metadata.builder().put(indexMetadata).build();
    }

    private static Map<String, String> getCustomDataMap(int option) {
        if (option > 1) {
            return Map.of();
        }
        String value = (option == 1) ? "true" : "false";
        return Map.of(
            RemoteStoreEnums.PathType.NAME,
            "dummy",
            RemoteStoreEnums.PathHashAlgorithm.NAME,
            "dummy",
            IndexMetadata.TRANSLOG_METADATA_KEY,
            value
        );
    }

    public void testFinalizeMigrationWithAllRemoteNodes() {
        String migratedIndex = "migrated-index";
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        DiscoveryNode remoteNode1 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNode remoteNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();
        Metadata docrepIdxMetadata = createIndexMetadataWithDocrepSettings(migratedIndex);
        assertDocrepSettingsApplied(docrepIdxMetadata.index(migratedIndex));
        Metadata remoteIndexMd = Metadata.builder(docrepIdxMetadata).persistentSettings(mockSettings).build();
        ClusterState clusterStateWithDocrepIndexSettings = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(remoteIndexMd)
            .nodes(discoveryNodes)
            .routingTable(createRoutingTableAllShardsStarted(migratedIndex, 1, 1, remoteNode1, remoteNode2))
            .build();
        Metadata mutatedMetadata = finalizeMigration(clusterStateWithDocrepIndexSettings, logger).metadata();
        assertTrue(mutatedMetadata.index(migratedIndex).getVersion() > docrepIdxMetadata.index(migratedIndex).getVersion());
        assertRemoteSettingsApplied(mutatedMetadata.index(migratedIndex));
    }

    public void testFinalizeMigrationWithAllDocrepNodes() {
        String docrepIndex = "docrep-index";
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        DiscoveryNode docrepNode1 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode docrepNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(docrepNode1)
            .localNodeId(docrepNode1.getId())
            .add(docrepNode2)
            .localNodeId(docrepNode2.getId())
            .build();
        Metadata docrepIdxMetadata = createIndexMetadataWithDocrepSettings(docrepIndex);
        assertDocrepSettingsApplied(docrepIdxMetadata.index(docrepIndex));
        Metadata remoteIndexMd = Metadata.builder(docrepIdxMetadata).persistentSettings(mockSettings).build();
        ClusterState clusterStateWithDocrepIndexSettings = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(remoteIndexMd)
            .nodes(discoveryNodes)
            .routingTable(createRoutingTableAllShardsStarted(docrepIndex, 1, 1, docrepNode1, docrepNode2))
            .build();
        Metadata mutatedMetadata = finalizeMigration(clusterStateWithDocrepIndexSettings, logger).metadata();
        assertEquals(docrepIdxMetadata.index(docrepIndex).getVersion(), mutatedMetadata.index(docrepIndex).getVersion());
        assertDocrepSettingsApplied(mutatedMetadata.index(docrepIndex));
    }

    public void testIsSwitchToStrictCompatibilityMode() {
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(mockSettings);
        assertTrue(isSwitchToStrictCompatibilityMode(request));

        mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed").build();
        request.persistentSettings(mockSettings);
        assertFalse(isSwitchToStrictCompatibilityMode(request));
    }

    private void assertRemoteSettingsApplied(IndexMetadata indexMetadata) {
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.SEGMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private void assertDocrepSettingsApplied(IndexMetadata indexMetadata) {
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.DOCUMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private RoutingTable createRoutingTableAllShardsStarted(
        String indexName,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode primaryHostingNode,
        DiscoveryNode replicaHostingNode
    ) {
        RoutingTable.Builder builder = RoutingTable.builder();
        Index index = new Index(indexName, UUID.randomUUID().toString());

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTable.addShard(
                TestShardRouting.newShardRouting(shardId, primaryHostingNode.getId(), true, ShardRoutingState.STARTED)
            );
            for (int j = 0; j < numberOfReplicas; j++) {
                indexShardRoutingTable.addShard(
                    TestShardRouting.newShardRouting(shardId, replicaHostingNode.getId(), false, ShardRoutingState.STARTED)
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.build());
        }
        return builder.add(indexRoutingTableBuilder.build()).build();
    }

    private Map<String, String> getRemoteStoreNodeAttributes() {
        Map<String, String> remoteStoreNodeAttributes = new HashMap<>();
        remoteStoreNodeAttributes.put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-cluster-repo-1");
        remoteStoreNodeAttributes.put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-segment-repo-1");
        remoteStoreNodeAttributes.put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-translog-repo-1");
        return remoteStoreNodeAttributes;
    }

    private void setupRemotePinnedTimestampFeature(boolean enabled) {
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), enabled).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    public void testGetPinnedTimestampLockedFilesFeatureDisabled() {
        setupRemotePinnedTimestampFeature(false);
        // Pinned timestamps 800, 900, 1000, 2000
        // Metadata with timestamp 990, 995, 1000, 1001
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        // Metadata timestamp 1001 <= Pinned Timestamp 2000
        Map<Long, String> metadataFilePinnedTimestampCache = new HashMap<>();
        Tuple<Map<Long, String>, Set<String>> metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L),
            Set.of(800L, 900L, 1000L, 2000L),
            metadataFilePinnedTimestampCache
        );
        Map<Long, String> metadataFiles = metadataAndLocks.v1();
        Set<String> implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(0, implicitLockedFiles.size());
        assertEquals(0, metadataFilePinnedTimestampCache.size());
    }

    public void testGetPinnedTimestampLockedFilesWithEmptyMetadataFiles() {
        setupRemotePinnedTimestampFeature(true);
        List<String> metadataFiles = Collections.emptyList();
        Set<Long> pinnedTimestampSet = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            new HashMap<>(),
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        assertTrue(implicitLockedFiles.isEmpty());
    }

    public void testGetPinnedTimestampLockedFilesWithNoPinnedTimestamps() {
        setupRemotePinnedTimestampFeature(true);
        List<String> metadataFiles = Arrays.asList("file1.txt", "file2.txt", "file3.txt");
        Set<Long> pinnedTimestampSet = Collections.emptySet();
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            new HashMap<>(),
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        assertTrue(implicitLockedFiles.isEmpty());
    }

    public void testGetPinnedTimestampLockedFilesWithNullMetadataFiles() {
        setupRemotePinnedTimestampFeature(true);
        List<String> metadataFiles = null;
        Set<Long> pinnedTimestampSet = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            new HashMap<>(),
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        assertTrue(implicitLockedFiles.isEmpty());
    }

    public void testGetPinnedTimestampLockedFilesWithNullPinnedTimestampSet() {
        setupRemotePinnedTimestampFeature(true);
        List<String> metadataFiles = Arrays.asList("file1.txt", "file2.txt", "file3.txt");
        Set<Long> pinnedTimestampSet = null;
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            new HashMap<>(),
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
            RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        assertTrue(implicitLockedFiles.isEmpty());
    }

    private Tuple<Map<Long, String>, Set<String>> testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
        List<Long> metadataFileTimestamps,
        Set<Long> pinnedTimetamps,
        Map<Long, String> metadataFilePinnedTimestampCache
    ) {
        String metadataPrefix = "metadata__1__2__3__4__5__";
        Map<Long, String> metadataFiles = new HashMap<>();
        for (Long metadataFileTimestamp : metadataFileTimestamps) {
            metadataFiles.put(metadataFileTimestamp, metadataPrefix + RemoteStoreUtils.invertLong(metadataFileTimestamp) + "__1");
        }
        return new Tuple<>(
            metadataFiles,
            RemoteStoreUtils.getPinnedTimestampLockedFiles(
                new ArrayList<>(metadataFiles.values()),
                pinnedTimetamps,
                metadataFilePinnedTimestampCache,
                RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
                RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
            )
        );
    }

    private Tuple<Map<Long, String>, Set<String>> testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
        Map<Long, Long> metadataFileTimestampsPrimaryTermMap,
        Set<Long> pinnedTimetamps,
        Map<Long, String> metadataFilePinnedTimestampCache
    ) {
        setupRemotePinnedTimestampFeature(true);
        Map<Long, String> metadataFiles = new HashMap<>();
        for (Map.Entry<Long, Long> metadataFileTimestampPrimaryTerm : metadataFileTimestampsPrimaryTermMap.entrySet()) {
            String primaryTerm = RemoteStoreUtils.invertLong(metadataFileTimestampPrimaryTerm.getValue());
            String metadataPrefix = "metadata__" + primaryTerm + "__2__3__4__5__";
            long metadataFileTimestamp = metadataFileTimestampPrimaryTerm.getKey();
            metadataFiles.put(metadataFileTimestamp, metadataPrefix + RemoteStoreUtils.invertLong(metadataFileTimestamp) + "__1");
        }
        return new Tuple<>(
            metadataFiles,
            RemoteStoreUtils.getPinnedTimestampLockedFiles(
                new ArrayList<>(metadataFiles.values()),
                pinnedTimetamps,
                metadataFilePinnedTimestampCache,
                RemoteSegmentStoreDirectory.MetadataFilenameUtils::getTimestamp,
                RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
            )
        );
    }

    public void testGetPinnedTimestampLockedFilesWithPinnedTimestamps() {
        setupRemotePinnedTimestampFeature(true);

        Map<Long, String> metadataFilePinnedTimestampCache = new HashMap<>();

        // Pinned timestamps 800, 900
        // Metadata with timestamp 990
        // No metadata matches the timestamp
        Tuple<Map<Long, String>, Set<String>> metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L),
            Set.of(800L, 900L),
            metadataFilePinnedTimestampCache
        );
        Map<Long, String> metadataFiles = metadataAndLocks.v1();
        Set<String> implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(0, implicitLockedFiles.size());
        assertEquals(0, metadataFilePinnedTimestampCache.size());

        // Pinned timestamps 800, 900, 1000
        // Metadata with timestamp 990
        // Metadata timestamp 990 <= Pinned Timestamp 1000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L),
            Set.of(800L, 900L, 1000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(990L)));
        // This is still 0 as we don't cache the latest metadata file as it can change (explained in the next test case)
        assertEquals(0, metadataFilePinnedTimestampCache.size());

        // Pinned timestamps 800, 900, 1000
        // Metadata with timestamp 990, 995
        // Metadata timestamp 995 <= Pinned Timestamp 1000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L),
            Set.of(800L, 900L, 1000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(995L)));
        // This is still 0 as we don't cache the latest metadata file as it can change
        assertEquals(0, metadataFilePinnedTimestampCache.size());

        // Pinned timestamps 800, 900, 1000
        // Metadata with timestamp 990, 995, 1000
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L),
            Set.of(800L, 900L, 1000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        // This is still 0 as we don't cache the latest metadata file as it can change
        assertEquals(0, metadataFilePinnedTimestampCache.size());

        // Pinned timestamps 800, 900, 1000, 2000
        // Metadata with timestamp 990, 995, 1000, 1001
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        // Metadata timestamp 1001 <= Pinned Timestamp 2000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L),
            Set.of(800L, 900L, 1000L, 2000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1001L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1000L), metadataFilePinnedTimestampCache.get(1000L));

        // Pinned timestamps 800, 900, 1000, 2000, 3000, 4000, 5000
        // Metadata with timestamp 990, 995, 1000, 1001
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        // Metadata timestamp 1001 <= Pinned Timestamp 2000
        // Metadata timestamp 1001 <= Pinned Timestamp 3000
        // Metadata timestamp 1001 <= Pinned Timestamp 4000
        // Metadata timestamp 1001 <= Pinned Timestamp 5000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L),
            Set.of(800L, 900L, 1000L, 2000L, 3000L, 4000L, 5000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1001L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1000L), metadataFilePinnedTimestampCache.get(1000L));

        // Pinned timestamps 800, 900, 1000, 2000, 3000, 4000, 5000
        // Metadata with timestamp 990, 995, 1000, 1001, 1900, 2300
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        // Metadata timestamp 1900 <= Pinned Timestamp 2000
        // Metadata timestamp 2300 <= Pinned Timestamp 3000
        // Metadata timestamp 2300 <= Pinned Timestamp 4000
        // Metadata timestamp 2300 <= Pinned Timestamp 5000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L, 1900L, 2300L),
            Set.of(800L, 900L, 1000L, 2000L, 3000L, 4000L, 5000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(3, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1900L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2300L)));
        // Now we cache all the matches except the last one.
        assertEquals(2, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1000L), metadataFilePinnedTimestampCache.get(1000L));
        assertEquals(metadataFiles.get(1900L), metadataFilePinnedTimestampCache.get(2000L));

        // Pinned timestamps 2000, 3000, 4000, 5000
        // Metadata with timestamp 990, 995, 1000, 1001, 1900, 2300
        // Metadata timestamp 1900 <= Pinned Timestamp 2000
        // Metadata timestamp 2300 <= Pinned Timestamp 3000
        // Metadata timestamp 2300 <= Pinned Timestamp 4000
        // Metadata timestamp 2300 <= Pinned Timestamp 5000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L, 1900L, 2300L),
            Set.of(2000L, 3000L, 4000L, 5000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1900L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2300L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1900L), metadataFilePinnedTimestampCache.get(2000L));

        // Pinned timestamps 2000, 3000, 4000, 5000
        // Metadata with timestamp 1001, 1900, 2300, 3000, 3001, 5500, 6000
        // Metadata timestamp 1900 <= Pinned Timestamp 2000
        // Metadata timestamp 3000 <= Pinned Timestamp 3000
        // Metadata timestamp 3001 <= Pinned Timestamp 4000
        // Metadata timestamp 3001 <= Pinned Timestamp 5000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(1001L, 1900L, 2300L, 3000L, 3001L, 5500L, 6000L),
            Set.of(2000L, 3000L, 4000L, 5000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(3, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1900L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(3000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(3001L)));
        // Now we cache all the matches except the last one.
        assertEquals(4, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1900L), metadataFilePinnedTimestampCache.get(2000L));
        assertEquals(metadataFiles.get(3000L), metadataFilePinnedTimestampCache.get(3000L));
        assertEquals(metadataFiles.get(3001L), metadataFilePinnedTimestampCache.get(4000L));
        assertEquals(metadataFiles.get(3001L), metadataFilePinnedTimestampCache.get(5000L));

        // Pinned timestamps 4000, 5000, 6000, 7000
        // Metadata with timestamp 2300, 3000, 3001, 5500, 6000
        // Metadata timestamp 3001 <= Pinned Timestamp 4000
        // Metadata timestamp 3001 <= Pinned Timestamp 5000
        // Metadata timestamp 6000 <= Pinned Timestamp 6000
        // Metadata timestamp 6000 <= Pinned Timestamp 7000
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(2300L, 3000L, 3001L, 5500L, 6000L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(3001L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6000L)));
        // Now we cache all the matches except the last one.
        assertEquals(2, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(3001L), metadataFilePinnedTimestampCache.get(4000L));
        assertEquals(metadataFiles.get(3001L), metadataFilePinnedTimestampCache.get(5000L));
    }

    public void testGetPinnedTimestampLockedFilesWithPinnedTimestampsDifferentPrefix() {
        setupRemotePinnedTimestampFeature(true);

        Map<Long, String> metadataFilePinnedTimestampCache = new HashMap<>();

        // Pinned timestamp 7000
        // Primary Term - Timestamp in md file
        // 6 - 7002
        // 6 - 6998
        // 5 - 6995
        // 5 - 6990
        Tuple<Map<Long, String>, Set<String>> metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            Map.of(7002L, 6L, 6998L, 6L, 6995L, 5L, 6990L, 5L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        Map<Long, String> metadataFiles = metadataAndLocks.v1();
        Set<String> implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6998L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(6998L), metadataFilePinnedTimestampCache.get(7000L));

        // Pinned timestamp 7000
        // Primary Term - Timestamp in md file
        // 6 - 7002
        // 5 - 6998
        // 5 - 6995
        // 5 - 6990
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            Map.of(7002L, 6L, 6998L, 5L, 6995L, 5L, 6990L, 5L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6998L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(6998L), metadataFilePinnedTimestampCache.get(7000L));

        // Pinned timestamp 7000
        // Primary Term - Timestamp in md file
        // 6 - 7002
        // 6 - 6998
        // 5 - 7001
        // 5 - 6990
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            Map.of(7002L, 6L, 6998L, 6L, 7001L, 5L, 6990L, 5L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6998L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(6998L), metadataFilePinnedTimestampCache.get(7000L));

        // Pinned timestamp 7000
        // Primary Term - Timestamp in md file
        // 6 - 7002
        // 5 - 7005
        // 5 - 6990
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            Map.of(7002L, 6L, 7005L, 5L, 6990L, 5L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6990L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(6990L), metadataFilePinnedTimestampCache.get(7000L));

        // Pinned timestamp 7000
        // Primary Term - Timestamp in md file
        // 6 - 6999
        // 5 - 7005
        // 5 - 6990
        metadataFilePinnedTimestampCache = new HashMap<>();
        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            Map.of(6999L, 6L, 7005L, 5L, 6990L, 5L),
            Set.of(4000L, 5000L, 6000L, 7000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(1, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(6999L)));
        // Now we cache all the matches except the last one.
        assertEquals(0, metadataFilePinnedTimestampCache.size());
    }

    public void testFilterOutMetadataFilesBasedOnAgeFeatureDisabled() {
        setupRemotePinnedTimestampFeature(false);
        List<String> metadataFiles = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            metadataFiles.add((System.currentTimeMillis() - randomIntBetween(-150000, 150000)) + "_file" + i + ".txt");
        }

        List<String> result = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> Long.valueOf(file.split("_")[0]),
            System.currentTimeMillis()
        );
        assertEquals(metadataFiles, result);
    }

    public void testFilterOutMetadataFilesBasedOnAge_AllFilesOldEnough() {
        setupRemotePinnedTimestampFeature(true);

        List<String> metadataFiles = Arrays.asList(
            (System.currentTimeMillis() - 150000) + "_file1.txt",
            (System.currentTimeMillis() - 300000) + "_file2.txt",
            (System.currentTimeMillis() - 450000) + "_file3.txt"
        );

        List<String> result = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> Long.valueOf(file.split("_")[0]),
            System.currentTimeMillis()
        );
        assertEquals(metadataFiles, result);
    }

    public void testFilterOutMetadataFilesBasedOnAge_SomeFilesTooNew() {
        setupRemotePinnedTimestampFeature(true);

        String file1 = (System.currentTimeMillis() - 150000) + "_file1.txt";
        String file2 = (System.currentTimeMillis() - 300000) + "_file2.txt";
        String file3 = (System.currentTimeMillis() + 450000) + "_file3.txt";

        List<String> metadataFiles = Arrays.asList(file1, file2, file3);

        List<String> result = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> Long.valueOf(file.split("_")[0]),
            System.currentTimeMillis()
        );
        List<String> expected = Arrays.asList(file1, file2);
        assertEquals(expected, result);
    }

    public void testFilterOutMetadataFilesBasedOnAge_AllFilesTooNew() {
        setupRemotePinnedTimestampFeature(true);

        String file1 = (System.currentTimeMillis() + 150000) + "_file1.txt";
        String file2 = (System.currentTimeMillis() + 300000) + "_file2.txt";
        String file3 = (System.currentTimeMillis() + 450000) + "_file3.txt";

        List<String> metadataFiles = Arrays.asList(file1, file2, file3);

        List<String> result = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> Long.valueOf(file.split("_")[0]),
            System.currentTimeMillis()
        );
        assertTrue(result.isEmpty());
    }

    public void testFilterOutMetadataFilesBasedOnAge_EmptyInputList() {
        setupRemotePinnedTimestampFeature(true);

        List<String> metadataFiles = Arrays.asList();

        List<String> result = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> Long.valueOf(file.split("_")[0]),
            System.currentTimeMillis()
        );
        assertTrue(result.isEmpty());
    }

    public void testIsPinnedTimestampStateStaleFeatureDisabled() {
        setupRemotePinnedTimestampFeature(false);
        assertFalse(RemoteStoreUtils.isPinnedTimestampStateStale());
    }

    public void testIsPinnedTimestampStateStaleFeatureEnabled() {
        setupRemotePinnedTimestampFeature(true);
        assertTrue(RemoteStoreUtils.isPinnedTimestampStateStale());
    }

    public void testGetPinnedTimestampLockedFilesWithCache() {
        setupRemotePinnedTimestampFeature(true);

        Map<Long, String> metadataFilePinnedTimestampCache = new HashMap<>();

        // Pinned timestamps 800, 900, 1000, 2000
        // Metadata with timestamp 990, 995, 1000, 1001
        // Metadata timestamp 1000 <= Pinned Timestamp 1000
        // Metadata timestamp 1001 <= Pinned Timestamp 2000
        Tuple<Map<Long, String>, Set<String>> metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L),
            Set.of(800L, 900L, 1000L, 2000L),
            metadataFilePinnedTimestampCache
        );
        Map<Long, String> metadataFiles = metadataAndLocks.v1();
        Set<String> implicitLockedFiles = metadataAndLocks.v2();

        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1001L)));
        // Now we cache all the matches except the last one.
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1000L), metadataFilePinnedTimestampCache.get(1000L));

        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L, 2000L, 2200L),
            Set.of(800L, 900L, 1000L, 2000L, 3000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();
        assertEquals(3, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(1000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2200L)));
        assertEquals(2, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(1000L), metadataFilePinnedTimestampCache.get(1000L));
        assertEquals(metadataFiles.get(2000L), metadataFilePinnedTimestampCache.get(2000L));

        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(990L, 995L, 1000L, 1001L, 2000L, 2200L, 2500L),
            Set.of(2000L, 3000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();
        assertEquals(2, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2000L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2500L)));
        assertEquals(1, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(2000L), metadataFilePinnedTimestampCache.get(2000L));

        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(2000L, 2200L, 2500L, 3001L, 4200L, 4600L, 5010L),
            Set.of(3000L, 4000L, 5000L, 6000L),
            metadataFilePinnedTimestampCache
        );
        metadataFiles = metadataAndLocks.v1();
        implicitLockedFiles = metadataAndLocks.v2();
        assertEquals(4, implicitLockedFiles.size());
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(2500L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(3001L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(4600L)));
        assertTrue(implicitLockedFiles.contains(metadataFiles.get(5010L)));
        assertEquals(3, metadataFilePinnedTimestampCache.size());
        assertEquals(metadataFiles.get(2500L), metadataFilePinnedTimestampCache.get(3000L));
        assertEquals(metadataFiles.get(3001L), metadataFilePinnedTimestampCache.get(4000L));
        assertEquals(metadataFiles.get(4600L), metadataFilePinnedTimestampCache.get(5000L));

        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(),
            Set.of(3000L, 4000L, 5000L, 6000L),
            metadataFilePinnedTimestampCache
        );
        implicitLockedFiles = metadataAndLocks.v2();
        assertEquals(0, implicitLockedFiles.size());
        assertEquals(3, metadataFilePinnedTimestampCache.size());

        assertThrows(
            AssertionError.class,
            () -> testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
                List.of(2000L, 2200L, 3001L, 4200L, 4600L, 5010L),
                Set.of(3000L, 4000L, 5000L, 6000L),
                metadataFilePinnedTimestampCache
            )
        );

        metadataAndLocks = testGetPinnedTimestampLockedFilesWithPinnedTimestamps(
            List.of(2000L, 2200L, 2500L, 3001L, 4200L, 4600L, 5010L),
            Set.of(),
            metadataFilePinnedTimestampCache
        );
        implicitLockedFiles = metadataAndLocks.v2();
        assertEquals(0, implicitLockedFiles.size());
        assertEquals(0, metadataFilePinnedTimestampCache.size());
    }
}
