/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_CUSTOM_KEY;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;

/**
 * Utils for remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreUtils {
    private static final Logger logger = LogManager.getLogger(RemoteStoreUtils.class);
    public static final int LONG_MAX_LENGTH = String.valueOf(Long.MAX_VALUE).length();

    /**
     * URL safe base 64 character set. This must not be changed as this is used in deriving the base64 equivalent of binary.
     */
    static final char[] URL_BASE64_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    /**
     * This method subtracts given numbers from Long.MAX_VALUE and returns a string representation of the result.
     * The resultant string is guaranteed to be of the same length that of Long.MAX_VALUE. If shorter, we add left padding
     * of 0s to the string.
     *
     * @param num number to get the inverted long string for
     * @return String value of Long.MAX_VALUE - num
     */
    public static String invertLong(long num) {
        if (num < 0) {
            throw new IllegalArgumentException("Negative long values are not allowed");
        }
        String invertedLong = String.valueOf(Long.MAX_VALUE - num);
        char[] characterArray = new char[LONG_MAX_LENGTH - invertedLong.length()];
        Arrays.fill(characterArray, '0');

        return new String(characterArray) + invertedLong;
    }

    /**
     * This method converts the given string into long and subtracts it from Long.MAX_VALUE
     *
     * @param str long in string format to be inverted
     * @return long value of the invert result
     */
    public static long invertLong(String str) {
        long num = Long.parseLong(str);
        if (num < 0) {
            throw new IllegalArgumentException("Strings representing negative long values are not allowed");
        }
        return Long.MAX_VALUE - num;
    }

    /**
     * Extracts the segment name from the provided segment file name
     *
     * @param filename Segment file name to parse
     * @return Name of the segment that the segment file belongs to
     */
    public static String getSegmentName(String filename) {
        // Segment file names follow patterns like "_0.cfe" or "_0_1_Lucene90_0.dvm".
        // Here, the segment name is "_0", which is the set of characters
        // starting with "_" until the next "_" or first ".".
        int endIdx = filename.indexOf('_', 1);
        if (endIdx == -1) {
            endIdx = filename.indexOf('.');
        }

        if (endIdx == -1) {
            throw new IllegalArgumentException("Unable to infer segment name for segment file " + filename);
        }

        return filename.substring(0, endIdx);
    }

    /**
     * @param mdFiles List of segment/translog metadata files
     * @param fn      Function to extract PrimaryTerm_Generation and Node Id from metadata file name .
     *                fn returns null if node id is not part of the file name
     */
    public static void verifyNoMultipleWriters(List<String> mdFiles, Function<String, Tuple<String, String>> fn) {
        Map<String, String> nodesByPrimaryTermAndGen = new HashMap<>();
        mdFiles.forEach(mdFile -> {
            Tuple<String, String> nodeIdByPrimaryTermAndGen = fn.apply(mdFile);
            if (nodeIdByPrimaryTermAndGen != null) {
                if (nodesByPrimaryTermAndGen.containsKey(nodeIdByPrimaryTermAndGen.v1())
                    && (!nodesByPrimaryTermAndGen.get(nodeIdByPrimaryTermAndGen.v1()).equals(nodeIdByPrimaryTermAndGen.v2()))) {
                    throw new IllegalStateException(
                        "Multiple metadata files from different nodes"
                            + "having same primary term and generations "
                            + nodeIdByPrimaryTermAndGen.v1()
                            + " detected "
                    );
                }
                nodesByPrimaryTermAndGen.put(nodeIdByPrimaryTermAndGen.v1(), nodeIdByPrimaryTermAndGen.v2());
            }
        });
    }

    /**
     * Converts an input hash which occupies 64 bits of space into Base64 (6 bits per character) String. This must not
     * be changed as it is used for creating path for storing remote store data on the remote store.
     * This converts the byte array to base 64 string. `/` is replaced with `_`, `+` is replaced with `-` and `=`
     * which is padded at the last is also removed. These characters are either used as delimiter or special character
     * requiring special handling in some vendors. The characters present in this base64 version are [A-Za-z0-9_-].
     * This must not be changed as it is used for creating path for storing remote store data on the remote store.
     */
    static String longToUrlBase64(long value) {
        byte[] hashBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        String base64Str = Base64.getUrlEncoder().encodeToString(hashBytes);
        return base64Str.substring(0, base64Str.length() - 1);
    }

    static long urlBase64ToLong(String base64Str) {
        byte[] hashBytes = Base64.getUrlDecoder().decode(base64Str);
        return ByteBuffer.wrap(hashBytes).getLong();
    }

    /**
     * Converts an input hash which occupies 64 bits of memory into a composite encoded string. The string will have 2 parts -
     * 1. Base 64 string and 2. Binary String. We will use the first 6 bits for creating the base 64 string.
     * For the second part, the rest of the bits (of length {@code len}-6) will be used as is in string form.
     */
    static String longToCompositeBase64AndBinaryEncoding(long value, int len) {
        if (len < 7 || len > 64) {
            throw new IllegalArgumentException("In longToCompositeBase64AndBinaryEncoding, len must be between 7 and 64 (both inclusive)");
        }
        String binaryEncoding = String.format(Locale.ROOT, "%64s", Long.toBinaryString(value)).replace(' ', '0');
        String base64Part = binaryEncoding.substring(0, 6);
        String binaryPart = binaryEncoding.substring(6, len);
        int base64DecimalValue = Integer.valueOf(base64Part, 2);
        assert base64DecimalValue >= 0 && base64DecimalValue < 64;
        return URL_BASE64_CHARSET[base64DecimalValue] + binaryPart;
    }

    /**
     * Fetches segment and translog repository names from remote store node attributes
     * @param discoveryNodes Current set of {@link DiscoveryNodes} in the cluster
     * @return {@link Tuple} with segment repository name as first element and translog repository name as second element
     */
    public static Map<String, String> getRemoteStoreRepoName(DiscoveryNodes discoveryNodes) {
        Optional<DiscoveryNode> remoteNode = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(DiscoveryNode::isRemoteStoreNode)
            .findFirst();
        assert remoteNode.isPresent() : "Cannot fetch remote store repository names as no remote nodes are present in the cluster";
        return remoteNode.get().getRemoteStoreRepoNames();
    }

    public static boolean hasAtLeastOneRemoteNode(DiscoveryNodes discoveryNodes) {
        return discoveryNodes.getNodes().values().stream().anyMatch(DiscoveryNode::isRemoteStoreNode);
    }

    /**
     * Utils for checking and mutating cluster state during remote migration
     *
     * @opensearch.internal
     */
    public static class RemoteMigrationClusterStateUtils {
        /**
         * During docrep to remote store migration, applies the following remote store based index settings
         * once all shards of an index have moved over to remote store enabled nodes
         * <br>
         * Also appends the requisite Remote Store Path based custom metadata to the existing index metadata
         */
        public static void maybeAddRemoteIndexSettings(
            IndexMetadata indexMetadata,
            IndexMetadata.Builder indexMetadataBuilder,
            RoutingTable routingTable,
            String index,
            DiscoveryNodes discoveryNodes,
            String segmentRepoName,
            String tlogRepoName
        ) {
            Settings currentIndexSettings = indexMetadata.getSettings();
            if (needsRemoteIndexSettingsUpdate(routingTable.indicesRouting().get(index), discoveryNodes, currentIndexSettings)) {
                logger.info(
                    "Index {} does not have remote store based index settings but all primary shards and STARTED replica shards have moved to remote enabled nodes. Applying remote store settings to the index",
                    index
                );
                Settings.Builder indexSettingsBuilder = Settings.builder().put(currentIndexSettings);
                MetadataCreateIndexService.updateRemoteStoreSettings(indexSettingsBuilder, segmentRepoName, tlogRepoName);
                indexMetadataBuilder.settings(indexSettingsBuilder);
                indexMetadataBuilder.settingsVersion(1 + indexMetadata.getVersion());
            } else {
                logger.debug("Index does not satisfy criteria for applying remote store settings");
            }
        }

        /**
         * Returns <code>true</code> iff all the below conditions are true:
         * - All primary shards are in {@link ShardRoutingState#STARTED} state and are in remote store enabled nodes
         * - No replica shard in {@link ShardRoutingState#INITIALIZING} or {@link ShardRoutingState#RELOCATING} state
         * - All {@link ShardRoutingState#STARTED} replica shards are in remote store enabled nodes
         *
         *
         * @param indexRoutingTable current {@link IndexRoutingTable} from cluster state
         * @param discoveryNodes set of discovery nodes from cluster state
         * @param currentIndexSettings current {@link IndexMetadata} from cluster state
         * @return <code>true</code> or <code>false</code> depending on the met conditions
         */
        public static boolean needsRemoteIndexSettingsUpdate(
            IndexRoutingTable indexRoutingTable,
            DiscoveryNodes discoveryNodes,
            Settings currentIndexSettings
        ) {
            assert currentIndexSettings != null : "IndexMetadata for a shard cannot be null";
            if (indexHasRemoteStoreSettings(currentIndexSettings) == false) {
                boolean allPrimariesStartedAndOnRemote = indexRoutingTable.shardsMatchingPredicate(ShardRouting::primary)
                    .stream()
                    .allMatch(
                        shardRouting -> shardRouting.started() && discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode()
                    );
                List<ShardRouting> replicaShards = indexRoutingTable.shardsMatchingPredicate(
                    shardRouting -> shardRouting.primary() == false
                );
                boolean noRelocatingReplicas = replicaShards.stream().noneMatch(ShardRouting::relocating);
                boolean allStartedReplicasOnRemote = replicaShards.stream()
                    .filter(ShardRouting::started)
                    .allMatch(shardRouting -> discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                return allPrimariesStartedAndOnRemote && noRelocatingReplicas && allStartedReplicasOnRemote;
            }
            return false;
        }

        /**
         * Updates the remote store path strategy metadata for the index when it is migrating to remote.
         * This should be run only when the first primary copy moves over from docrep to remote.
         * Checks are in place to make this execution no-op if the index metadata is already present
         *
         * @param indexMetadata Current {@link IndexMetadata}
         * @param indexMetadataBuilder Mutated {@link IndexMetadata.Builder} having the previous state updates
         * @param index index name
         * @param discoveryNodes Current {@link DiscoveryNodes} from the cluster state
         * @param settings current cluster settings from {@link ClusterState}
         */
        public static void maybeUpdateRemoteStorePathStrategy(
            IndexMetadata indexMetadata,
            IndexMetadata.Builder indexMetadataBuilder,
            String index,
            DiscoveryNodes discoveryNodes,
            Settings settings
        ) {
            if (indexHasRemotePathMetadata(indexMetadata) == false) {
                logger.info("Adding remote store path strategy for index [{}] during migration", index);
                indexMetadataBuilder.putCustom(REMOTE_STORE_CUSTOM_KEY, createRemoteStorePathTypeMetadata(settings, discoveryNodes));
            } else {
                logger.debug("Does not match criteria to update remote store path type for index {}", index);
            }
        }

        /**
         * Generates the remote store path type information to be added to custom data of index metadata.
         *
         * @param settings Current Cluster settings from {@link ClusterState}
         * @param discoveryNodes Current {@link DiscoveryNodes} from the cluster state
         * @return {@link Map} to be added as custom data in index metadata
         */
        public static Map<String, String> createRemoteStorePathTypeMetadata(Settings settings, DiscoveryNodes discoveryNodes) {
            Version minNodeVersion = discoveryNodes.getMinNodeVersion();
            RemoteStoreEnums.PathType pathType = Version.CURRENT.compareTo(minNodeVersion) <= 0
                ? CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.get(settings)
                : RemoteStoreEnums.PathType.FIXED;
            RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = pathType == RemoteStoreEnums.PathType.FIXED
                ? null
                : CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.get(settings);
            Map<String, String> remoteCustomData = new HashMap<>();
            remoteCustomData.put(RemoteStoreEnums.PathType.NAME, pathType.name());
            if (Objects.nonNull(pathHashAlgorithm)) {
                remoteCustomData.put(RemoteStoreEnums.PathHashAlgorithm.NAME, pathHashAlgorithm.name());
            }
            return remoteCustomData;
        }

        public static boolean indexHasAllRemoteStoreRelatedMetadata(IndexMetadata indexMetadata) {
            return indexHasRemoteStoreSettings(indexMetadata.getSettings()) && indexHasRemotePathMetadata(indexMetadata);
        }

        /**
         * Assert current index settings have:
         * - index.remote_store.enabled == true
         * - index.remote_store.segment.repository != null
         * - index.remote_store.translog.repository != null
         * - index.replication.type == SEGMENT
         *
         * @param indexSettings Current index settings
         * @return <code>true</code> if all above conditions match. <code>false</code> otherwise
         */
        public static boolean indexHasRemoteStoreSettings(Settings indexSettings) {
            return IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(indexSettings)
                && IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexSettings)
                && IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexSettings)
                && IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexSettings) == ReplicationType.SEGMENT;
        }

        /**
         * Asserts current index metadata customs has the {@link IndexMetadata#REMOTE_STORE_CUSTOM_KEY} key.
         * If it does, checks if the following sub-keys are present
         * - path_type
         * - path_hash_algorithm
         *
         * @param indexMetadata Current index metadata
         * @return <code>true</code> if all above conditions match. <code>false</code> otherwise
         */
        public static boolean indexHasRemotePathMetadata(IndexMetadata indexMetadata) {
            Map<String, String> customMetadata = indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY);
            if (Objects.nonNull(customMetadata)) {
                return Objects.nonNull(customMetadata.get(RemoteStoreEnums.PathType.NAME))
                    && Objects.nonNull(customMetadata.get(RemoteStoreEnums.PathHashAlgorithm.NAME));
            }
            return false;
        }
    }
}
