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
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdater.indexHasRemoteStoreSettings;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA;

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
     * Determines the remote store path strategy by reading the custom data map in IndexMetadata class.
     */
    public static RemoteStorePathStrategy determineRemoteStorePathStrategy(IndexMetadata indexMetadata) {
        Map<String, String> remoteCustomData = indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assert remoteCustomData == null || remoteCustomData.containsKey(RemoteStoreEnums.PathType.NAME);
        if (remoteCustomData != null && remoteCustomData.containsKey(RemoteStoreEnums.PathType.NAME)) {
            RemoteStoreEnums.PathType pathType = RemoteStoreEnums.PathType.parseString(
                remoteCustomData.get(RemoteStoreEnums.PathType.NAME)
            );
            String hashAlgoStr = remoteCustomData.get(RemoteStoreEnums.PathHashAlgorithm.NAME);
            RemoteStoreEnums.PathHashAlgorithm hashAlgorithm = Objects.nonNull(hashAlgoStr)
                ? RemoteStoreEnums.PathHashAlgorithm.parseString(hashAlgoStr)
                : null;
            return new RemoteStorePathStrategy(pathType, hashAlgorithm);
        }
        return new RemoteStorePathStrategy(RemoteStoreEnums.PathType.FIXED);
    }

    /**
     * Determines if translog file object metadata can be used to store checkpoint file data.
     */
    public static boolean determineTranslogMetadataEnabled(IndexMetadata indexMetadata) {
        Map<String, String> remoteCustomData = indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assert remoteCustomData == null || remoteCustomData.containsKey(IndexMetadata.TRANSLOG_METADATA_KEY);
        if (remoteCustomData != null && remoteCustomData.containsKey(IndexMetadata.TRANSLOG_METADATA_KEY)) {
            return Boolean.parseBoolean(remoteCustomData.get(IndexMetadata.TRANSLOG_METADATA_KEY));
        }
        return false;
    }

    /**
     * Generates the remote store path type information to be added to custom data of index metadata during migration
     *
     * @param clusterSettings Current Cluster settings from {@link ClusterState}
     * @param discoveryNodes  Current {@link DiscoveryNodes} from the cluster state
     * @return {@link Map} to be added as custom data in index metadata
     */
    public static Map<String, String> determineRemoteStoreCustomMetadataDuringMigration(
        Settings clusterSettings,
        DiscoveryNodes discoveryNodes
    ) {
        Map<String, String> remoteCustomData = new HashMap<>();
        Version minNodeVersion = discoveryNodes.getMinNodeVersion();

        // TODO: During the document replication to a remote store migration, there should be a check to determine if the registered
        // translog blobstore supports custom metadata or not.
        // Currently, the blobStoreMetadataEnabled flag is set to false because the integration tests run on the local file system, which
        // does not support custom metadata.
        // https://github.com/opensearch-project/OpenSearch/issues/13745
        boolean blobStoreMetadataEnabled = false;
        boolean translogMetadata = Version.V_2_15_0.compareTo(minNodeVersion) <= 0
            && CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.get(clusterSettings)
            && blobStoreMetadataEnabled;

        remoteCustomData.put(IndexMetadata.TRANSLOG_METADATA_KEY, Boolean.toString(translogMetadata));

        RemoteStoreEnums.PathType pathType = Version.V_2_15_0.compareTo(minNodeVersion) <= 0
            ? CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.get(clusterSettings)
            : RemoteStoreEnums.PathType.FIXED;
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = pathType == RemoteStoreEnums.PathType.FIXED
            ? null
            : CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.get(clusterSettings);
        remoteCustomData.put(RemoteStoreEnums.PathType.NAME, pathType.name());
        if (Objects.nonNull(pathHashAlgorithm)) {
            remoteCustomData.put(RemoteStoreEnums.PathHashAlgorithm.NAME, pathHashAlgorithm.name());
        }
        return remoteCustomData;
    }

    /**
     * Fetches segment and translog repository names from remote store node attributes.
     * Returns a blank {@link HashMap} if the cluster does not contain any remote nodes.
     * <br>
     * Caller need to handle null checks if {@link DiscoveryNodes} object does not have any remote nodes
     *
     * @param discoveryNodes Current set of {@link DiscoveryNodes} in the cluster
     * @return {@link Map} of data repository node attributes keys and their values
     */
    public static Map<String, String> getRemoteStoreRepoName(DiscoveryNodes discoveryNodes) {
        Optional<DiscoveryNode> remoteNode = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(DiscoveryNode::isRemoteStoreNode)
            .findFirst();
        return remoteNode.map(RemoteStoreNodeAttribute::getDataRepoNames).orElseGet(HashMap::new);
    }

    /**
     * Invoked after a cluster settings update.
     * Checks if the applied cluster settings has switched the cluster to STRICT mode.
     * If so, checks and applies appropriate index settings depending on the current set
     * of node types in the cluster
     * This has been intentionally done after the cluster settings update
     * flow. That way we are not interfering with the usual settings update
     * and the cluster state mutation that comes along with it
     *
     * @param isCompatibilityModeChanging flag passed from cluster settings update call to denote if a compatibility mode change has been done
     * @param request request payload passed from cluster settings update
     * @param currentState cluster state generated after changing cluster settings were applied
     * @param logger Logger reference
     * @return Mutated cluster state with remote store index settings applied, no-op if the cluster is not switching to `STRICT` compatibility mode
     */
    public static ClusterState checkAndFinalizeRemoteStoreMigration(
        boolean isCompatibilityModeChanging,
        ClusterUpdateSettingsRequest request,
        ClusterState currentState,
        Logger logger
    ) {
        if (isCompatibilityModeChanging && isSwitchToStrictCompatibilityMode(request)) {
            return finalizeMigration(currentState, logger);
        }
        return currentState;
    }

    /**
     * Finalizes the docrep to remote-store migration process by applying remote store based index settings
     * on indices that are missing them. No-Op if all indices already have the settings applied through
     * IndexMetadataUpdater
     *
     * @param incomingState mutated cluster state after cluster settings were applied
     * @return new cluster state with index settings updated
     */
    public static ClusterState finalizeMigration(ClusterState incomingState, Logger logger) {
        Map<String, DiscoveryNode> discoveryNodeMap = incomingState.nodes().getNodes();
        if (discoveryNodeMap.isEmpty() == false) {
            // At this point, we have already validated that all nodes in the cluster are of uniform type.
            // Either all of them are remote store enabled, or all of them are docrep enabled
            boolean remoteStoreEnabledNodePresent = discoveryNodeMap.values().stream().findFirst().get().isRemoteStoreNode();
            if (remoteStoreEnabledNodePresent == true) {
                List<IndexMetadata> indicesWithoutRemoteStoreSettings = getIndicesWithoutRemoteStoreSettings(incomingState, logger);
                if (indicesWithoutRemoteStoreSettings.isEmpty() == true) {
                    logger.info("All indices in the cluster has remote store based index settings");
                } else {
                    Metadata mutatedMetadata = applyRemoteStoreSettings(incomingState, indicesWithoutRemoteStoreSettings, logger);
                    return ClusterState.builder(incomingState).metadata(mutatedMetadata).build();
                }
            } else {
                logger.debug("All nodes in the cluster are not remote nodes. Skipping.");
            }
        }
        return incomingState;
    }

    /**
     * Filters out indices which does not have remote store based
     * index settings applied even after all shard copies have
     * migrated to remote store enabled nodes
     */
    private static List<IndexMetadata> getIndicesWithoutRemoteStoreSettings(ClusterState clusterState, Logger logger) {
        Collection<IndexMetadata> allIndicesMetadata = clusterState.metadata().indices().values();
        if (allIndicesMetadata.isEmpty() == false) {
            List<IndexMetadata> indicesWithoutRemoteSettings = allIndicesMetadata.stream()
                .filter(idxMd -> indexHasRemoteStoreSettings(idxMd.getSettings()) == false)
                .collect(Collectors.toList());
            logger.debug(
                "Attempting to switch to strict mode. Count of indices without remote store settings {}",
                indicesWithoutRemoteSettings.size()
            );
            return indicesWithoutRemoteSettings;
        }
        return Collections.emptyList();
    }

    /**
     * Applies remote store index settings through {@link RemoteMigrationIndexMetadataUpdater}
     */
    private static Metadata applyRemoteStoreSettings(
        ClusterState clusterState,
        List<IndexMetadata> indicesWithoutRemoteStoreSettings,
        Logger logger
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.getMetadata());
        RoutingTable currentRoutingTable = clusterState.getRoutingTable();
        DiscoveryNodes currentDiscoveryNodes = clusterState.getNodes();
        Settings currentClusterSettings = clusterState.metadata().settings();
        for (IndexMetadata indexMetadata : indicesWithoutRemoteStoreSettings) {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            RemoteMigrationIndexMetadataUpdater indexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
                currentDiscoveryNodes,
                currentRoutingTable,
                indexMetadata,
                currentClusterSettings,
                logger
            );
            indexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexMetadata.getIndex().getName());
            metadataBuilder.put(indexMetadataBuilder);
        }
        return metadataBuilder.build();
    }

    /**
     * Checks if the incoming cluster settings payload is attempting to switch
     * the cluster to `STRICT` compatibility mode
     * Visible only for tests
     */
    public static boolean isSwitchToStrictCompatibilityMode(ClusterUpdateSettingsRequest request) {
        Settings incomingSettings = Settings.builder().put(request.persistentSettings()).put(request.transientSettings()).build();
        return RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(
            incomingSettings
        ) == RemoteStoreNodeService.CompatibilityMode.STRICT;
    }

    /**
     * Determines and returns a set of metadata files that match provided pinned timestamps.
     *
     * This method is an overloaded version of getPinnedTimestampLockedFiles and do not use cached entries to find
     * the metadata file
     *
     * @param metadataFiles A list of metadata file names. Expected to be sorted in descending order of timestamp.
     * @param pinnedTimestampSet A set of timestamps representing pinned points in time.
     * @param getTimestampFunction A function that extracts the timestamp from a metadata file name.
     * @param prefixFunction A function that extracts a tuple of prefix information from a metadata file name.
     * @param ignorePinnedTimestampEnabledSetting A flag to ignore pinned timestamp enabled setting
     * @return A set of metadata file names that are implicitly locked based on the pinned timestamps.
     */
    public static Set<String> getPinnedTimestampLockedFiles(
        List<String> metadataFiles,
        Set<Long> pinnedTimestampSet,
        Function<String, Long> getTimestampFunction,
        Function<String, Tuple<String, String>> prefixFunction,
        boolean ignorePinnedTimestampEnabledSetting
    ) {
        return getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            new HashMap<>(),
            getTimestampFunction,
            prefixFunction,
            ignorePinnedTimestampEnabledSetting
        );
    }

    /**
     * Determines and returns a set of metadata files that match provided pinned timestamps. If pinned timestamp
     * feature is not enabled, this function is a no-op.
     *
     * This method identifies metadata files that are considered implicitly locked due to their timestamps
     * matching or being the closest preceding timestamp to the pinned timestamps. It uses a caching mechanism
     * to improve performance for previously processed timestamps.
     *
     * The method performs the following steps:
     *           1. Validates input parameters.
     *           2. Updates the cache (metadataFilePinnedTimestampMap) to remove outdated entries.
     *           3. Processes cached entries and identifies new timestamps to process.
     *           4. For new timestamps, iterates through metadata files to find matching or closest preceding files.
     *           5. Updates the cache with newly processed timestamps and their corresponding metadata files.
     *
     * @param metadataFiles A list of metadata file names. Expected to be sorted in descending order of timestamp.
     * @param pinnedTimestampSet A set of timestamps representing pinned points in time.
     * @param metadataFilePinnedTimestampMap A map used for caching processed timestamps and their corresponding metadata files.
     * @param getTimestampFunction A function that extracts the timestamp from a metadata file name.
     * @param prefixFunction A function that extracts a tuple of prefix information from a metadata file name.
     * @return A set of metadata file names that are implicitly locked based on the pinned timestamps.
     *
     */
    public static Set<String> getPinnedTimestampLockedFiles(
        List<String> metadataFiles,
        Set<Long> pinnedTimestampSet,
        Map<Long, String> metadataFilePinnedTimestampMap,
        Function<String, Long> getTimestampFunction,
        Function<String, Tuple<String, String>> prefixFunction
    ) {
        return getPinnedTimestampLockedFiles(
            metadataFiles,
            pinnedTimestampSet,
            metadataFilePinnedTimestampMap,
            getTimestampFunction,
            prefixFunction,
            false
        );
    }

    private static Set<String> getPinnedTimestampLockedFiles(
        List<String> metadataFiles,
        Set<Long> pinnedTimestampSet,
        Map<Long, String> metadataFilePinnedTimestampMap,
        Function<String, Long> getTimestampFunction,
        Function<String, Tuple<String, String>> prefixFunction,
        boolean ignorePinnedTimestampEnabledSetting
    ) {
        Set<String> implicitLockedFiles = new HashSet<>();

        if (ignorePinnedTimestampEnabledSetting == false && RemoteStoreSettings.isPinnedTimestampsEnabled() == false) {
            return implicitLockedFiles;
        }

        if (metadataFiles == null || metadataFiles.isEmpty() || pinnedTimestampSet == null) {
            return implicitLockedFiles;
        }

        // Remove entries for timestamps that are no longer pinned
        metadataFilePinnedTimestampMap.keySet().retainAll(pinnedTimestampSet);

        // Add cached entries and collect new timestamps
        Set<Long> newPinnedTimestamps = new TreeSet<>(Collections.reverseOrder());
        for (Long pinnedTimestamp : pinnedTimestampSet) {
            String cachedFile = metadataFilePinnedTimestampMap.get(pinnedTimestamp);
            if (cachedFile != null) {
                assert metadataFiles.contains(cachedFile) : "Metadata files should contain [" + cachedFile + "]";
                implicitLockedFiles.add(cachedFile);
            } else {
                newPinnedTimestamps.add(pinnedTimestamp);
            }
        }

        if (newPinnedTimestamps.isEmpty()) {
            return implicitLockedFiles;
        }

        // Sort metadata files in descending order of timestamp
        // ToDo: Do we really need this? Files fetched from remote store are already lexicographically sorted.
        metadataFiles.sort(String::compareTo);

        // If we have metadata files from multiple writers, it can result in picking file generated by stale primary.
        // To avoid this, we fail fast.
        RemoteStoreUtils.verifyNoMultipleWriters(metadataFiles, prefixFunction);

        Iterator<Long> timestampIterator = newPinnedTimestamps.iterator();
        Long currentPinnedTimestamp = timestampIterator.next();
        long prevMdTimestamp = Long.MAX_VALUE;
        for (String metadataFileName : metadataFiles) {
            long currentMdTimestamp = getTimestampFunction.apply(metadataFileName);
            // We always prefer md file with higher values of prefix like primary term, generation etc.
            if (currentMdTimestamp > prevMdTimestamp) {
                continue;
            }
            while (currentMdTimestamp <= currentPinnedTimestamp && prevMdTimestamp > currentPinnedTimestamp) {
                implicitLockedFiles.add(metadataFileName);
                // Do not cache entry for latest metadata file as the next metadata can also match the same pinned timestamp
                if (prevMdTimestamp != Long.MAX_VALUE) {
                    metadataFilePinnedTimestampMap.put(currentPinnedTimestamp, metadataFileName);
                }
                if (timestampIterator.hasNext() == false) {
                    return implicitLockedFiles;
                }
                currentPinnedTimestamp = timestampIterator.next();
            }
            prevMdTimestamp = currentMdTimestamp;
        }

        return implicitLockedFiles;
    }

    /**
     * Filters out metadata files based on their age and pinned timestamps settings.
     *
     * This method filters a list of metadata files, keeping only those that are older
     * than a certain threshold determined by the last successful fetch of pinned timestamps
     * and a configured lookback interval.
     *
     * @param metadataFiles A list of metadata file names to be filtered.
     * @param getTimestampFunction A function that extracts a timestamp from a metadata file name.
     * @param lastSuccessfulFetchOfPinnedTimestamps The timestamp of the last successful fetch of pinned timestamps.
     * @return A new list containing only the metadata files that meet the age criteria.
     *         If pinned timestamps are not enabled, returns a copy of the input list.
     */
    public static List<String> filterOutMetadataFilesBasedOnAge(
        List<String> metadataFiles,
        Function<String, Long> getTimestampFunction,
        long lastSuccessfulFetchOfPinnedTimestamps
    ) {
        if (RemoteStoreSettings.isPinnedTimestampsEnabled() == false) {
            return new ArrayList<>(metadataFiles);
        }
        // We allow now() - loopback interval to be pinned. Also, the actual pinning can take at most loopback interval
        // This means the pinned timestamp can be available for read after at most (2 * loopback interval)
        long maximumAllowedTimestamp = lastSuccessfulFetchOfPinnedTimestamps - (2 * RemoteStoreSettings
            .getPinnedTimestampsLookbackInterval()
            .getMillis());
        List<String> metadataFilesWithMinAge = new ArrayList<>();
        for (String metadataFileName : metadataFiles) {
            long metadataTimestamp = getTimestampFunction.apply(metadataFileName);
            if (metadataTimestamp < maximumAllowedTimestamp) {
                metadataFilesWithMinAge.add(metadataFileName);
            }
        }
        return metadataFilesWithMinAge;
    }

    /**
     * Determines if the pinned timestamp state is stale.
     *
     * This method checks whether the last successful fetch of pinned timestamps
     * is considered stale based on the current time and configured intervals.
     * The state is considered stale if the last successful fetch occurred before
     * a certain threshold, which is calculated as three times the scheduler interval
     * plus the lookback interval.
     *
     * @return true if the pinned timestamp state is stale, false otherwise.
     *         Always returns false if pinned timestamps are not enabled.
     */
    public static boolean isPinnedTimestampStateStale() {
        if (RemoteStoreSettings.isPinnedTimestampsEnabled() == false) {
            return false;
        }
        long lastSuccessfulFetchTimestamp = RemoteStorePinnedTimestampService.getPinnedTimestamps().v1();
        long staleBufferInMillis = (RemoteStoreSettings.getPinnedTimestampsSchedulerInterval().millis() * 3) + RemoteStoreSettings
            .getPinnedTimestampsLookbackInterval()
            .millis();
        return lastSuccessfulFetchTimestamp < (System.currentTimeMillis() - staleBufferInMillis);
    }
}
