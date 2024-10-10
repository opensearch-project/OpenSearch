/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.PersistedStateStats;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.node.DiscoveryNodes.Builder;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.StringKeyDiffProvider;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableServiceFactory;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.model.RemoteClusterBlocks;
import org.opensearch.gateway.remote.model.RemoteClusterStateCustoms;
import org.opensearch.gateway.remote.model.RemoteClusterStateManifestInfo;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodes;
import org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.gateway.remote.routingtable.RemoteRoutingTableDiff;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.cluster.ClusterState.CUSTOM_VALUE_SERIALIZER;
import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V2;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V3;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTE;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.UploadedMetadataResults;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.clusterUUIDContainer;
import static org.opensearch.gateway.remote.model.RemoteClusterStateCustoms.CLUSTER_STATE_CUSTOM;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_METADATA_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteClusterStateConfigured;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableConfigured;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
@InternalApi
public class RemoteClusterStateService implements Closeable {

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    /**
     * Gates the functionality of remote publication.
     */
    public static final String REMOTE_PUBLICATION_SETTING_KEY = "cluster.remote_store.publication.enabled";

    public static final Setting<Boolean> REMOTE_PUBLICATION_SETTING = Setting.boolSetting(
        REMOTE_PUBLICATION_SETTING_KEY,
        false,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );

    public static final TimeValue REMOTE_STATE_READ_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> REMOTE_STATE_READ_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.read_timeout",
        REMOTE_STATE_READ_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RemoteClusterStateValidationMode> REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING = new Setting<>(
        "cluster.remote_store.state.checksum_validation.mode",
        RemoteClusterStateValidationMode.NONE.name(),
        RemoteClusterStateValidationMode::parseString,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
    * Controls the fixed prefix for the cluster state path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.state.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    /**
     * Validation mode for cluster state checksum.
     *  None: Validation will be disabled.
     *  Debug: Validation enabled but only matches checksum and logs failing entities.
     *  Trace: Matches checksum and downloads full cluster state to find diff in failing entities. Only logs failures.
     *  Failure: Throws exception on failing validation.
     */
    public enum RemoteClusterStateValidationMode {
        DEBUG("debug"),
        TRACE("trace"),
        FAILURE("failure"),
        NONE("none");

        public final String mode;

        RemoteClusterStateValidationMode(String mode) {
            this.mode = mode;
        }

        public static RemoteClusterStateValidationMode parseString(String mode) {
            try {
                return RemoteClusterStateValidationMode.valueOf(mode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + mode
                        + "] mode is not supported. "
                        + "supported modes are ["
                        + Arrays.toString(RemoteClusterStateValidationMode.values())
                        + "]"
                );
            }
        }
    }

    private TimeValue remoteStateReadTimeout;
    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private final List<IndexMetadataUploadListener> indexMetadataUploadListeners;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private RemoteRoutingTableService remoteRoutingTableService;
    private volatile TimeValue slowWriteLoggingThreshold;
    private RemoteClusterStateValidationMode remoteClusterStateValidationMode;

    private final RemotePersistenceStats remoteStateStats;
    private RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final String CLUSTER_STATE_UPLOAD_TIME_LOG_STRING = "writing cluster state for version [{}] took [{}ms]";
    private final String METADATA_UPDATE_LOG_STRING = "wrote metadata for [{}] indices and skipped [{}] unchanged "
        + "indices, coordination metadata updated : [{}], settings metadata updated : [{}], templates metadata "
        + "updated : [{}], custom metadata updated : [{}], indices routing updated : [{}]";
    private AtomicBoolean isPublicationEnabled;
    private final String remotePathPrefix;

    private final RemoteClusterStateCache remoteClusterStateCache;
    // ToXContent Params with gateway mode.
    // We are using gateway context mode to persist all custom metadata.
    public static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(1);
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterService clusterService,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool,
        List<IndexMetadataUploadListener> indexMetadataUploadListeners,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        assert isRemoteClusterStateConfigured(settings) : "Remote cluster state is not configured";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        clusterSettings = clusterService.getClusterSettings();
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.remoteStateReadTimeout = clusterSettings.get(REMOTE_STATE_READ_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STATE_READ_TIMEOUT_SETTING, this::setRemoteStateReadTimeout);
        this.remoteClusterStateValidationMode = REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING, this::setChecksumValidationMode);

        this.remoteStateStats = new RemotePersistenceStats();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.indexMetadataUploadListeners = indexMetadataUploadListeners;
        this.isPublicationEnabled = new AtomicBoolean(
            clusterSettings.get(REMOTE_PUBLICATION_SETTING)
                && RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled(settings)
                && RemoteStoreNodeAttribute.isRemoteRoutingTableConfigured(settings)
        );
        clusterSettings.addSettingsUpdateConsumer(REMOTE_PUBLICATION_SETTING, this::setRemotePublicationSetting);
        this.remotePathPrefix = CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX.get(settings);
        this.remoteRoutingTableService = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            clusterSettings,
            threadpool,
            ClusterName.CLUSTER_NAME_SETTING.get(settings).value()
        );
        remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(this, clusterService, remoteRoutingTableService);
        remoteClusterStateCache = new RemoteClusterStateCache();
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public RemoteClusterStateManifestInfo writeFullMetadata(ClusterState clusterState, String previousClusterUUID, int codecVersion)
        throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        boolean publicationEnabled = isPublicationEnabled.get();
        UploadedMetadataResults uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values()),
            emptyMap(),
            RemoteGlobalMetadataManager.filterCustoms(clusterState.metadata().customs(), publicationEnabled),
            true,
            true,
            true,
            publicationEnabled,
            publicationEnabled,
            publicationEnabled,
            publicationEnabled ? clusterState.customs() : Collections.emptyMap(),
            publicationEnabled,
            remoteRoutingTableService.getIndicesRouting(clusterState.getRoutingTable()),
            null
        );

        ClusterStateDiffManifest clusterStateDiffManifest = new ClusterStateDiffManifest(
            clusterState,
            ClusterState.EMPTY_STATE,
            MANIFEST_CURRENT_CODEC_VERSION,
            null,
            null
        );
        final RemoteClusterStateManifestInfo manifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousClusterUUID,
            clusterStateDiffManifest,
            !remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.NONE)
                ? new ClusterStateChecksum(clusterState, threadpool)
                : null,
            false,
            codecVersion
        );

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateUploadSucceeded();
        remoteStateStats.stateUploadTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote full state with [{}] indices and [{}] indicesRouting",
                durationMillis,
                slowWriteLoggingThreshold,
                uploadedMetadataResults.uploadedIndexMetadata.size(),
                uploadedMetadataResults.uploadedIndicesRoutingMetadata.size()
            );
        } else {
            logger.debug(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices, [{}] indicesRouting and global metadata",
                durationMillis,
                uploadedMetadataResults.uploadedIndexMetadata.size(),
                uploadedMetadataResults.uploadedIndicesRoutingMetadata.size()
            );
        }
        return manifestDetails;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return {@link RemoteClusterStateManifestInfo} object containing uploaded manifest detail
     */
    public RemoteClusterStateManifestInfo writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        if (previousClusterState == null) {
            throw new IllegalArgumentException("previousClusterState cannot be null");
        }
        if (clusterState == null) {
            throw new IllegalArgumentException("clusterState cannot be null");
        }
        if (previousManifest == null) {
            throw new IllegalArgumentException("previousManifest cannot be null");
        }
        logger.trace("WRITING INCREMENTAL STATE");

        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        boolean firstUploadForSplitGlobalMetadata = !previousManifest.hasMetadataAttributesFiles();

        final DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customsDiff = remoteGlobalMetadataManager
            .getCustomsDiff(clusterState, previousClusterState, firstUploadForSplitGlobalMetadata, isPublicationEnabled.get());
        final DiffableUtils.MapDiff<String, ClusterState.Custom, Map<String, ClusterState.Custom>> clusterStateCustomsDiff =
            remoteClusterStateAttributesManager.getUpdatedCustoms(clusterState, previousClusterState, isPublicationEnabled.get(), false);
        final Map<String, UploadedMetadataAttribute> allUploadedCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        final Map<String, UploadedMetadataAttribute> allUploadedClusterStateCustomsMap = new HashMap<>(
            previousManifest.getClusterStateCustomMap()
        );
        final Map<String, IndexMetadata> indicesToBeDeletedFromRemote = new HashMap<>(previousClusterState.metadata().indices());
        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();
        // We prepare a map that contains the previous index metadata for the indexes for which version has changed.
        Map<String, IndexMetadata> prevIndexMetadataByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            String indexName = indexMetadata.getIndex().getName();
            final IndexMetadata prevIndexMetadata = indicesToBeDeletedFromRemote.get(indexName);
            Long previousVersion = prevIndexMetadata != null ? prevIndexMetadata.getVersion() : null;
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.debug(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                toUpload.add(indexMetadata);
                prevIndexMetadataByName.put(indexName, prevIndexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            // index present in current cluster state
            indicesToBeDeletedFromRemote.remove(indexMetadata.getIndex().getName());
        }

        final List<IndexRoutingTable> indicesRoutingToUpload = new ArrayList<>();
        final List<String> deletedIndicesRouting = new ArrayList<>();
        final StringKeyDiffProvider<IndexRoutingTable> routingTableDiff = remoteRoutingTableService.getIndicesRoutingMapDiff(
            previousClusterState.getRoutingTable(),
            clusterState.getRoutingTable()
        );
        if (routingTableDiff != null && routingTableDiff.provideDiff() != null) {
            routingTableDiff.provideDiff()
                .getDiffs()
                .forEach((k, v) -> indicesRoutingToUpload.add(clusterState.getRoutingTable().index(k)));
            routingTableDiff.provideDiff().getUpserts().forEach((k, v) -> indicesRoutingToUpload.add(v));
            deletedIndicesRouting.addAll(routingTableDiff.provideDiff().getDeletes());
        }

        UploadedMetadataResults uploadedMetadataResults;
        // For migration case from codec V0 or V1 to V2, we have added null check on metadata attribute files,
        // If file is empty and codec is 1 then write global metadata.
        boolean updateCoordinationMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isCoordinationMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        ;
        boolean updateSettingsMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isSettingsMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        boolean updateTransientSettingsMetadata = Metadata.isTransientSettingsMetadataEqual(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        boolean updateTemplatesMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isTemplatesMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;

        final boolean updateDiscoveryNodes = isPublicationEnabled.get()
            && clusterState.getNodes().delta(previousClusterState.getNodes()).hasChanges();
        final boolean updateClusterBlocks = isPublicationEnabled.get() && !clusterState.blocks().equals(previousClusterState.blocks());
        final boolean updateHashesOfConsistentSettings = isPublicationEnabled.get()
            && Metadata.isHashesOfConsistentSettingsEqual(previousClusterState.metadata(), clusterState.metadata()) == false;

        uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            toUpload,
            prevIndexMetadataByName,
            customsDiff.getUpserts(),
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            updateDiscoveryNodes,
            updateClusterBlocks,
            updateTransientSettingsMetadata,
            clusterStateCustomsDiff.getUpserts(),
            updateHashesOfConsistentSettings,
            indicesRoutingToUpload,
            routingTableDiff
        );

        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        allUploadedClusterStateCustomsMap.putAll(uploadedMetadataResults.uploadedClusterStateCustomMetadataMap);
        // remove the data for removed custom/indices
        customsDiff.getDeletes().forEach(allUploadedCustomMap::remove);
        indicesToBeDeletedFromRemote.keySet().forEach(allUploadedIndexMetadata::remove);
        clusterStateCustomsDiff.getDeletes().forEach(allUploadedClusterStateCustomsMap::remove);

        if (!updateCoordinationMetadata) {
            uploadedMetadataResults.uploadedCoordinationMetadata = previousManifest.getCoordinationMetadata();
        }
        if (!updateSettingsMetadata) {
            uploadedMetadataResults.uploadedSettingsMetadata = previousManifest.getSettingsMetadata();
        }
        if (!updateTransientSettingsMetadata) {
            uploadedMetadataResults.uploadedTransientSettingsMetadata = previousManifest.getTransientSettingsMetadata();
        }
        if (!updateTemplatesMetadata) {
            uploadedMetadataResults.uploadedTemplatesMetadata = previousManifest.getTemplatesMetadata();
        }
        if (!updateDiscoveryNodes) {
            uploadedMetadataResults.uploadedDiscoveryNodes = previousManifest.getDiscoveryNodesMetadata();
        }
        if (!updateClusterBlocks) {
            uploadedMetadataResults.uploadedClusterBlocks = previousManifest.getClusterBlocksMetadata();
        }
        if (!updateHashesOfConsistentSettings) {
            uploadedMetadataResults.uploadedHashesOfConsistentSettings = previousManifest.getHashesOfConsistentSettings();
        }
        uploadedMetadataResults.uploadedCustomMetadataMap = allUploadedCustomMap;
        uploadedMetadataResults.uploadedClusterStateCustomMetadataMap = allUploadedClusterStateCustomsMap;
        uploadedMetadataResults.uploadedIndexMetadata = new ArrayList<>(allUploadedIndexMetadata.values());

        uploadedMetadataResults.uploadedIndicesRoutingMetadata = remoteRoutingTableService.getAllUploadedIndicesRouting(
            previousManifest,
            uploadedMetadataResults.uploadedIndicesRoutingMetadata,
            deletedIndicesRouting
        );

        ClusterStateDiffManifest clusterStateDiffManifest = new ClusterStateDiffManifest(
            clusterState,
            previousClusterState,
            MANIFEST_CURRENT_CODEC_VERSION,
            routingTableDiff,
            uploadedMetadataResults.uploadedIndicesRoutingDiffMetadata != null
                ? uploadedMetadataResults.uploadedIndicesRoutingDiffMetadata.getUploadedFilename()
                : null
        );

        final RemoteClusterStateManifestInfo manifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousManifest.getPreviousClusterUUID(),
            clusterStateDiffManifest,
            !remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.NONE)
                ? new ClusterStateChecksum(clusterState, threadpool)
                : null,
            false,
            previousManifest.getCodecVersion()
        );

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateUploadSucceeded();
        remoteStateStats.stateUploadTook(durationMillis);
        ParameterizedMessage clusterStateUploadTimeMessage = new ParameterizedMessage(
            CLUSTER_STATE_UPLOAD_TIME_LOG_STRING,
            manifestDetails.getClusterMetadataManifest().getStateVersion(),
            durationMillis
        );
        ParameterizedMessage metadataUpdateMessage = new ParameterizedMessage(
            METADATA_UPDATE_LOG_STRING,
            numIndicesUpdated,
            numIndicesUnchanged,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            customsDiff.getUpserts().size(),
            indicesRoutingToUpload.size()
        );
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            // TODO update logs to add more details about objects uploaded
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsDiff.getUpserts().size()
            );
        } else {
            logger.debug("{}; {}", clusterStateUploadTimeMessage, metadataUpdateMessage);
            logger.debug(
                "writing cluster state for version [{}] took [{}ms]; "
                    + "wrote metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                manifestDetails.getClusterMetadataManifest().getStateVersion(),
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsDiff.getUpserts().size()
            );
        }
        return manifestDetails;
    }

    // package private for testing
    UploadedMetadataResults writeMetadataInParallel(
        ClusterState clusterState,
        List<IndexMetadata> indexToUpload,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        Map<String, Metadata.Custom> customToUpload,
        boolean uploadCoordinationMetadata,
        boolean uploadSettingsMetadata,
        boolean uploadTemplateMetadata,
        boolean uploadDiscoveryNodes,
        boolean uploadClusterBlock,
        boolean uploadTransientSettingMetadata,
        Map<String, ClusterState.Custom> clusterStateCustomToUpload,
        boolean uploadHashesOfConsistentSettings,
        List<IndexRoutingTable> indicesRoutingToUpload,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff
    ) throws IOException {
        assert Objects.nonNull(indexMetadataUploadListeners) : "indexMetadataUploadListeners can not be null";
        int totalUploadTasks = indexToUpload.size() + indexMetadataUploadListeners.size() + customToUpload.size()
            + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0)
            + (uploadDiscoveryNodes ? 1 : 0) + (uploadClusterBlock ? 1 : 0) + (uploadTransientSettingMetadata ? 1 : 0)
            + clusterStateCustomToUpload.size() + (uploadHashesOfConsistentSettings ? 1 : 0) + indicesRoutingToUpload.size()
            + ((routingTableDiff != null
                && routingTableDiff.provideDiff() != null
                && (!routingTableDiff.provideDiff().getDiffs().isEmpty()
                    || !routingTableDiff.provideDiff().getDeletes().isEmpty()
                    || !routingTableDiff.provideDiff().getUpserts().isEmpty())) ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        List<String> uploadTasks = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new ConcurrentHashMap<>(totalUploadTasks);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(
            ActionListener.wrap((ClusterMetadataManifest.UploadedMetadata uploadedMetadata) -> {
                logger.trace(String.format(Locale.ROOT, "Metadata component %s uploaded successfully.", uploadedMetadata.getComponent()));
                results.put(uploadedMetadata.getComponent(), uploadedMetadata);
            }, ex -> {
                logger.error(
                    () -> new ParameterizedMessage("Exception during transfer of Metadata Fragment to Remote {}", ex.getMessage()),
                    ex
                );
                exceptionList.add(ex);
            }),
            latch
        );

        if (uploadSettingsMetadata) {
            uploadTasks.add(SETTING_METADATA);
            remoteGlobalMetadataManager.writeAsync(
                SETTING_METADATA,
                new RemotePersistentSettingsMetadata(
                    clusterState.metadata().persistentSettings(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }
        if (uploadTransientSettingMetadata) {
            uploadTasks.add(TRANSIENT_SETTING_METADATA);
            remoteGlobalMetadataManager.writeAsync(
                TRANSIENT_SETTING_METADATA,
                new RemoteTransientSettingsMetadata(
                    clusterState.metadata().transientSettings(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.add(COORDINATION_METADATA);
            remoteGlobalMetadataManager.writeAsync(
                COORDINATION_METADATA,
                new RemoteCoordinationMetadata(
                    clusterState.metadata().coordinationMetadata(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.add(TEMPLATES_METADATA);
            remoteGlobalMetadataManager.writeAsync(
                TEMPLATES_METADATA,
                new RemoteTemplatesMetadata(
                    clusterState.metadata().templatesMetadata(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }
        if (uploadDiscoveryNodes) {
            uploadTasks.add(DISCOVERY_NODES);
            remoteClusterStateAttributesManager.writeAsync(
                RemoteDiscoveryNodes.DISCOVERY_NODES,
                new RemoteDiscoveryNodes(
                    clusterState.nodes(),
                    clusterState.version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }
        if (uploadClusterBlock) {
            uploadTasks.add(CLUSTER_BLOCKS);
            remoteClusterStateAttributesManager.writeAsync(
                RemoteClusterBlocks.CLUSTER_BLOCKS,
                new RemoteClusterBlocks(
                    clusterState.blocks(),
                    clusterState.version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }
        if (uploadHashesOfConsistentSettings) {
            uploadTasks.add(HASHES_OF_CONSISTENT_SETTINGS);
            remoteGlobalMetadataManager.writeAsync(
                HASHES_OF_CONSISTENT_SETTINGS,
                new RemoteHashesOfConsistentSettings(
                    (DiffableStringMap) clusterState.metadata().hashesOfConsistentSettings(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }
        customToUpload.forEach((key, value) -> {
            String customComponent = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, key);
            uploadTasks.add(customComponent);
            remoteGlobalMetadataManager.writeAsync(
                customComponent,
                new RemoteCustomMetadata(
                    value,
                    key,
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    namedWriteableRegistry
                ),
                listener
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.add(indexMetadata.getIndex().getName());
            remoteIndexMetadataManager.writeAsync(
                indexMetadata.getIndex().getName(),
                new RemoteIndexMetadata(
                    indexMetadata,
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry(),
                    remoteIndexMetadataManager.getPathTypeSetting(),
                    remoteIndexMetadataManager.getPathHashAlgoSetting(),
                    remotePathPrefix
                ),
                listener
            );
        });

        clusterStateCustomToUpload.forEach((key, value) -> {
            uploadTasks.add(key);
            remoteClusterStateAttributesManager.writeAsync(
                CLUSTER_STATE_CUSTOM,
                new RemoteClusterStateCustoms(
                    value,
                    key,
                    clusterState.version(),
                    clusterState.metadata().clusterUUID(),
                    blobStoreRepository.getCompressor(),
                    namedWriteableRegistry
                ),
                listener
            );
        });
        indicesRoutingToUpload.forEach(indexRoutingTable -> {
            uploadTasks.add(INDEX_ROUTING_METADATA_PREFIX + indexRoutingTable.getIndex().getName());
            remoteRoutingTableService.getAsyncIndexRoutingWriteAction(
                clusterState.metadata().clusterUUID(),
                clusterState.term(),
                clusterState.version(),
                indexRoutingTable,
                listener
            );
        });
        if (routingTableDiff != null
            && routingTableDiff.provideDiff() != null
            && (!routingTableDiff.provideDiff().getDiffs().isEmpty()
                || !routingTableDiff.provideDiff().getDeletes().isEmpty()
                || !routingTableDiff.provideDiff().getUpserts().isEmpty())) {
            uploadTasks.add(RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_FILE);
            remoteRoutingTableService.getAsyncIndexRoutingDiffWriteAction(
                clusterState.metadata().clusterUUID(),
                clusterState.term(),
                clusterState.version(),
                routingTableDiff,
                listener
            );
        }
        invokeIndexMetadataUploadListeners(indexToUpload, prevIndexMetadataByName, latch, exceptionList);

        try {
            if (latch.await(remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                // TODO: We should add metrics where transfer is timing out. [Issue: #10687]
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of following metadata to complete - %s",
                        String.join(", ", uploadTasks)
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of metadata to complete - %s", String.join(", ", uploadTasks)),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Exception during transfer of following metadata to Remote - %s", String.join(", ", uploadTasks))
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        if (results.size() != uploadTasks.size()) {
            throw new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Some metadata components were not uploaded successfully. Objects to be uploaded: %s, uploaded objects: %s",
                    String.join(", ", uploadTasks),
                    String.join(", ", results.keySet())
                )
            );
        }
        UploadedMetadataResults response = new UploadedMetadataResults();
        results.forEach((name, uploadedMetadata) -> {
            if (uploadedMetadata.getClass().equals(UploadedIndexMetadata.class)
                && uploadedMetadata.getComponent().contains(INDEX_ROUTING_METADATA_PREFIX)) {
                response.uploadedIndicesRoutingMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_FILE.equals(name)) {
                response.uploadedIndicesRoutingDiffMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (name.startsWith(CUSTOM_METADATA)) {
                // component name for custom metadata will look like custom--<metadata-attribute>
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (name.startsWith(CLUSTER_STATE_CUSTOM)) {
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedClusterStateCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (COORDINATION_METADATA.equals(name)) {
                response.uploadedCoordinationMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemotePersistentSettingsMetadata.SETTING_METADATA.equals(name)) {
                response.uploadedSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (TEMPLATES_METADATA.equals(name)) {
                response.uploadedTemplatesMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (name.contains(UploadedIndexMetadata.COMPONENT_PREFIX)) {
                response.uploadedIndexMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA.equals(name)) {
                response.uploadedTransientSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteDiscoveryNodes.DISCOVERY_NODES.equals(uploadedMetadata.getComponent())) {
                response.uploadedDiscoveryNodes = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteClusterBlocks.CLUSTER_BLOCKS.equals(uploadedMetadata.getComponent())) {
                response.uploadedClusterBlocks = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS.equals(uploadedMetadata.getComponent())) {
                response.uploadedHashesOfConsistentSettings = (UploadedMetadataAttribute) uploadedMetadata;
            } else {
                throw new IllegalStateException("Unknown metadata component name " + name);
            }
        });
        logger.trace("response {}", response.uploadedIndicesRoutingMetadata.toString());
        return response;
    }

    /**
     * Invokes the index metadata upload listener but does not wait for the execution to complete.
     */
    private void invokeIndexMetadataUploadListeners(
        List<IndexMetadata> updatedIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList
    ) {
        for (IndexMetadataUploadListener listener : indexMetadataUploadListeners) {
            String listenerName = listener.getClass().getSimpleName();
            listener.onUpload(
                updatedIndexMetadataList,
                prevIndexMetadataByName,
                getIndexMetadataUploadActionListener(updatedIndexMetadataList, prevIndexMetadataByName, latch, exceptionList, listenerName)
            );
        }

    }

    private ActionListener<Void> getIndexMetadataUploadActionListener(
        List<IndexMetadata> newIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList,
        String listenerName
    ) {
        long startTime = System.nanoTime();
        return new LatchedActionListener<>(
            ActionListener.wrap(
                ignored -> logger.trace(
                    new ParameterizedMessage(
                        "listener={} : Invoked successfully with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                        listenerName,
                        newIndexMetadataList,
                        prevIndexMetadataByName.values(),
                        (System.nanoTime() - startTime)
                    )
                ),
                ex -> {
                    logger.error(
                        new ParameterizedMessage(
                            "listener={} : Exception during invocation with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                            listenerName,
                            newIndexMetadataList,
                            prevIndexMetadataByName.values(),
                            (System.nanoTime() - startTime)
                        ),
                        ex
                    );
                    exceptionList.add(ex);
                }
            ),
            latch
        );
    }

    public RemoteManifestManager getRemoteManifestManager() {
        return remoteManifestManager;
    }

    public RemoteClusterStateCleanupManager getCleanupManager() {
        return remoteClusterStateCleanupManager;
    }

    @Nullable
    public RemoteClusterStateManifestInfo markLastStateAsCommitted(
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest,
        boolean commitVotingConfig
    ) throws IOException {
        assert clusterState != null : "Last accepted cluster state is not set";
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        UploadedMetadataAttribute uploadedCoordinationMetadata = previousManifest.getCoordinationMetadata();
        if (commitVotingConfig) {
            // update the coordination metadata if voting config is committed
            uploadedCoordinationMetadata = writeMetadataInParallel(
                clusterState,
                emptyList(),
                emptyMap(),
                emptyMap(),
                true,
                false,
                false,
                false,
                false,
                false,
                emptyMap(),
                false,
                emptyList(),
                null
            ).uploadedCoordinationMetadata;
        }
        UploadedMetadataResults uploadedMetadataResults = new UploadedMetadataResults(
            previousManifest.getIndices(),
            previousManifest.getCustomMetadataMap(),
            uploadedCoordinationMetadata,
            previousManifest.getSettingsMetadata(),
            previousManifest.getTemplatesMetadata(),
            previousManifest.getTransientSettingsMetadata(),
            previousManifest.getDiscoveryNodesMetadata(),
            previousManifest.getClusterBlocksMetadata(),
            previousManifest.getIndicesRouting(),
            previousManifest.getHashesOfConsistentSettings(),
            previousManifest.getClusterStateCustomMap()
        );

        RemoteClusterStateManifestInfo committedManifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousManifest.getPreviousClusterUUID(),
            previousManifest.getDiffManifest(),
            !remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.NONE)
                ? new ClusterStateChecksum(clusterState, threadpool)
                : null,
            true,
            previousManifest.getCodecVersion()
        );
        if (!previousManifest.isClusterUUIDCommitted() && committedManifestDetails.getClusterMetadataManifest().isClusterUUIDCommitted()) {
            remoteClusterStateCleanupManager.deleteStaleClusterUUIDs(clusterState, committedManifestDetails.getClusterMetadataManifest());
        }

        return committedManifestDetails;
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        return remoteManifestManager.getLatestClusterMetadataManifest(clusterName, clusterUUID);
    }

    public ClusterMetadataManifest getClusterMetadataManifestByFileName(String clusterUUID, String fileName) {
        return remoteManifestManager.getRemoteClusterMetadataManifestByFileName(clusterUUID, fileName);
    }

    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(
        String clusterName,
        String clusterUUID,
        long term,
        long version
    ) {
        return remoteManifestManager.getClusterMetadataManifestByTermVersion(clusterName, clusterUUID, term, version);
    }

    @Override
    public void close() throws IOException {
        remoteClusterStateCleanupManager.close();
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
        this.remoteRoutingTableService.close();
    }

    public void start() {
        assert isRemoteClusterStateConfigured(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        String clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        blobStoreTransferService = new BlobStoreTransferService(getBlobStore(), threadpool);

        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            namedWriteableRegistry,
            threadpool
        );
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(
            clusterSettings,
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            threadpool
        );
        remoteManifestManager = new RemoteManifestManager(
            clusterSettings,
            clusterName,
            nodeId,
            blobStoreRepository,
            blobStoreTransferService,
            threadpool
        );
        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            namedWriteableRegistry,
            threadpool
        );

        remoteRoutingTableService.start();
        remoteClusterStateCleanupManager.start();
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    private void setChecksumValidationMode(RemoteClusterStateValidationMode remoteClusterStateValidationMode) {
        this.remoteClusterStateValidationMode = remoteClusterStateValidationMode;
    }

    private void setRemotePublicationSetting(boolean remotePublicationSetting) {
        if (remotePublicationSetting == false) {
            this.isPublicationEnabled.set(false);
        } else {
            this.isPublicationEnabled.set(isRemoteStoreClusterStateEnabled(settings) && isRemoteRoutingTableConfigured(settings));
        }
    }

    // Package private for unit test
    RemoteRoutingTableService getRemoteRoutingTableService() {
        return this.remoteRoutingTableService;
    }

    ThreadPool getThreadpool() {
        return threadpool;
    }

    BlobStoreRepository getBlobStoreRepository() {
        return blobStoreRepository;
    }

    BlobStore getBlobStore() {
        return blobStoreRepository.blobStore();
    }

    /**
     * Fetch latest ClusterState from remote, including global metadata, index metadata and cluster state version
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@link IndexMetadata}
     */
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID, boolean includeEphemeral) throws IOException {
        Optional<ClusterMetadataManifest> clusterMetadataManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (clusterMetadataManifest.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Latest cluster metadata manifest is not present for the provided clusterUUID: %s", clusterUUID)
            );
        }

        return getClusterStateForManifest(clusterName, clusterMetadataManifest.get(), nodeId, includeEphemeral);
    }

    // package private for testing
    ClusterState readClusterStateInParallel(
        ClusterState previousState,
        ClusterMetadataManifest manifest,
        String clusterUUID,
        String localNodeId,
        List<UploadedIndexMetadata> indicesToRead,
        Map<String, UploadedMetadataAttribute> customToRead,
        boolean readCoordinationMetadata,
        boolean readSettingsMetadata,
        boolean readTransientSettingsMetadata,
        boolean readTemplatesMetadata,
        boolean readDiscoveryNodes,
        boolean readClusterBlocks,
        List<UploadedIndexMetadata> indicesRoutingToRead,
        boolean readHashesOfConsistentSettings,
        Map<String, UploadedMetadataAttribute> clusterStateCustomToRead,
        boolean readIndexRoutingTableDiff,
        boolean includeEphemeral
    ) {
        int totalReadTasks = indicesToRead.size() + customToRead.size() + (readCoordinationMetadata ? 1 : 0) + (readSettingsMetadata
            ? 1
            : 0) + (readTemplatesMetadata ? 1 : 0) + (readDiscoveryNodes ? 1 : 0) + (readClusterBlocks ? 1 : 0)
            + (readTransientSettingsMetadata ? 1 : 0) + (readHashesOfConsistentSettings ? 1 : 0) + clusterStateCustomToRead.size()
            + indicesRoutingToRead.size() + (readIndexRoutingTableDiff ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalReadTasks);
        List<RemoteReadResult> readResults = Collections.synchronizedList(new ArrayList<>());
        List<IndexRoutingTable> readIndexRoutingTableResults = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Diff<RoutingTable>> readIndexRoutingTableDiffResults = new AtomicReference<>();
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalReadTasks));

        LatchedActionListener<RemoteReadResult> listener = new LatchedActionListener<>(ActionListener.wrap(response -> {
            logger.debug("Successfully read cluster state component from remote");
            readResults.add(response);
        }, ex -> {
            logger.error("Failed to read cluster state from remote", ex);
            exceptionList.add(ex);
        }), latch);

        for (UploadedIndexMetadata indexMetadata : indicesToRead) {
            remoteIndexMetadataManager.readAsync(
                indexMetadata.getIndexName(),
                new RemoteIndexMetadata(
                    RemoteClusterStateUtils.getFormattedIndexFileName(indexMetadata.getUploadedFilename()),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }

        LatchedActionListener<IndexRoutingTable> routingTableLatchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> {
                logger.debug(() -> new ParameterizedMessage("Successfully read index-routing for index {}", response.getIndex().getName()));
                readIndexRoutingTableResults.add(response);
            }, ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to read index-routing from remote"), ex);
                exceptionList.add(ex);
            }),
            latch
        );

        for (UploadedIndexMetadata indexRouting : indicesRoutingToRead) {
            remoteRoutingTableService.getAsyncIndexRoutingReadAction(
                clusterUUID,
                indexRouting.getUploadedFilename(),
                routingTableLatchedActionListener
            );
        }

        LatchedActionListener<Diff<RoutingTable>> routingTableDiffLatchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> {
                logger.debug("Successfully read routing table diff component from remote");
                readIndexRoutingTableDiffResults.set(response);
            }, ex -> {
                logger.error("Failed to read routing table diff from remote", ex);
                exceptionList.add(ex);
            }),
            latch
        );

        if (readIndexRoutingTableDiff) {
            remoteRoutingTableService.getAsyncIndexRoutingTableDiffReadAction(
                clusterUUID,
                manifest.getDiffManifest().getIndicesRoutingDiffPath(),
                routingTableDiffLatchedActionListener
            );
        }

        for (Map.Entry<String, UploadedMetadataAttribute> entry : customToRead.entrySet()) {
            remoteGlobalMetadataManager.readAsync(
                entry.getValue().getAttributeName(),
                new RemoteCustomMetadata(
                    entry.getValue().getUploadedFilename(),
                    entry.getKey(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    namedWriteableRegistry
                ),
                listener
            );
        }

        if (readCoordinationMetadata) {
            remoteGlobalMetadataManager.readAsync(
                COORDINATION_METADATA,
                new RemoteCoordinationMetadata(
                    manifest.getCoordinationMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }

        if (readSettingsMetadata) {
            remoteGlobalMetadataManager.readAsync(
                SETTING_METADATA,
                new RemotePersistentSettingsMetadata(
                    manifest.getSettingsMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }

        if (readTransientSettingsMetadata) {
            remoteGlobalMetadataManager.readAsync(
                TRANSIENT_SETTING_METADATA,
                new RemoteTransientSettingsMetadata(
                    manifest.getTransientSettingsMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }

        if (readTemplatesMetadata) {
            remoteGlobalMetadataManager.readAsync(
                TEMPLATES_METADATA,
                new RemoteTemplatesMetadata(
                    manifest.getTemplatesMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    blobStoreRepository.getNamedXContentRegistry()
                ),
                listener
            );
        }

        if (readDiscoveryNodes) {
            remoteClusterStateAttributesManager.readAsync(
                DISCOVERY_NODES,
                new RemoteDiscoveryNodes(
                    manifest.getDiscoveryNodesMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }

        if (readClusterBlocks) {
            remoteClusterStateAttributesManager.readAsync(
                CLUSTER_BLOCKS,
                new RemoteClusterBlocks(
                    manifest.getClusterBlocksMetadata().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }

        if (readHashesOfConsistentSettings) {
            remoteGlobalMetadataManager.readAsync(
                HASHES_OF_CONSISTENT_SETTINGS,
                new RemoteHashesOfConsistentSettings(
                    manifest.getHashesOfConsistentSettings().getUploadedFilename(),
                    clusterUUID,
                    blobStoreRepository.getCompressor()
                ),
                listener
            );
        }

        for (Map.Entry<String, UploadedMetadataAttribute> entry : clusterStateCustomToRead.entrySet()) {
            remoteClusterStateAttributesManager.readAsync(
                // pass component name as cluster-state-custom--<custom_name>, so that we can interpret it later
                String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, entry.getKey()),
                new RemoteClusterStateCustoms(
                    entry.getValue().getUploadedFilename(),
                    entry.getValue().getAttributeName(),
                    clusterUUID,
                    blobStoreRepository.getCompressor(),
                    namedWriteableRegistry
                ),
                listener
            );
        }

        try {
            if (latch.await(this.remoteStateReadTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException exception = new RemoteStateTransferException(
                    "Timed out waiting to read cluster state from remote within timeout " + this.remoteStateReadTimeout
                );
                exceptionList.forEach(exception::addSuppressed);
                throw exception;
            }
        } catch (InterruptedException e) {
            exceptionList.forEach(e::addSuppressed);
            RemoteStateTransferException ex = new RemoteStateTransferException(
                "Interrupted while waiting to read cluster state from metadata"
            );
            Thread.currentThread().interrupt();
            throw ex;
        }

        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException("Exception during reading cluster state from remote");
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(previousState);
        AtomicReference<Builder> discoveryNodesBuilder = new AtomicReference<>(DiscoveryNodes.builder());
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());
        metadataBuilder.version(manifest.getMetadataVersion());
        metadataBuilder.clusterUUID(manifest.getClusterUUID());
        metadataBuilder.clusterUUIDCommitted(manifest.isClusterUUIDCommitted());
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        Map<String, IndexRoutingTable> indicesRouting = new HashMap<>(previousState.routingTable().getIndicesRouting());

        readResults.forEach(remoteReadResult -> {
            switch (remoteReadResult.getComponent()) {
                case RemoteIndexMetadata.INDEX:
                    IndexMetadata indexMetadata = (IndexMetadata) remoteReadResult.getObj();
                    indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
                    break;
                case CUSTOM_METADATA:
                    Metadata.Custom metadataCustom = (Metadata.Custom) remoteReadResult.getObj();
                    if (includeEphemeral || (!includeEphemeral && metadataCustom.context().contains(XContentContext.GATEWAY))) {
                        metadataBuilder.putCustom(remoteReadResult.getComponentName(), (Metadata.Custom) remoteReadResult.getObj());
                    }
                    break;
                case COORDINATION_METADATA:
                    metadataBuilder.coordinationMetadata((CoordinationMetadata) remoteReadResult.getObj());
                    break;
                case SETTING_METADATA:
                    metadataBuilder.persistentSettings((Settings) remoteReadResult.getObj());
                    break;
                case TRANSIENT_SETTING_METADATA:
                    metadataBuilder.transientSettings((Settings) remoteReadResult.getObj());
                    break;
                case TEMPLATES_METADATA:
                    metadataBuilder.templates((TemplatesMetadata) remoteReadResult.getObj());
                    break;
                case HASHES_OF_CONSISTENT_SETTINGS:
                    metadataBuilder.hashesOfConsistentSettings((DiffableStringMap) remoteReadResult.getObj());
                    break;
                case CLUSTER_STATE_ATTRIBUTE:
                    if (remoteReadResult.getComponentName().equals(DISCOVERY_NODES)) {
                        discoveryNodesBuilder.set(DiscoveryNodes.builder((DiscoveryNodes) remoteReadResult.getObj()));
                    } else if (remoteReadResult.getComponentName().equals(CLUSTER_BLOCKS)) {
                        clusterStateBuilder.blocks((ClusterBlocks) remoteReadResult.getObj());
                    } else if (remoteReadResult.getComponentName().startsWith(CLUSTER_STATE_CUSTOM)) {
                        // component name for mat is "cluster-state-custom--custom_name"
                        String custom = remoteReadResult.getComponentName().split(CUSTOM_DELIMITER)[1];
                        clusterStateBuilder.putCustom(custom, (ClusterState.Custom) remoteReadResult.getObj());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown component: " + remoteReadResult.getComponent());
            }
        });

        metadataBuilder.indices(indexMetadataMap);
        if (readDiscoveryNodes) {
            clusterStateBuilder.nodes(discoveryNodesBuilder.get().localNodeId(localNodeId));
        }

        clusterStateBuilder.metadata(metadataBuilder).version(manifest.getStateVersion()).stateUUID(manifest.getStateUUID());

        readIndexRoutingTableResults.forEach(
            indexRoutingTable -> indicesRouting.put(indexRoutingTable.getIndex().getName(), indexRoutingTable)
        );
        Diff<RoutingTable> routingTableDiff = readIndexRoutingTableDiffResults.get();
        RoutingTable newRoutingTable = new RoutingTable(manifest.getRoutingTableVersion(), indicesRouting);
        if (routingTableDiff != null) {
            newRoutingTable = routingTableDiff.apply(previousState.getRoutingTable());
        }
        clusterStateBuilder.routingTable(newRoutingTable);

        return clusterStateBuilder.build();
    }

    public ClusterState getClusterStateForManifest(
        String clusterName,
        ClusterMetadataManifest manifest,
        String localNodeId,
        boolean includeEphemeral
    ) throws IOException {
        ClusterState stateFromCache = remoteClusterStateCache.getState(clusterName, manifest);
        if (stateFromCache != null) {
            return stateFromCache;
        }

        final ClusterState clusterState;
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (manifest.onOrAfterCodecVersion(CODEC_V2)) {
            clusterState = readClusterStateInParallel(
                ClusterState.builder(new ClusterName(clusterName)).build(),
                manifest,
                manifest.getClusterUUID(),
                localNodeId,
                manifest.getIndices(),
                manifest.getCustomMetadataMap(),
                manifest.getCoordinationMetadata() != null,
                manifest.getSettingsMetadata() != null,
                includeEphemeral && manifest.getTransientSettingsMetadata() != null,
                manifest.getTemplatesMetadata() != null,
                includeEphemeral && manifest.getDiscoveryNodesMetadata() != null,
                includeEphemeral && manifest.getClusterBlocksMetadata() != null,
                includeEphemeral ? manifest.getIndicesRouting() : emptyList(),
                includeEphemeral && manifest.getHashesOfConsistentSettings() != null,
                includeEphemeral ? manifest.getClusterStateCustomMap() : emptyMap(),
                false,
                includeEphemeral
            );

            if (includeEphemeral
                && !remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.NONE)
                && manifest.getClusterStateChecksum() != null) {
                validateClusterStateFromChecksum(manifest, clusterState, clusterName, localNodeId, true);
            }
        } else {
            ClusterState state = readClusterStateInParallel(
                ClusterState.builder(new ClusterName(clusterName)).build(),
                manifest,
                manifest.getClusterUUID(),
                localNodeId,
                manifest.getIndices(),
                // for manifest codec V1, we don't have the following objects to read, so not passing anything
                emptyMap(),
                false,
                false,
                false,
                false,
                false,
                false,
                emptyList(),
                false,
                emptyMap(),
                false,
                false
            );
            Metadata.Builder mb = Metadata.builder(remoteGlobalMetadataManager.getGlobalMetadata(manifest.getClusterUUID(), manifest));
            mb.indices(state.metadata().indices());
            clusterState = ClusterState.builder(state).metadata(mb).build();
        }
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateFullDownloadSucceeded();
        remoteStateStats.stateFullDownloadTook(durationMillis);
        if (includeEphemeral) {
            // cache only if the entire cluster-state is present
            remoteClusterStateCache.putState(clusterState);
        }
        return clusterState;
    }

    public ClusterState getClusterStateUsingDiff(ClusterMetadataManifest manifest, ClusterState previousState, String localNodeId) {
        assert manifest.getDiffManifest() != null : "Diff manifest null which is required for downloading cluster state";
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        ClusterStateDiffManifest diff = manifest.getDiffManifest();
        boolean includeEphemeral = true;

        List<UploadedIndexMetadata> updatedIndices = diff.getIndicesUpdated().stream().map(idx -> {
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndices()
                .stream()
                .filter(idx2 -> idx2.getIndexName().equals(idx))
                .findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());

        Map<String, UploadedMetadataAttribute> updatedCustomMetadata = new HashMap<>();
        if (diff.getCustomMetadataUpdated() != null) {
            for (String customType : diff.getCustomMetadataUpdated()) {
                updatedCustomMetadata.put(customType, manifest.getCustomMetadataMap().get(customType));
            }
        }
        Map<String, UploadedMetadataAttribute> updatedClusterStateCustom = new HashMap<>();
        if (diff.getClusterStateCustomUpdated() != null) {
            for (String customType : diff.getClusterStateCustomUpdated()) {
                updatedClusterStateCustom.put(customType, manifest.getClusterStateCustomMap().get(customType));
            }
        }

        List<UploadedIndexMetadata> updatedIndexRouting = new ArrayList<>();
        if (manifest.getCodecVersion() == CODEC_V2 || manifest.getCodecVersion() == CODEC_V3) {
            updatedIndexRouting.addAll(
                remoteRoutingTableService.getUpdatedIndexRoutingTableMetadata(diff.getIndicesRoutingUpdated(), manifest.getIndicesRouting())
            );
        }

        ClusterState updatedClusterState = readClusterStateInParallel(
            previousState,
            manifest,
            manifest.getClusterUUID(),
            localNodeId,
            updatedIndices,
            updatedCustomMetadata,
            diff.isCoordinationMetadataUpdated(),
            diff.isSettingsMetadataUpdated(),
            diff.isTransientSettingsMetadataUpdated(),
            diff.isTemplatesMetadataUpdated(),
            diff.isDiscoveryNodesUpdated(),
            diff.isClusterBlocksUpdated(),
            updatedIndexRouting,
            diff.isHashesOfConsistentSettingsUpdated(),
            updatedClusterStateCustom,
            manifest.getDiffManifest() != null
                && manifest.getDiffManifest().getIndicesRoutingDiffPath() != null
                && !manifest.getDiffManifest().getIndicesRoutingDiffPath().isEmpty(),
            includeEphemeral
        );
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(updatedClusterState);
        Metadata.Builder metadataBuilder = Metadata.builder(updatedClusterState.metadata());
        // remove the deleted indices from the metadata
        for (String index : diff.getIndicesDeleted()) {
            metadataBuilder.remove(index);
        }
        // remove the deleted metadata customs from the metadata
        if (diff.getCustomMetadataDeleted() != null) {
            for (String customType : diff.getCustomMetadataDeleted()) {
                metadataBuilder.removeCustom(customType);
            }
        }

        // remove the deleted cluster state customs from the metadata
        if (diff.getClusterStateCustomDeleted() != null) {
            for (String customType : diff.getClusterStateCustomDeleted()) {
                clusterStateBuilder.removeCustom(customType);
            }
        }

        HashMap<String, IndexRoutingTable> indexRoutingTables = new HashMap<>(updatedClusterState.getRoutingTable().getIndicesRouting());
        if (manifest.getCodecVersion() == CODEC_V2 || manifest.getCodecVersion() == CODEC_V3) {
            for (String indexName : diff.getIndicesRoutingDeleted()) {
                indexRoutingTables.remove(indexName);
            }
        }

        ClusterState clusterState = clusterStateBuilder.stateUUID(manifest.getStateUUID())
            .version(manifest.getStateVersion())
            .metadata(metadataBuilder)
            .routingTable(new RoutingTable(manifest.getRoutingTableVersion(), indexRoutingTables))
            .build();
        if (!remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.NONE) && manifest.getClusterStateChecksum() != null) {
            validateClusterStateFromChecksum(manifest, clusterState, previousState.getClusterName().value(), localNodeId, false);
        }
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateDiffDownloadSucceeded();
        remoteStateStats.stateDiffDownloadTook(durationMillis);

        assert includeEphemeral == true;
        // newState includes all the fields of cluster-state (includeEphemeral=true always)
        remoteClusterStateCache.putState(clusterState);
        return clusterState;
    }

    void validateClusterStateFromChecksum(
        ClusterMetadataManifest manifest,
        ClusterState clusterState,
        String clusterName,
        String localNodeId,
        boolean isFullStateDownload
    ) {
        ClusterStateChecksum newClusterStateChecksum = new ClusterStateChecksum(clusterState, threadpool);
        List<String> failedValidation = newClusterStateChecksum.getMismatchEntities(manifest.getClusterStateChecksum());
        if (failedValidation.isEmpty()) {
            return;
        }
        logger.error(
            () -> new ParameterizedMessage(
                "Cluster state checksums do not match. Checksum from manifest {}, checksum from created cluster state {}. Entities failing validation {}",
                manifest.getClusterStateChecksum(),
                newClusterStateChecksum,
                failedValidation
            )
        );
        if (isFullStateDownload) {
            remoteStateStats.stateFullDownloadValidationFailed();
        } else {
            remoteStateStats.stateDiffDownloadValidationFailed();
        }

        if (isFullStateDownload && remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.FAILURE)) {
            throw new IllegalStateException(
                "Cluster state checksums do not match during full state read. Validation failed for " + failedValidation
            );
        }
        if (remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.FAILURE)
            || remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.TRACE)) {
            // download full cluster state and match against state created for the failing entities
            ClusterState fullClusterState = readClusterStateInParallel(
                ClusterState.builder(new ClusterName(clusterName)).build(),
                manifest,
                manifest.getClusterUUID(),
                localNodeId,
                manifest.getIndices(),
                manifest.getCustomMetadataMap(),
                manifest.getCoordinationMetadata() != null,
                manifest.getSettingsMetadata() != null,
                manifest.getTransientSettingsMetadata() != null,
                manifest.getTemplatesMetadata() != null,
                manifest.getDiscoveryNodesMetadata() != null,
                manifest.getClusterBlocksMetadata() != null,
                manifest.getIndicesRouting(),
                manifest.getHashesOfConsistentSettings() != null,
                manifest.getClusterStateCustomMap(),
                false,
                true
            );
            for (String failedEntity : failedValidation) {
                switch (failedEntity) {
                    case ClusterStateChecksum.ROUTING_TABLE_CS:
                        Diff<RoutingTable> routingTableDiff = fullClusterState.routingTable().diff(clusterState.routingTable());
                        logger.error(() -> new ParameterizedMessage("Failing Diff in routing table {}", routingTableDiff));
                        break;
                    case ClusterStateChecksum.NODES_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in discovery nodes {}",
                                fullClusterState.nodes().diff(clusterState.nodes())
                            )
                        );
                        break;
                    case ClusterStateChecksum.BLOCKS_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in cluster blocks {}",
                                fullClusterState.blocks().diff(clusterState.blocks())
                            )
                        );
                        break;
                    case ClusterStateChecksum.CUSTOMS_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in cluster state customs {}",
                                DiffableUtils.diff(
                                    clusterState.customs(),
                                    fullClusterState.customs(),
                                    DiffableUtils.getStringKeySerializer(),
                                    CUSTOM_VALUE_SERIALIZER
                                )
                            )
                        );
                        break;
                    case ClusterStateChecksum.COORDINATION_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in coordination md. current md {}, full state md {}",
                                clusterState.metadata().coordinationMetadata(),
                                fullClusterState.metadata().coordinationMetadata()
                            )
                        );
                        break;
                    case ClusterStateChecksum.TRANSIENT_SETTINGS_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in transient settings md. current md {}, full state md {}",
                                clusterState.metadata().transientSettings(),
                                fullClusterState.metadata().transientSettings()
                            )
                        );

                        break;
                    case ClusterStateChecksum.SETTINGS_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in settings md. current md {}, full state md {}",
                                clusterState.metadata().settings(),
                                fullClusterState.metadata().settings()
                            )
                        );

                        break;
                    case ClusterStateChecksum.HASHES_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in hashes md {}",
                                ((DiffableStringMap) fullClusterState.metadata().hashesOfConsistentSettings()).diff(
                                    (DiffableStringMap) clusterState.metadata().hashesOfConsistentSettings()
                                )
                            )
                        );
                        break;
                    case ClusterStateChecksum.TEMPLATES_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in templates md{}",
                                fullClusterState.metadata().templatesMetadata().diff(clusterState.metadata().templatesMetadata())
                            )
                        );
                        break;
                    case ClusterStateChecksum.CUSTOM_MD_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in customs md {}",
                                DiffableUtils.diff(
                                    clusterState.metadata().customs(),
                                    fullClusterState.metadata().customs(),
                                    DiffableUtils.getStringKeySerializer(),
                                    Metadata.CUSTOM_VALUE_SERIALIZER
                                )
                            )
                        );
                        break;
                    case ClusterStateChecksum.INDICES_CS:
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Failing Diff in index md {}",
                                DiffableUtils.diff(
                                    clusterState.metadata().indices(),
                                    fullClusterState.metadata().indices(),
                                    DiffableUtils.getStringKeySerializer()
                                )
                            )
                        );
                        break;
                    default:
                        logger.error(() -> new ParameterizedMessage("Unknown failed entity {}", failedEntity));
                        break;
                }
            }
        }
        if (remoteClusterStateValidationMode.equals(RemoteClusterStateValidationMode.FAILURE)) {
            throw new IllegalStateException(
                "Cluster state checksums do not match during diff read. Validation failed for " + failedValidation
            );
        }
    }

    /**
     * Fetch the previous cluster UUIDs from remote state store and return the most recent valid cluster UUID
     *
     * @param clusterName The cluster name for which previous cluster UUID is to be fetched
     * @return Last valid cluster UUID
     */
    public String getLastKnownUUIDFromRemote(String clusterName) {
        try {
            Set<String> clusterUUIDs = getAllClusterUUIDs(clusterName);
            Map<String, ClusterMetadataManifest> latestManifests = remoteManifestManager.getLatestManifestForAllClusterUUIDs(
                clusterName,
                clusterUUIDs
            );
            List<String> validChain = createClusterChain(latestManifests, clusterName);
            if (validChain.isEmpty()) {
                return ClusterState.UNKNOWN_UUID;
            }
            return validChain.get(0);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while fetching previous UUIDs from remote store for cluster name: %s", clusterName),
                e
            );
        }
    }

    public boolean isRemotePublicationEnabled() {
        return this.isPublicationEnabled.get();
    }

    public void setRemoteStateReadTimeout(TimeValue remoteStateReadTimeout) {
        this.remoteStateReadTimeout = remoteStateReadTimeout;
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(getBlobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(blobStoreRepository, clusterName).children();
        if (clusterUUIDMetadata == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(clusterUUIDMetadata.keySet());
    }

    private Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    /**
     * This method creates a valid cluster UUID chain.
     *
     * @param manifestsByClusterUUID Map of latest ClusterMetadataManifest for every cluster UUID
     * @return List of cluster UUIDs. The first element is the most recent cluster UUID in the chain
     */
    private List<String> createClusterChain(final Map<String, ClusterMetadataManifest> manifestsByClusterUUID, final String clusterName) {
        final List<ClusterMetadataManifest> validClusterManifests = manifestsByClusterUUID.values()
            .stream()
            .filter(this::isValidClusterUUID)
            .collect(Collectors.toList());
        final Map<String, String> clusterUUIDGraph = validClusterManifests.stream()
            .collect(Collectors.toMap(ClusterMetadataManifest::getClusterUUID, ClusterMetadataManifest::getPreviousClusterUUID));
        final List<String> topLevelClusterUUIDs = validClusterManifests.stream()
            .map(ClusterMetadataManifest::getClusterUUID)
            .filter(clusterUUID -> !clusterUUIDGraph.containsValue(clusterUUID))
            .collect(Collectors.toList());

        if (topLevelClusterUUIDs.isEmpty()) {
            // This can occur only when there are no valid cluster UUIDs
            assert validClusterManifests.isEmpty() : "There are no top level cluster UUIDs even when there are valid cluster UUIDs";
            logger.info("There is no valid previous cluster UUID. All cluster UUIDs evaluated are: {}", manifestsByClusterUUID.keySet());
            return emptyList();
        }
        if (topLevelClusterUUIDs.size() > 1) {
            logger.info("Top level cluster UUIDs: {}", topLevelClusterUUIDs);
            // If the valid cluster UUIDs are more that 1, it means there was some race condition where
            // more then 2 cluster manager nodes tried to become active cluster manager and published
            // 2 cluster UUIDs which followed the same previous UUID.
            final Map<String, ClusterMetadataManifest> manifestsByClusterUUIDTrimmed = trimClusterUUIDs(
                manifestsByClusterUUID,
                topLevelClusterUUIDs,
                clusterName
            );
            if (manifestsByClusterUUID.size() == manifestsByClusterUUIDTrimmed.size()) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "The system has ended into multiple valid cluster states in the remote store. "
                            + "Please check their latest manifest to decide which one you want to keep. Valid Cluster UUIDs: - %s",
                        topLevelClusterUUIDs
                    )
                );
            }
            return createClusterChain(manifestsByClusterUUIDTrimmed, clusterName);
        }
        final List<String> validChain = new ArrayList<>();
        String currentUUID = topLevelClusterUUIDs.get(0);
        while (currentUUID != null && !ClusterState.UNKNOWN_UUID.equals(currentUUID)) {
            validChain.add(currentUUID);
            // Getting the previous cluster UUID of a cluster UUID from the clusterUUID Graph
            currentUUID = clusterUUIDGraph.get(currentUUID);
        }
        logger.info("Known UUIDs found in remote store : [{}]", validChain);
        return validChain;
    }

    /**
     * This method take a map of manifests for different cluster UUIDs and removes the
     * manifest of a cluster UUID if the latest metadata for that cluster UUID is equivalent
     * to the latest metadata of its previous UUID.
     *
     * @return Trimmed map of manifests
     */
    private Map<String, ClusterMetadataManifest> trimClusterUUIDs(
        final Map<String, ClusterMetadataManifest> latestManifestsByClusterUUID,
        final List<String> validClusterUUIDs,
        final String clusterName
    ) {
        final Map<String, ClusterMetadataManifest> trimmedUUIDs = new HashMap<>(latestManifestsByClusterUUID);
        for (String clusterUUID : validClusterUUIDs) {
            ClusterMetadataManifest currentManifest = trimmedUUIDs.get(clusterUUID);
            // Here we compare the manifest of current UUID to that of previous UUID
            // In case currentUUID's latest manifest is same as previous UUIDs latest manifest,
            // that means it was restored from previousUUID and no IndexMetadata update was performed on it.
            if (!ClusterState.UNKNOWN_UUID.equals(currentManifest.getPreviousClusterUUID())) {
                ClusterMetadataManifest previousManifest = trimmedUUIDs.get(currentManifest.getPreviousClusterUUID());
                if (isMetadataEqual(currentManifest, previousManifest, clusterName)
                    && remoteGlobalMetadataManager.isGlobalMetadataEqual(currentManifest, previousManifest, clusterName)) {
                    trimmedUUIDs.remove(clusterUUID);
                }
            }
        }
        return trimmedUUIDs;
    }

    private boolean isMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        // todo clusterName can be set as final in the constructor
        if (first.getIndices().size() != second.getIndices().size()) {
            return false;
        }
        final Map<String, UploadedIndexMetadata> secondIndices = second.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                uploadedIndexMetadata,
                first.getClusterUUID()
            );
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                secondUploadedIndexMetadata,
                second.getClusterUUID()
            );
            if (firstIndexMetadata.equals(secondIndexMetadata) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidClusterUUID(ClusterMetadataManifest manifest) {
        return manifest.isClusterUUIDCommitted();
    }

    // package private setter which are required for injecting mock managers, these setters are not supposed to be used elsewhere
    void setRemoteIndexMetadataManager(RemoteIndexMetadataManager remoteIndexMetadataManager) {
        this.remoteIndexMetadataManager = remoteIndexMetadataManager;
    }

    void setRemoteGlobalMetadataManager(RemoteGlobalMetadataManager remoteGlobalMetadataManager) {
        this.remoteGlobalMetadataManager = remoteGlobalMetadataManager;
    }

    void setRemoteClusterStateAttributesManager(RemoteClusterStateAttributesManager remoteClusterStateAttributeManager) {
        this.remoteClusterStateAttributesManager = remoteClusterStateAttributeManager;
    }

    public void writeMetadataFailed() {
        remoteStateStats.stateUploadFailed();
    }

    public RemotePersistenceStats getRemoteStateStats() {
        return remoteStateStats;
    }

    public PersistedStateStats getUploadStats() {
        return remoteStateStats.getUploadStats();
    }

    public PersistedStateStats getFullDownloadStats() {
        return remoteStateStats.getRemoteFullDownloadStats();
    }

    public PersistedStateStats getDiffDownloadStats() {
        return remoteStateStats.getRemoteDiffDownloadStats();
    }

    public void fullDownloadFailed() {
        remoteStateStats.stateFullDownloadFailed();
    }

    public void diffDownloadFailed() {
        remoteStateStats.stateDiffDownloadFailed();
    }

    RemoteClusterStateCache getRemoteClusterStateCache() {
        return remoteClusterStateCache;
    }

}
