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
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {

    public static final String METADATA_NAME_FORMAT = "%s.dat";

    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";

    public static final int RETAINED_MANIFESTS = 10;

    public static final String DELIMITER = "__";
    public static final String CUSTOM_DELIMITER = "--";

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "metadata",
        METADATA_NAME_FORMAT,
        Metadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<CoordinationMetadata> COORDINATION_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "coordination",
        METADATA_NAME_FORMAT,
        CoordinationMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "settings",
        METADATA_NAME_FORMAT,
        Settings::fromXContent
    );

    public static final ChecksumBlobStoreFormat<TemplatesMetadata> TEMPLATES_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "templates",
        METADATA_NAME_FORMAT,
        TemplatesMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<Metadata.Custom> CUSTOM_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "custom",
        METADATA_NAME_FORMAT,
        null // no need to reader here, as this object is only used to write/serialize the object
    );

    /**
     * Manifest format compatible with older codec v0, where codec version was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V0 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV0);

    /**
     * Manifest format compatible with older codec v1, where codec versions/global metadata was introduced.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V1 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV1);

    /**
     * Manifest format compatible with codec v2, where global metadata file is replaced with multiple metadata attribute files
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
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

    public static final String CLUSTER_STATE_PATH_TOKEN = "cluster-state";
    public static final String INDEX_PATH_TOKEN = "index";
    public static final String GLOBAL_METADATA_PATH_TOKEN = "global-metadata";
    public static final String MANIFEST_PATH_TOKEN = "manifest";
    public static final String MANIFEST_FILE_PREFIX = "manifest";
    public static final String METADATA_FILE_PREFIX = "metadata";
    public static final String COORDINATION_METADATA = "coordination";
    public static final String SETTING_METADATA = "settings";
    public static final String TEMPLATES_METADATA = "templates";
    public static final String CUSTOM_METADATA = "custom";
    public static final int SPLITED_MANIFEST_FILE_LENGTH = 6; // file name manifest__term__version__C/P__timestamp__codecversion

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private final List<IndexMetadataUploadListener> indexMetadataUploadListeners;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private volatile TimeValue indexMetadataUploadTimeout;
    private volatile TimeValue globalMetadataUploadTimeout;
    private volatile TimeValue metadataManifestUploadTimeout;

    private final AtomicBoolean deleteStaleMetadataRunning = new AtomicBoolean(false);
    private final RemotePersistenceStats remoteStateStats;
    private final String CLUSTER_STATE_UPLOAD_TIME_LOG_STRING = "writing cluster state for version [{}] took [{}ms]";
    private final String METADATA_UPDATE_LOG_STRING = "wrote metadata for [{}] indices and skipped [{}] unchanged "
        + "indices, coordination metadata updated : [{}], settings metadata updated : [{}], templates metadata "
        + "updated : [{}], custom metadata updated : [{}]";
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;
    public static final int MANIFEST_CURRENT_CODEC_VERSION = ClusterMetadataManifest.CODEC_V3;
    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 2;

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
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool,
        List<IndexMetadataUploadListener> indexMetadataUploadListeners
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
        this.remoteStateStats = new RemotePersistenceStats();
        this.indexMetadataUploadListeners = indexMetadataUploadListeners;
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataManifest writeFullMetadata(ClusterState clusterState, String previousClusterUUID) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        UploadedMetadataResults uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values()),
            Collections.emptyMap(),
            clusterState.metadata().customs(),
            true,
            true,
            true
        );
        final ClusterMetadataManifest manifest = uploadManifest(
            clusterState,
            uploadedMetadataResults.uploadedIndexMetadata,
            previousClusterUUID,
            uploadedMetadataResults.uploadedCoordinationMetadata,
            uploadedMetadataResults.uploadedSettingsMetadata,
            uploadedMetadataResults.uploadedTemplatesMetadata,
            uploadedMetadataResults.uploadedCustomMetadataMap,
            false
        );
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        } else {
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices and global metadata",
                durationMillis,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        }
        return manifest;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return The uploaded ClusterMetadataManifest file
     */
    @Nullable
    public ClusterMetadataManifest writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();

        final Map<String, UploadedMetadataAttribute> customsToBeDeletedFromRemote = new HashMap<>(previousManifest.getCustomMetadataMap());
        final Map<String, Metadata.Custom> customsToUpload = getUpdatedCustoms(clusterState, previousClusterState);
        final Map<String, UploadedMetadataAttribute> allUploadedCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        for (final String custom : clusterState.metadata().customs().keySet()) {
            // remove all the customs which are present currently
            customsToBeDeletedFromRemote.remove(custom);
        }

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
        UploadedMetadataResults uploadedMetadataResults;
        // For migration case from codec V0 or V1 to V2, we have added null check on metadata attribute files,
        // If file is empty and codec is 1 then write global metadata.
        boolean firstUploadForSplitGlobalMetadata = !previousManifest.hasMetadataAttributesFiles();
        boolean updateCoordinationMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isCoordinationMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        ;
        boolean updateSettingsMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isSettingsMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        boolean updateTemplatesMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isTemplatesMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;

        uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            toUpload,
            prevIndexMetadataByName,
            firstUploadForSplitGlobalMetadata ? clusterState.metadata().customs() : customsToUpload,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata
        );

        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        // remove the data for removed custom/indices
        customsToBeDeletedFromRemote.keySet().forEach(allUploadedCustomMap::remove);
        indicesToBeDeletedFromRemote.keySet().forEach(allUploadedIndexMetadata::remove);

        final ClusterMetadataManifest manifest = uploadManifest(
            clusterState,
            new ArrayList<>(allUploadedIndexMetadata.values()),
            previousManifest.getPreviousClusterUUID(),
            updateCoordinationMetadata ? uploadedMetadataResults.uploadedCoordinationMetadata : previousManifest.getCoordinationMetadata(),
            updateSettingsMetadata ? uploadedMetadataResults.uploadedSettingsMetadata : previousManifest.getSettingsMetadata(),
            updateTemplatesMetadata ? uploadedMetadataResults.uploadedTemplatesMetadata : previousManifest.getTemplatesMetadata(),
            firstUploadForSplitGlobalMetadata || !customsToUpload.isEmpty()
                ? allUploadedCustomMap
                : previousManifest.getCustomMetadataMap(),
            false
        );
        deleteStaleClusterMetadata(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), RETAINED_MANIFESTS);

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        ParameterizedMessage clusterStateUploadTimeMessage = new ParameterizedMessage(
            CLUSTER_STATE_UPLOAD_TIME_LOG_STRING,
            manifest.getStateVersion(),
            durationMillis
        );
        ParameterizedMessage metadataUpdateMessage = new ParameterizedMessage(
            METADATA_UPDATE_LOG_STRING,
            numIndicesUpdated,
            numIndicesUnchanged,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            customsToUpload.size()
        );
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "{} which is above the warn threshold of [{}]; {}",
                clusterStateUploadTimeMessage,
                slowWriteLoggingThreshold,
                metadataUpdateMessage
            );
        } else {
            logger.info("{}; {}", clusterStateUploadTimeMessage, metadataUpdateMessage);
        }
        return manifest;
    }

    private UploadedMetadataResults writeMetadataInParallel(
        ClusterState clusterState,
        List<IndexMetadata> indexToUpload,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        Map<String, Metadata.Custom> customToUpload,
        boolean uploadCoordinationMetadata,
        boolean uploadSettingsMetadata,
        boolean uploadTemplateMetadata
    ) throws IOException {
        assert Objects.nonNull(indexMetadataUploadListeners) : "indexMetadataUploadListeners can not be null";
        int totalUploadTasks = indexToUpload.size() + indexMetadataUploadListeners.size() + customToUpload.size()
            + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        Map<String, CheckedRunnable<IOException>> uploadTasks = new HashMap<>(totalUploadTasks);
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new HashMap<>(totalUploadTasks);
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
            uploadTasks.put(
                SETTING_METADATA,
                getAsyncMetadataWriteAction(
                    clusterState,
                    SETTING_METADATA,
                    SETTINGS_METADATA_FORMAT,
                    clusterState.metadata().persistentSettings(),
                    listener
                )
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.put(
                COORDINATION_METADATA,
                getAsyncMetadataWriteAction(
                    clusterState,
                    COORDINATION_METADATA,
                    COORDINATION_METADATA_FORMAT,
                    clusterState.metadata().coordinationMetadata(),
                    listener
                )
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.put(
                TEMPLATES_METADATA,
                getAsyncMetadataWriteAction(
                    clusterState,
                    TEMPLATES_METADATA,
                    TEMPLATES_METADATA_FORMAT,
                    clusterState.metadata().templatesMetadata(),
                    listener
                )
            );
        }
        customToUpload.forEach((key, value) -> {
            String customComponent = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, key);
            uploadTasks.put(
                customComponent,
                getAsyncMetadataWriteAction(clusterState, customComponent, CUSTOM_METADATA_FORMAT, value, listener)
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.put(indexMetadata.getIndex().getName(), getIndexMetadataAsyncAction(clusterState, indexMetadata, listener));
        });

        // start async upload of all required metadata files
        for (CheckedRunnable<IOException> uploadTask : uploadTasks.values()) {
            uploadTask.run();
        }

        invokeIndexMetadataUploadListeners(indexToUpload, prevIndexMetadataByName, latch, exceptionList);

        try {
            if (latch.await(getGlobalMetadataUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                // TODO: We should add metrics where transfer is timing out. [Issue: #10687]
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of following metadata to complete - %s",
                        String.join(", ", uploadTasks.keySet())
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for transfer of metadata to complete - %s",
                    String.join(", ", uploadTasks.keySet())
                ),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Exception during transfer of following metadata to Remote - %s",
                    String.join(", ", uploadTasks.keySet())
                )
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        UploadedMetadataResults response = new UploadedMetadataResults();
        results.forEach((name, uploadedMetadata) -> {
            if (name.contains(CUSTOM_METADATA)) {
                // component name for custom metadata will look like custom--<metadata-attribute>
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (COORDINATION_METADATA.equals(name)) {
                response.uploadedCoordinationMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (SETTING_METADATA.equals(name)) {
                response.uploadedSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (TEMPLATES_METADATA.equals(name)) {
                response.uploadedTemplatesMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (name.contains(UploadedIndexMetadata.COMPONENT_PREFIX)) {
                response.uploadedIndexMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else {
                throw new IllegalStateException("Unknown metadata component name " + name);
            }
        });
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

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param clusterState          current ClusterState
     * @param indexMetadata         {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    private CheckedRunnable<IOException> getIndexMetadataAsyncAction(
        ClusterState clusterState,
        IndexMetadata indexMetadata,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            indexMetadata.getIndexUUID()
        );
        final String indexMetadataFilename = indexMetadataFileName(indexMetadata);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataContainer.path().buildAsString() + indexMetadataFilename
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().toString(), ex))
        );

        return () -> INDEX_METADATA_FORMAT.writeAsyncWithUrgentPriority(
            indexMetadata,
            indexMetadataContainer,
            indexMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );
    }

    /**
     * Allows async upload of Metadata components to remote
     */

    private CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        ClusterState clusterState,
        String component,
        ChecksumBlobStoreFormat componentMetadataBlobStore,
        ToXContent componentMetadata,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        final BlobContainer globalMetadataContainer = globalMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );
        final String componentMetadataFilename = metadataAttributeFileName(component, clusterState.metadata().version());
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(new UploadedMetadataAttribute(component, componentMetadataFilename)),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(component, ex))
        );
        return () -> componentMetadataBlobStore.writeAsyncWithUrgentPriority(
            componentMetadata,
            globalMetadataContainer,
            componentMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );
    }

    @Nullable
    public ClusterMetadataManifest markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        assert clusterState != null : "Last accepted cluster state is not set";
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        ClusterMetadataManifest committedManifest = uploadManifest(
            clusterState,
            previousManifest.getIndices(),
            previousManifest.getPreviousClusterUUID(),
            previousManifest.getCoordinationMetadata(),
            previousManifest.getSettingsMetadata(),
            previousManifest.getTemplatesMetadata(),
            previousManifest.getCustomMetadataMap(),
            true
        );
        deleteStaleClusterUUIDs(clusterState, committedManifest);
        return committedManifest;
    }

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    private ClusterMetadataManifest uploadManifest(
        ClusterState clusterState,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        String previousClusterUUID,
        UploadedMetadataAttribute uploadedCoordinationMetadata,
        UploadedMetadataAttribute uploadedSettingsMetadata,
        UploadedMetadataAttribute uploadedTemplatesMetadata,
        Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String manifestFileName = getManifestFileName(
                clusterState.term(),
                clusterState.version(),
                committed,
                MANIFEST_CURRENT_CODEC_VERSION
            );
            final ClusterMetadataManifest manifest = new ClusterMetadataManifest(
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                Version.CURRENT,
                nodeId,
                committed,
                MANIFEST_CURRENT_CODEC_VERSION,
                null,
                uploadedIndexMetadata,
                previousClusterUUID,
                clusterState.metadata().clusterUUIDCommitted(),
                uploadedCoordinationMetadata,
                uploadedSettingsMetadata,
                uploadedTemplatesMetadata,
                uploadedCustomMetadataMap,
                clusterState.routingTable().version(),
                // TODO: Add actual list of changed indices routing with index routing upload flow.
                new ArrayList<>()
            );
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest, manifestFileName);
            return manifest;
        }
    }

    private void writeMetadataManifest(String clusterName, String clusterUUID, ClusterMetadataManifest uploadManifest, String fileName)
        throws IOException {
        AtomicReference<String> result = new AtomicReference<String>();
        AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();

        final BlobContainer metadataManifestContainer = manifestContainer(clusterName, clusterUUID);

        // latch to wait until upload is not finished
        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener completionListener = new LatchedActionListener<>(ActionListener.wrap(resp -> {
            logger.trace(String.format(Locale.ROOT, "Manifest file uploaded successfully."));
        }, ex -> { exceptionReference.set(ex); }), latch);

        getClusterMetadataManifestBlobStoreFormat(fileName).writeAsyncWithUrgentPriority(
            uploadManifest,
            metadataManifestContainer,
            fileName,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete")
                );
                throw ex;
            }
        } catch (InterruptedException ex) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete - %s"),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionReference.get() != null) {
            throw new RemoteStateTransferException(exceptionReference.get().getMessage(), exceptionReference.get());
        }
        logger.debug(
            "Metadata manifest file [{}] written during [{}] phase. ",
            fileName,
            uploadManifest.isCommitted() ? "commit" : "publish"
        );
    }

    private BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(clusterName, clusterUUID).add(INDEX_PATH_TOKEN).add(indexUUID));
    }

    private BlobContainer globalMetadataContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(clusterName, clusterUUID).add(GLOBAL_METADATA_PATH_TOKEN));
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    private BlobPath getCusterMetadataBasePath(String clusterName, String clusterUUID) {
        return blobStoreRepository.basePath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID);
    }

    private BlobContainer clusterUUIDContainer(String clusterName) {
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add(CLUSTER_STATE_PATH_TOKEN)
            );
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

    private void setGlobalMetadataUploadTimeout(TimeValue newGlobalMetadataUploadTimeout) {
        this.globalMetadataUploadTimeout = newGlobalMetadataUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue newMetadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = newMetadataManifestUploadTimeout;
    }

    private Map<String, Metadata.Custom> getUpdatedCustoms(ClusterState currentState, ClusterState previousState) {
        if (Metadata.isCustomMetadataEqual(previousState.metadata(), currentState.metadata())) {
            return new HashMap<>();
        }
        Map<String, Metadata.Custom> updatedCustom = new HashMap<>();
        Set<String> currentCustoms = new HashSet<>(currentState.metadata().customs().keySet());
        for (Map.Entry<String, Metadata.Custom> cursor : previousState.metadata().customs().entrySet()) {
            if (cursor.getValue().context().contains(Metadata.XContentContext.GATEWAY)) {
                if (currentCustoms.contains(cursor.getKey())
                    && !cursor.getValue().equals(currentState.metadata().custom(cursor.getKey()))) {
                    // If the custom metadata is updated, we need to upload the new version.
                    updatedCustom.put(cursor.getKey(), currentState.metadata().custom(cursor.getKey()));
                }
                currentCustoms.remove(cursor.getKey());
            }
        }
        for (String custom : currentCustoms) {
            Metadata.Custom cursor = currentState.metadata().custom(custom);
            if (cursor.context().contains(Metadata.XContentContext.GATEWAY)) {
                updatedCustom.put(custom, cursor);
            }
        }
        return updatedCustom;
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return this.indexMetadataUploadTimeout;
    }

    public TimeValue getGlobalMetadataUploadTimeout() {
        return this.globalMetadataUploadTimeout;
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return this.metadataManifestUploadTimeout;
    }

    static String getManifestFileName(long term, long version, boolean committed, int codecVersion) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest__<inverted_term>__<inverted_version>__C/P__<inverted__timestamp>__<codec_version>
        return String.join(
            DELIMITER,
            MANIFEST_PATH_TOKEN,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            (committed ? "C" : "P"), // C for committed and P for published
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(codecVersion) // Keep the codec version at last place only, during read we reads last place to
            // determine codec version.
        );
    }

    static String indexMetadataFileName(IndexMetadata indexMetadata) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/<index_UUID>/metadata__<inverted_index_metadata_version>__<inverted__timestamp>__<codec
        // version>
        return String.join(
            DELIMITER,
            METADATA_FILE_PREFIX,
            RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION) // Keep the codec version at last place only, during read we reads last
            // place to determine codec version.
        );
    }

    private static String globalMetadataFileName(Metadata metadata) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/metadata__<inverted_metadata_version>__<inverted__timestamp>__<codec_version>
        return String.join(
            DELIMITER,
            METADATA_FILE_PREFIX,
            RemoteStoreUtils.invertLong(metadata.version()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
    }

    private static String metadataAttributeFileName(String componentPrefix, Long metadataVersion) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/<componentPrefix>__<inverted_metadata_version>__<inverted__timestamp>__<codec_version>
        return String.join(
            DELIMITER,
            componentPrefix,
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
    }

    private BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return getCusterMetadataBasePath(clusterName, clusterUUID).add(MANIFEST_PATH_TOKEN);
    }

    /**
     * Fetch latest index metadata from remote cluster state
     *
     * @param clusterUUID             uuid of cluster state to refer to in remote
     * @param clusterName             name of the cluster
     * @param clusterMetadataManifest manifest file of cluster
     * @return {@code Map<String, IndexMetadata>} latest IndexUUID to IndexMetadata map
     */
    private Map<String, IndexMetadata> getIndexMetadataMap(
        String clusterName,
        String clusterUUID,
        ClusterMetadataManifest clusterMetadataManifest
    ) {
        assert Objects.equals(clusterUUID, clusterMetadataManifest.getClusterUUID())
            : "Corrupt ClusterMetadataManifest found. Cluster UUID mismatch.";
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        for (UploadedIndexMetadata uploadedIndexMetadata : clusterMetadataManifest.getIndices()) {
            IndexMetadata indexMetadata = getIndexMetadata(clusterName, clusterUUID, uploadedIndexMetadata);
            remoteIndexMetadata.put(uploadedIndexMetadata.getIndexUUID(), indexMetadata);
        }
        return remoteIndexMetadata;
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param clusterUUID           uuid of cluster state to refer to in remote
     * @param clusterName           name of the cluster
     * @param uploadedIndexMetadata {@link UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    private IndexMetadata getIndexMetadata(String clusterName, String clusterUUID, UploadedIndexMetadata uploadedIndexMetadata) {
        BlobContainer blobContainer = indexMetadataContainer(clusterName, clusterUUID, uploadedIndexMetadata.getIndexUUID());
        try {
            String[] splitPath = uploadedIndexMetadata.getUploadedFilename().split("/");
            return INDEX_METADATA_FORMAT.read(
                blobContainer,
                splitPath[splitPath.length - 1],
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    /**
     * Fetch latest ClusterState from remote, including global metadata, index metadata and cluster state version
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@link IndexMetadata}
     */
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID) {
        Optional<ClusterMetadataManifest> clusterMetadataManifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
        if (clusterMetadataManifest.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Latest cluster metadata manifest is not present for the provided clusterUUID: %s", clusterUUID)
            );
        }

        // Fetch Global Metadata
        Metadata globalMetadata = getGlobalMetadata(clusterName, clusterUUID, clusterMetadataManifest.get());

        // Fetch Index Metadata
        Map<String, IndexMetadata> indices = getIndexMetadataMap(clusterName, clusterUUID, clusterMetadataManifest.get());

        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        indices.values().forEach(indexMetadata -> { indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata); });

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .version(clusterMetadataManifest.get().getStateVersion())
            .metadata(Metadata.builder(globalMetadata).indices(indexMetadataMap).build())
            .build();
    }

    private Metadata getGlobalMetadata(String clusterName, String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        String globalMetadataFileName = clusterMetadataManifest.getGlobalMetadataFileName();
        try {
            // Fetch Global metadata
            if (globalMetadataFileName != null) {
                String[] splitPath = globalMetadataFileName.split("/");
                return GLOBAL_METADATA_FORMAT.read(
                    globalMetadataContainer(clusterName, clusterUUID),
                    splitPath[splitPath.length - 1],
                    blobStoreRepository.getNamedXContentRegistry()
                );
            } else if (clusterMetadataManifest.hasMetadataAttributesFiles()) {
                CoordinationMetadata coordinationMetadata = getCoordinationMetadata(
                    clusterName,
                    clusterUUID,
                    clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()
                );
                Settings settingsMetadata = getSettingsMetadata(
                    clusterName,
                    clusterUUID,
                    clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()
                );
                TemplatesMetadata templatesMetadata = getTemplatesMetadata(
                    clusterName,
                    clusterUUID,
                    clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()
                );
                Metadata.Builder builder = new Metadata.Builder();
                builder.coordinationMetadata(coordinationMetadata);
                builder.persistentSettings(settingsMetadata);
                builder.templates(templatesMetadata);
                clusterMetadataManifest.getCustomMetadataMap()
                    .forEach(
                        (key, value) -> builder.putCustom(
                            key,
                            getCustomsMetadata(clusterName, clusterUUID, value.getUploadedFilename(), key)
                        )
                    );
                return builder.build();
            } else {
                return Metadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Global Metadata - %s", globalMetadataFileName),
                e
            );
        }
    }

    private CoordinationMetadata getCoordinationMetadata(String clusterName, String clusterUUID, String coordinationMetadataFileName) {
        try {
            // Fetch Coordination metadata
            if (coordinationMetadataFileName != null) {
                String[] splitPath = coordinationMetadataFileName.split("/");
                return COORDINATION_METADATA_FORMAT.read(
                    globalMetadataContainer(clusterName, clusterUUID),
                    splitPath[splitPath.length - 1],
                    blobStoreRepository.getNamedXContentRegistry()
                );
            } else {
                return CoordinationMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Coordination Metadata - %s", coordinationMetadataFileName),
                e
            );
        }
    }

    private Settings getSettingsMetadata(String clusterName, String clusterUUID, String settingsMetadataFileName) {
        try {
            // Fetch Settings metadata
            if (settingsMetadataFileName != null) {
                String[] splitPath = settingsMetadataFileName.split("/");
                return SETTINGS_METADATA_FORMAT.read(
                    globalMetadataContainer(clusterName, clusterUUID),
                    splitPath[splitPath.length - 1],
                    blobStoreRepository.getNamedXContentRegistry()
                );
            } else {
                return Settings.EMPTY;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Settings Metadata - %s", settingsMetadataFileName),
                e
            );
        }
    }

    private TemplatesMetadata getTemplatesMetadata(String clusterName, String clusterUUID, String templatesMetadataFileName) {
        try {
            // Fetch Templates metadata
            if (templatesMetadataFileName != null) {
                String[] splitPath = templatesMetadataFileName.split("/");
                return TEMPLATES_METADATA_FORMAT.read(
                    globalMetadataContainer(clusterName, clusterUUID),
                    splitPath[splitPath.length - 1],
                    blobStoreRepository.getNamedXContentRegistry()
                );
            } else {
                return TemplatesMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Templates Metadata - %s", templatesMetadataFileName),
                e
            );
        }
    }

    private Metadata.Custom getCustomsMetadata(String clusterName, String clusterUUID, String customMetadataFileName, String custom) {
        requireNonNull(customMetadataFileName);
        try {
            // Fetch Custom metadata
            String[] splitPath = customMetadataFileName.split("/");
            ChecksumBlobStoreFormat<Metadata.Custom> customChecksumBlobStoreFormat = new ChecksumBlobStoreFormat<>(
                "custom",
                METADATA_NAME_FORMAT,
                (parser -> Metadata.Custom.fromXContent(parser, custom))
            );
            return customChecksumBlobStoreFormat.read(
                globalMetadataContainer(clusterName, clusterUUID),
                splitPath[splitPath.length - 1],
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Custom Metadata - %s", customMetadataFileName),
                e
            );
        }
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
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
            Map<String, ClusterMetadataManifest> latestManifests = getLatestManifestForAllClusterUUIDs(clusterName, clusterUUIDs);
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

    private Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(clusterName).children();
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
            return Collections.emptyList();
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
                    && isGlobalMetadataEqual(currentManifest, previousManifest, clusterName)) {
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
            .collect(Collectors.toMap(md -> md.getIndexName(), Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = getIndexMetadata(clusterName, first.getClusterUUID(), uploadedIndexMetadata);
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = getIndexMetadata(clusterName, second.getClusterUUID(), secondUploadedIndexMetadata);
            if (firstIndexMetadata.equals(secondIndexMetadata) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isGlobalMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        Metadata secondGlobalMetadata = getGlobalMetadata(clusterName, second.getClusterUUID(), second);
        Metadata firstGlobalMetadata = getGlobalMetadata(clusterName, first.getClusterUUID(), first);
        return Metadata.isGlobalResourcesMetadataEquals(firstGlobalMetadata, secondGlobalMetadata);
    }

    private boolean isValidClusterUUID(ClusterMetadataManifest manifest) {
        return manifest.isClusterUUIDCommitted();
    }

    /**
     * Fetch ClusterMetadataManifest files from remote state store in order
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param limit       max no of files to fetch
     * @return all manifest file names
     */
    private List<BlobMetadata> getManifestFileNames(String clusterName, String clusterUUID, int limit) throws IllegalStateException {
        try {

            /*
              {@link BlobContainer#listBlobsByPrefixInSortedOrder} will list the latest manifest file first
              as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
              when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            return manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                MANIFEST_FILE_PREFIX + DELIMITER,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    private ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            return getClusterMetadataManifestBlobStoreFormat(filename).read(
                manifestContainer(clusterName, clusterUUID),
                filename,
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    private ChecksumBlobStoreFormat<ClusterMetadataManifest> getClusterMetadataManifestBlobStoreFormat(String fileName) {
        long codecVersion = getManifestCodecVersion(fileName);
        if (codecVersion == MANIFEST_CURRENT_CODEC_VERSION) {
            return CLUSTER_METADATA_MANIFEST_FORMAT;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V1) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V1;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V0) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V0;
        }

        throw new IllegalArgumentException("Cluster metadata manifest file is corrupted, don't have valid codec version");
    }

    private int getManifestCodecVersion(String fileName) {
        String[] splitName = fileName.split(DELIMITER);
        if (splitName.length == SPLITED_MANIFEST_FILE_LENGTH) {
            return Integer.parseInt(splitName[splitName.length - 1]); // Last value would be codec version.
        } else if (splitName.length < SPLITED_MANIFEST_FILE_LENGTH) { // Where codec is not part of file name, i.e. default codec version 0
            // is used.
            return ClusterMetadataManifest.CODEC_V0;
        } else {
            throw new IllegalArgumentException("Manifest file name is corrupted");
        }
    }

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    public void writeMetadataFailed() {
        getStats().stateFailed();
    }

    /**
     * Exception for Remote state transfer.
     */
    public static class RemoteStateTransferException extends RuntimeException {

        public RemoteStateTransferException(String errorDesc) {
            super(errorDesc);
        }

        public RemoteStateTransferException(String errorDesc, Throwable cause) {
            super(errorDesc, cause);
        }
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     *
     * @param clusterName  name of the cluster
     * @param clusterUUIDs clusteUUIDs for which the remote state needs to be purged
     */
    void deleteStaleUUIDsClusterMetadata(String clusterName, List<String> clusterUUIDs) {
        clusterUUIDs.forEach(clusterUUID -> {
            getBlobStoreTransferService().deleteAsync(
                ThreadPool.Names.REMOTE_PURGE,
                getCusterMetadataBasePath(clusterName, clusterUUID),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.info("Deleted all remote cluster metadata for cluster UUID - {}", clusterUUID);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting all remote cluster metadata for cluster UUID {}",
                                clusterUUID
                            ),
                            e
                        );
                        remoteStateStats.cleanUpAttemptFailed();
                    }
                }
            );
        });
    }

    /**
     * Deletes older than last {@code versionsToRetain} manifests. Also cleans up unreferenced IndexMetadata associated with older manifests
     *
     * @param clusterName       name of the cluster
     * @param clusterUUID       uuid of cluster state to refer to in remote
     * @param manifestsToRetain no of latest manifest files to keep in remote
     */
    // package private for testing
    void deleteStaleClusterMetadata(String clusterName, String clusterUUID, int manifestsToRetain) {
        if (deleteStaleMetadataRunning.compareAndSet(false, true) == false) {
            logger.info("Delete stale cluster metadata task is already in progress.");
            return;
        }
        try {
            getBlobStoreTransferService().listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                getManifestFolderPath(clusterName, clusterUUID),
                "manifest",
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        if (blobMetadata.size() > manifestsToRetain) {
                            deleteClusterMetadata(
                                clusterName,
                                clusterUUID,
                                blobMetadata.subList(0, manifestsToRetain - 1),
                                blobMetadata.subList(manifestsToRetain - 1, blobMetadata.size())
                            );
                        }
                        deleteStaleMetadataRunning.set(false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting Remote Cluster Metadata for clusterUUIDs {}",
                                clusterUUID
                            )
                        );
                        deleteStaleMetadataRunning.set(false);
                    }
                }
            );
        } catch (Exception e) {
            deleteStaleMetadataRunning.set(false);
            throw e;
        }
    }

    private void deleteClusterMetadata(
        String clusterName,
        String clusterUUID,
        List<BlobMetadata> activeManifestBlobMetadata,
        List<BlobMetadata> staleManifestBlobMetadata
    ) {
        try {
            Set<String> filesToKeep = new HashSet<>();
            Set<String> staleManifestPaths = new HashSet<>();
            Set<String> staleIndexMetadataPaths = new HashSet<>();
            Set<String> staleGlobalMetadataPaths = new HashSet<>();
            activeManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                clusterMetadataManifest.getIndices()
                    .forEach(uploadedIndexMetadata -> filesToKeep.add(uploadedIndexMetadata.getUploadedFilename()));
                if (clusterMetadataManifest.getGlobalMetadataFileName() != null) {
                    filesToKeep.add(clusterMetadataManifest.getGlobalMetadataFileName());
                } else {
                    filesToKeep.add(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
                    clusterMetadataManifest.getCustomMetadataMap()
                        .forEach((key, value) -> { filesToKeep.add(value.getUploadedFilename()); });
                }
            });
            staleManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                staleManifestPaths.add(new BlobPath().add(MANIFEST_PATH_TOKEN).buildAsString() + blobMetadata.name());
                if (clusterMetadataManifest.getGlobalMetadataFileName() != null) {
                    if (filesToKeep.contains(clusterMetadataManifest.getGlobalMetadataFileName()) == false) {
                        String[] globalMetadataSplitPath = clusterMetadataManifest.getGlobalMetadataFileName().split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                globalMetadataSplitPath[globalMetadataSplitPath.length - 1]
                            )
                        );
                    }
                } else {
                    if (filesToKeep.contains(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()) == false) {
                        String[] coordinationMetadataSplitPath = clusterMetadataManifest.getCoordinationMetadata()
                            .getUploadedFilename()
                            .split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                coordinationMetadataSplitPath[coordinationMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()) == false) {
                        String[] templatesMetadataSplitPath = clusterMetadataManifest.getTemplatesMetadata()
                            .getUploadedFilename()
                            .split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                templatesMetadataSplitPath[templatesMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()) == false) {
                        String[] settingsMetadataSplitPath = clusterMetadataManifest.getSettingsMetadata().getUploadedFilename().split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                settingsMetadataSplitPath[settingsMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    clusterMetadataManifest.getCustomMetadataMap().forEach((key, value) -> {
                        if (filesToKeep.contains(value.getUploadedFilename()) == false) {
                            String[] customMetadataSplitPath = value.getUploadedFilename().split("/");
                            staleGlobalMetadataPaths.add(
                                new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                    customMetadataSplitPath[customMetadataSplitPath.length - 1]
                                )
                            );
                        }
                    });
                }

                clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
                    if (filesToKeep.contains(uploadedIndexMetadata.getUploadedFilename()) == false) {
                        staleIndexMetadataPaths.add(
                            new BlobPath().add(INDEX_PATH_TOKEN).add(uploadedIndexMetadata.getIndexUUID()).buildAsString()
                                + INDEX_METADATA_FORMAT.blobName(uploadedIndexMetadata.getUploadedFilename())
                        );
                    }
                });
            });

            if (staleManifestPaths.isEmpty()) {
                logger.debug("No stale Remote Cluster Metadata files found");
                return;
            }

            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleGlobalMetadataPaths));
            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleIndexMetadataPaths));
            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleManifestPaths));
        } catch (IllegalStateException e) {
            logger.error("Error while fetching Remote Cluster Metadata manifests", e);
        } catch (IOException e) {
            logger.error("Error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        } catch (Exception e) {
            logger.error("Unexpected error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        }
    }

    private void deleteStalePaths(String clusterName, String clusterUUID, List<String> stalePaths) throws IOException {
        logger.debug(String.format(Locale.ROOT, "Deleting stale files from remote - %s", stalePaths));
        getBlobStoreTransferService().deleteBlobs(getCusterMetadataBasePath(clusterName, clusterUUID), stalePaths);
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     *
     * @param clusterState      current state of the cluster
     * @param committedManifest last committed ClusterMetadataManifest
     */
    public void deleteStaleClusterUUIDs(ClusterState clusterState, ClusterMetadataManifest committedManifest) {
        threadpool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
            String clusterName = clusterState.getClusterName().value();
            logger.debug("Deleting stale cluster UUIDs data from remote [{}]", clusterName);
            Set<String> allClustersUUIDsInRemote;
            try {
                allClustersUUIDsInRemote = new HashSet<>(getAllClusterUUIDs(clusterState.getClusterName().value()));
            } catch (IOException e) {
                logger.info(String.format(Locale.ROOT, "Error while fetching all cluster UUIDs for [%s]", clusterName));
                return;
            }
            // Retain last 2 cluster uuids data
            allClustersUUIDsInRemote.remove(committedManifest.getClusterUUID());
            allClustersUUIDsInRemote.remove(committedManifest.getPreviousClusterUUID());
            deleteStaleUUIDsClusterMetadata(clusterName, new ArrayList<>(allClustersUUIDsInRemote));
        });
    }

    public RemotePersistenceStats getStats() {
        return remoteStateStats;
    }

    private static class UploadedMetadataResults {
        List<UploadedIndexMetadata> uploadedIndexMetadata;
        Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap;
        UploadedMetadataAttribute uploadedCoordinationMetadata;
        UploadedMetadataAttribute uploadedSettingsMetadata;
        UploadedMetadataAttribute uploadedTemplatesMetadata;

        public UploadedMetadataResults(
            List<UploadedIndexMetadata> uploadedIndexMetadata,
            Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap,
            UploadedMetadataAttribute uploadedCoordinationMetadata,
            UploadedMetadataAttribute uploadedSettingsMetadata,
            UploadedMetadataAttribute uploadedTemplatesMetadata
        ) {
            this.uploadedIndexMetadata = uploadedIndexMetadata;
            this.uploadedCustomMetadataMap = uploadedCustomMetadataMap;
            this.uploadedCoordinationMetadata = uploadedCoordinationMetadata;
            this.uploadedSettingsMetadata = uploadedSettingsMetadata;
            this.uploadedTemplatesMetadata = uploadedTemplatesMetadata;
        }

        public UploadedMetadataResults() {
            this.uploadedIndexMetadata = new ArrayList<>();
            this.uploadedCustomMetadataMap = new HashMap<>();
            this.uploadedCoordinationMetadata = null;
            this.uploadedSettingsMetadata = null;
            this.uploadedTemplatesMetadata = null;
        }
    }
}
