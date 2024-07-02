/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteStateTransferException;
import org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class InternalRemoteRoutingTableService extends AbstractLifecycleComponent implements RemoteRoutingTableService {

    /**
     * This setting is used to set the remote routing table store blob store path type strategy.
     */
    public static final Setting<RemoteStoreEnums.PathType> REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.routing_table.path_type",
        RemoteStoreEnums.PathType.HASHED_PREFIX.toString(),
        RemoteStoreEnums.PathType::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting is used to set the remote routing table store blob store path hash algorithm strategy.
     * This setting will come to effect if the {@link #REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING}
     * is either {@code HASHED_PREFIX} or {@code HASHED_INFIX}.
     */
    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING = new Setting<>(
        "cluster.remote_store.routing_table.path_hash_algo",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.toString(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String INDEX_ROUTING_PATH_TOKEN = "index-routing";
    public static final String INDEX_ROUTING_FILE_PREFIX = "index_routing";
    public static final String INDEX_ROUTING_METADATA_PREFIX = "indexRouting--";

    private static final Logger logger = LogManager.getLogger(InternalRemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private RemoteStoreEnums.PathType pathType;
    private RemoteStoreEnums.PathHashAlgorithm pathHashAlgo;
    private ThreadPool threadPool;

    public InternalRemoteRoutingTableService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadpool
    ) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.pathType = clusterSettings.get(REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING);
        this.pathHashAlgo = clusterSettings.get(REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING, this::setPathTypeSetting);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING, this::setPathHashAlgoSetting);
        this.threadPool = threadpool;
    }

    private void setPathTypeSetting(RemoteStoreEnums.PathType pathType) {
        this.pathType = pathType;
    }

    private void setPathHashAlgoSetting(RemoteStoreEnums.PathHashAlgorithm pathHashAlgo) {
        this.pathHashAlgo = pathHashAlgo;
    }

    public List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable) {
        return new ArrayList<>(routingTable.indicesRouting().values());
    }

    /**
     * Returns diff between the two routing tables, which includes upserts and deletes.
     * @param before previous routing table
     * @param after current routing table
     * @return diff of the previous and current routing table
     */
    public DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    ) {
        return DiffableUtils.diff(
            before.getIndicesRouting(),
            after.getIndicesRouting(),
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER
        );
    }

    /**
     * Create async action for writing one {@code IndexRoutingTable} to remote store
     * @param clusterState current cluster state
     * @param indexRouting indexRoutingTable to write to remote store
     * @param latchedActionListener listener for handling async action response
     * @param clusterBasePath base path for remote file
     * @return returns runnable async action
     */
    public CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    ) {

        BlobPath indexRoutingPath = clusterBasePath.add(INDEX_ROUTING_PATH_TOKEN);
        BlobPath path = pathType.path(
            RemoteStorePathStrategy.PathInput.builder().basePath(indexRoutingPath).indexUUID(indexRouting.getIndex().getUUID()).build(),
            pathHashAlgo
        );
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        final String fileName = getIndexRoutingFileName(clusterState.term(), clusterState.version());

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedIndexMetadata(
                    indexRouting.getIndex().getName(),
                    indexRouting.getIndex().getUUID(),
                    path.buildAsString() + fileName,
                    INDEX_ROUTING_METADATA_PREFIX
                )
            ),
            ex -> latchedActionListener.onFailure(
                new RemoteStateTransferException("Exception in writing index to remote store: " + indexRouting.getIndex().toString(), ex)
            )
        );

        return () -> uploadIndex(indexRouting, fileName, blobContainer, completionListener);
    }

    /**
     * Combines IndicesRoutingMetadata from previous manifest and current uploaded indices, removes deleted indices.
     * @param previousManifest previous manifest, used to get all existing indices routing paths
     * @param indicesRoutingUploaded current uploaded indices routings
     * @param indicesRoutingToDelete indices to delete
     * @return combined list of metadata
     */
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    ) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingUploaded.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );
        indicesRoutingToDelete.forEach(allUploadedIndicesRouting::remove);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    private void uploadIndex(
        IndexRoutingTable indexRouting,
        String fileName,
        BlobContainer blobContainer,
        ActionListener<Void> completionListener
    ) {
        RemoteIndexRoutingTable indexRoutingInput = new RemoteIndexRoutingTable(indexRouting);
        BytesReference bytesInput = null;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            indexRoutingInput.writeTo(streamOutput);
            bytesInput = streamOutput.bytes();
        } catch (IOException e) {
            logger.error("Failed to serialize IndexRoutingTable for [{}]: [{}]", indexRouting, e);
            completionListener.onFailure(e);
            return;
        }

        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            try {
                blobContainer.writeBlob(fileName, bytesInput.streamInput(), bytesInput.length(), true);
                completionListener.onResponse(null);
            } catch (IOException e) {
                logger.error("Failed to write IndexRoutingTable to remote store for indexRouting [{}]: [{}]", indexRouting, e);
                completionListener.onFailure(e);
            }
            return;
        }

        try (IndexInput input = new ByteArrayIndexInput("indexrouting", BytesReference.toBytes(bytesInput))) {
            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    fileName,
                    fileName,
                    input.length(),
                    true,
                    WritePriority.URGENT,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                    null,
                    false
                )
            ) {
                ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                    remoteTransferContainer.createWriteContext(),
                    completionListener
                );
            } catch (IOException e) {
                logger.error("Failed to write IndexRoutingTable to remote store for indexRouting [{}]: [{}]", indexRouting, e);
                completionListener.onFailure(e);
            }
        } catch (IOException e) {
            logger.error(
                "Failed to create transfer object for IndexRoutingTable for remote store upload for indexRouting [{}]: [{}]",
                indexRouting,
                e
            );
            completionListener.onFailure(e);
        }
    }

    @Override
    public CheckedRunnable<IOException> getAsyncIndexRoutingReadAction(
        String uploadedFilename,
        Index index,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    ) {
        int idx = uploadedFilename.lastIndexOf("/");
        String blobFileName = uploadedFilename.substring(idx + 1);
        BlobContainer blobContainer = blobStoreRepository.blobStore()
            .blobContainer(BlobPath.cleanPath().add(uploadedFilename.substring(0, idx)));

        return () -> readAsync(
            blobContainer,
            blobFileName,
            index,
            threadPool.executor(ThreadPool.Names.REMOTE_STATE_READ),
            ActionListener.wrap(
                response -> latchedActionListener.onResponse(response.getIndexRoutingTable()),
                latchedActionListener::onFailure
            )
        );
    }

    private void readAsync(
        BlobContainer blobContainer,
        String name,
        Index index,
        ExecutorService executorService,
        ActionListener<RemoteIndexRoutingTable> listener
    ) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(blobContainer, name, index));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private RemoteIndexRoutingTable read(BlobContainer blobContainer, String path, Index index) {
        try {
            return new RemoteIndexRoutingTable(blobContainer.readBlob(path), index);
        } catch (IOException | AssertionError e) {
            logger.error(() -> new ParameterizedMessage("RoutingTable read failed for path {}", path), e);
            throw new RemoteStateTransferException("Failed to read RemoteRoutingTable from Manifest with error ", e);
        }
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    ) {
        return updatedIndicesRouting.stream().map(idx -> {
            Optional<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadataOptional = allIndicesRouting.stream()
                .filter(idx2 -> idx2.getIndexName().equals(idx))
                .findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());
    }

    private String getIndexRoutingFileName(long term, long version) {
        return String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );
    }

    @Override
    protected void doClose() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    @Override
    protected void doStart() {
        assert isRemoteRoutingTableEnabled(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    @Override
    protected void doStop() {}

    @Override
    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException {
        try {
            logger.debug(() -> "Deleting stale index routing files from remote - " + stalePaths);
            blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath()).deleteBlobsIgnoringIfNotExists(stalePaths);
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete some stale index routing paths from {}", stalePaths), e);
            throw e;
        }
    }

}
