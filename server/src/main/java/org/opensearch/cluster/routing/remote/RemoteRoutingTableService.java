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
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInput;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.lucene.store.IndexInput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;

import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class RemoteRoutingTableService extends AbstractLifecycleComponent {

    /**
     * Cluster setting to specify if routing table should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_ROUTING_TABLE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.routing_table.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final String INDEX_ROUTING_PATH_TOKEN = "index-routing";
    public static final String INDEX_ROUTING_FILE_PREFIX = "index_routing";
    public static final String DELIMITER = "__";
    public static final String INDEX_ROUTING_METADATA_PREFIX = "indexRouting--";

    private static final Logger logger = LogManager.getLogger(RemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private BlobStoreRepository blobStoreRepository;

    public RemoteRoutingTableService(Supplier<RepositoriesService> repositoriesService, Settings settings) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
    }

    public List<IndexRoutingTable> getChangedIndicesRouting(ClusterState previousClusterState,
                                                            ClusterState clusterState) {
        Map<String, IndexRoutingTable> previousIndexRoutingTable = previousClusterState.getRoutingTable().getIndicesRouting();
        List<IndexRoutingTable> changedIndicesRouting = new ArrayList<>();
        for (IndexRoutingTable indexRouting : clusterState.getRoutingTable().getIndicesRouting().values()) {
            if (!(previousIndexRoutingTable.containsKey(indexRouting.getIndex().getName()) && indexRouting.equals(previousIndexRoutingTable.get(indexRouting.getIndex().getName())))) {
                changedIndicesRouting.add(indexRouting);
                logger.info("changedIndicesRouting {}", indexRouting.prettyPrint());
            }
        }

        return changedIndicesRouting;
    }

    public CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    ) throws IOException {

        BlobPath indexRoutingPath = clusterBasePath.add(INDEX_ROUTING_PATH_TOKEN);
        logger.info("custerMetadataBasePath {}", indexRoutingPath);

        BlobPath path = RemoteStoreEnums.PathType.HASHED_PREFIX.path(RemoteStorePathStrategy.BasePathInput.builder()
                .basePath(indexRoutingPath)
                .indexUUID(indexRouting.getIndex().getUUID())
                .build(),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64);
        logger.info("path from prefix hasd  {}", path);
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        final String fileName = getIndexRoutingFileName();
        logger.info("fileName {}", fileName);

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedIndexMetadata(
                    indexRouting.getIndex().getName(),
                    indexRouting.getIndex().getUUID(),
                    path.buildAsString() + fileName,
                    INDEX_ROUTING_METADATA_PREFIX
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateService.RemoteStateTransferException(indexRouting.getIndex().toString(), ex))
        );

        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            logger.info("TRYING FILE UPLOAD");

            return () -> {
                logger.info("Going to upload {}", indexRouting.prettyPrint());

                uploadIndex(indexRouting, fileName , blobContainer);
                logger.info("upload done {}", indexRouting.prettyPrint());

                completionListener.onResponse(null);
                logger.info("response done {}", indexRouting.prettyPrint());

            };
        }

        logger.info("TRYING S3 UPLOAD");

        IndexRoutingTableInput indexRoutingInput = new IndexRoutingTableInput(indexRouting);
        BytesReference bytesInput = indexRoutingInput.write();
        try (
            IndexInput input = new ByteArrayIndexInput("indexrouting",BytesReference.toBytes(bytesInput))) {
            long expectedChecksum;
            try {
                expectedChecksum = checksumOfChecksum(input.clone(), 8);
            } catch (Exception e) {
                throw e;
            }
            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    fileName,
                    fileName,
                    input.length(),
                    true,
                    WritePriority.URGENT,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                    expectedChecksum,
                    ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported()
                )
            ) {
                return () -> ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), completionListener);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(ClusterMetadataManifest previousManifest, List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingToUpload, Set<String> indicesRoutingToDelete) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingToUpload.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );

        indicesRoutingToDelete.forEach(index -> allUploadedIndicesRouting.remove(index));

        logger.info("allUploadedIndicesRouting ROUTING {}", allUploadedIndicesRouting);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    private void uploadIndex(IndexRoutingTable indexRouting, String fileName, BlobContainer container) {
        logger.info("Starting write");

        try {
            IndexRoutingTableInput indexRoutingInput = new IndexRoutingTableInput(indexRouting);
            BytesReference bytesInput = indexRoutingInput.write();
            container.writeBlob(fileName, bytesInput.streamInput(), bytesInput.length(), true);
            logger.info("SUccessful write");
        } catch (IOException e) {
            logger.error("Failed to write {}", e);
        }
    }

    private String getIndexRoutingFileName() {
        return String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
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

}
