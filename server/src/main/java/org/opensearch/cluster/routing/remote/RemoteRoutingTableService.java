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
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
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

    private static final DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER = new DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable>() {
        @Override
        public void write(IndexRoutingTable value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }

        @Override
        public IndexRoutingTable read(StreamInput in, String key) throws IOException {
            return IndexRoutingTable.readFrom(in);
        }
    };

    public static DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return DiffableUtils.diff(
            before.getIndicesRouting(),
            after.getIndicesRouting(),
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER
        );
    }

    public CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    ) throws IOException {

        BlobPath indexRoutingPath = clusterBasePath.add(INDEX_ROUTING_PATH_TOKEN);
        BlobPath path = RemoteStoreEnums.PathType.HASHED_PREFIX.path(RemoteStorePathStrategy.BasePathInput.builder()
                .basePath(indexRoutingPath)
                .indexUUID(indexRouting.getIndex().getUUID())
                .build(),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64);
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        final String fileName = getIndexRoutingFileName();

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedIndexMetadata(
                    indexRouting.getIndex().getName(),
                    indexRouting.getIndex().getUUID(),
                    path.buildAsString() + fileName,
                    INDEX_ROUTING_METADATA_PREFIX
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateService.RemoteStateTransferException("Exception in writing index to remote store: " + indexRouting.getIndex().toString(), ex))
        );

        return () -> uploadIndex(indexRouting, fileName , blobContainer, completionListener);
    }


    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(ClusterMetadataManifest previousManifest, List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingToUpload, Set<String> indicesRoutingToDelete) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingToUpload.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );
        indicesRoutingToDelete.forEach(allUploadedIndicesRouting::remove);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    private void uploadIndex(IndexRoutingTable indexRouting, String fileName, BlobContainer blobContainer, ActionListener<Void> completionListener) throws IOException {
        RemoteIndexRoutingTable indexRoutingInput = new RemoteIndexRoutingTable(indexRouting);
        BytesReference bytesInput = null;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            indexRoutingInput.writeTo(streamOutput);
            bytesInput = streamOutput.bytes();
        } catch (IOException e) {
            throw new IOException("Failed to serialize IndexRoutingTable. ", e);
        }

        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
                try {
                    blobContainer.writeBlob(fileName, bytesInput.streamInput(), bytesInput.length(), true);
                    completionListener.onResponse(null);
                } catch (IOException e) {
                    throw new IOException("Failed to write IndexRoutingTable to remote store. ", e);
                }
                return;
        }

        try (
            IndexInput input = new ByteArrayIndexInput("indexrouting",BytesReference.toBytes(bytesInput))) {
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
                 ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), completionListener);
            } catch (IOException e) {
                throw new IOException("Failed to write IndexRoutingTable to remote store. ", e);
            }
        } catch (IOException e) {
            throw new IOException("Failed to create transfer object for IndexRoutingTable for remote store upload. ", e);
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
