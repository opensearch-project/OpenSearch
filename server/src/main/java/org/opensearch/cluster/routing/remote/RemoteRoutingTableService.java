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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
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
        "cluster.remote_store.routing.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    public static final String INDEX_ROUTING_PATH_TOKEN = "index-routing";
    public static final String ROUTING_TABLE = "routing-table";
    public static final String INDEX_ROUTING_FILE_PREFIX = "index_routing";
    public static final String DELIMITER = "__";
    public static final String INDEX_ROUTING_METADATA_PREFIX = "indexRouting--";
    private static final Logger logger = LogManager.getLogger(RemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

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


    public RemoteRoutingTableService(Supplier<RepositoriesService> repositoriesService,
                                     Settings settings, ThreadPool threadPool) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadPool;
    }


    private String getIndexRoutingFileName() {
        return String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );

    }

    public CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String uploadedFilename,
        Index index,
        LatchedActionListener<IndexRoutingTable> latchedActionListener) {
        int idx = uploadedFilename.lastIndexOf("/");
        String blobFileName = uploadedFilename.substring(idx+1);
        BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer( BlobPath.cleanPath().add(uploadedFilename.substring(0,idx)));

        return () -> readAsync(
            blobContainer,
            blobFileName,
            index,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ActionListener.wrap(response -> latchedActionListener.onResponse(response.getIndexRoutingTable()), latchedActionListener::onFailure)
        );
    }

    public void readAsync(BlobContainer blobContainer, String name, Index index, ExecutorService executorService, ActionListener<RemoteIndexRoutingTable> listener) throws IOException {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(blobContainer, name, index));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public RemoteIndexRoutingTable read(BlobContainer blobContainer, String path, Index index) {
        try {
            return new RemoteIndexRoutingTable(blobContainer.readBlob(path), index);
        } catch (IOException | AssertionError e) {
            logger.info("RoutingTable read failed with error: {}", e.toString());
            throw new RemoteClusterStateService.RemoteStateTransferException("Failed to read RemoteRoutingTable from Manifest with error ", e);
        }
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(List<String> updatedIndicesRouting, List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting) {
        return updatedIndicesRouting.stream().map(idx -> {
            Optional<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadataOptional = allIndicesRouting.stream().filter(idx2 -> idx2.getIndexName().equals(idx)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());
    }


    public static DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return DiffableUtils.diff(
            before.getIndicesRouting(),
            after.getIndicesRouting(),
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER
        );
    }


    @Override
    public void doClose() throws IOException {
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
