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
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInputStream;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class RemoteRoutingTableService implements Closeable {

    /**
     * Cluster setting to specify if routing table should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_ROUTING_TABLE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.routing.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    private static final Logger logger = LogManager.getLogger(RemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private final ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;

    public RemoteRoutingTableService(Supplier<RepositoriesService> repositoriesService,
                                     Settings settings,
                                     ClusterSettings clusterSettings) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.clusterSettings = clusterSettings;
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> writeFullRoutingTable(ClusterState clusterState, String previousClusterUUID) {
        //batch index count and parallelize
        RoutingTable currentRoutingTable = clusterState.getRoutingTable();
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndices = new ArrayList<>();
        for(IndexRoutingTable indexRouting: currentRoutingTable.getIndicesRouting().values()){
            try {
                InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting, currentRoutingTable.version(), Version.CURRENT);
                BlobContainer container = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath().add("routing-table"));
                container.writeBlobWithMetadata(indexRouting.getIndex().getName(), indexRoutingStream, indexRoutingStream.read(), true, null);
                logger.info("SUccessful write");
                uploadedIndices.add(new ClusterMetadataManifest.UploadedIndexMetadata(indexRouting.getIndex().getName(), indexRouting.getIndex().getUUID(), "dummyfilename"));
            }catch (IOException e) {
                logger.error("Failed to write {}", e);
            }
        }
        logger.info("uploadedIndices {}", uploadedIndices);

        return uploadedIndices;
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> writeIncrementalRoutingTable(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));
        logger.info("allUploadedIndicesRouting ROUTING {}", allUploadedIndicesRouting);

        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndices = new ArrayList<>();
        for(IndexRoutingTable indexRouting: clusterState.getRoutingTable().getIndicesRouting().values()){
            if(previousClusterState.getRoutingTable().getIndicesRouting().containsKey(indexRouting.getIndex().getName())) {
                logger.info("index exists {}", indexRouting.getIndex().getName());
                //existing index, check if shards are changed

                IndexRoutingTable previousIndexRouting = previousClusterState.getRoutingTable().getIndicesRouting().get(indexRouting.getIndex().getName());
                if (indexRouting.equals(previousIndexRouting)){
                    uploadedIndices.add(allUploadedIndicesRouting.get(indexRouting.getIndex().getName()));
                    continue;
                }
                try {
                    InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting, clusterState.getRoutingTable().version(), Version.CURRENT);
                    BlobContainer container = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath().add("routing-table").add(indexRouting.getIndex().getName()).add(String.valueOf(clusterState.getRoutingTable().version())));
                    container.writeBlob(indexRouting.getIndex().getName(), indexRoutingStream, 4096,true);
                    logger.info("SUccessful write");
                    uploadedIndices.add(new ClusterMetadataManifest.UploadedIndexMetadata(indexRouting.getIndex().getName(), indexRouting.getIndex().getUUID(),  container.path().buildAsString()));
                } catch (IOException e) {
                    logger.error("Failed to write {}", e);
                }
            } else {
                // new index upload
                logger.info("cuurent version {}, previous sversiion {}", clusterState.getRoutingTable().version(), previousClusterState.getRoutingTable().version());
                try {
                    InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting, clusterState.getRoutingTable().version(), Version.CURRENT);
                    BlobContainer container = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath().add("routing-table").add(indexRouting.getIndex().getName()).add(String.valueOf(clusterState.getRoutingTable().version())));
                    container.writeBlob(indexRouting.getIndex().getName(), indexRoutingStream, 4096,true);
                    //container.writeBlobWithMetadata(indexRouting.getIndex().getName(), indexRoutingStream, indexRoutingStream.read(), true, null);
                    logger.info("SUccessful write");
                    uploadedIndices.add(new ClusterMetadataManifest.UploadedIndexMetadata(indexRouting.getIndex().getName(), indexRouting.getIndex().getUUID(), container.path().buildAsString()));
                } catch (IOException e) {
                    logger.error("Failed to write {}", e);
                }
            }
        }
        return uploadedIndices;
    }

    public RoutingTable getLatestRoutingTable(String clusterName, String clusterUUID) {
        return null;
    }

    public RoutingTable getIncrementalRoutingTable(ClusterState previousClusterState, ClusterMetadataManifest previousManifest, String clusterName, String clusterUUID) {
        return null;
    }

    private void deleteStaleRoutingTable(String clusterName, String clusterUUID, int manifestsToRetain) {
    }

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public void start() {
        assert isRemoteRoutingTableEnabled(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

}
