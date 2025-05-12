package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Transport action responsible for collecting segment and translog metadata
 * from all shards of a given index.
 *
 * @opensearch.internal
 */
public class TransportRemoteStoreMetadataAction extends TransportAction<RemoteStoreMetadataRequest, RemoteStoreMetadataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRemoteStoreMetadataAction.class);
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper;

    @Inject
    public TransportRemoteStoreMetadataAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(RemoteStoreMetadataAction.NAME, actionFilters, transportService.getTaskManager());
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
            new RemoteSegmentMetadataHandlerFactory(),
            RemoteSegmentMetadata.VERSION_ONE,
            RemoteSegmentMetadata.CURRENT_VERSION,
            RemoteSegmentMetadata.METADATA_CODEC
        );
    }

    @Override
    protected void doExecute(Task task, RemoteStoreMetadataRequest request, ActionListener<RemoteStoreMetadataResponse> listener) {
        try {
            ClusterState state = clusterService.state();

            // Check blocks
            ClusterBlockException blockException = checkBlocks(state, request);
            if (blockException != null) {
                listener.onFailure(blockException);
                return;
            }

            // Resolve concrete indices
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
            if (concreteIndices.length == 0) {
                listener.onResponse(new RemoteStoreMetadataResponse(new RemoteStoreMetadata[0], 0, 0, 0, Collections.emptyList()));
                return;
            }

            // Get relevant shards
            List<ShardRouting> selectedShards = getSelectedShards(state, request, concreteIndices);

            // Process each shard
            List<RemoteStoreMetadata> responses = new ArrayList<>();
            AtomicInteger successfulShards = new AtomicInteger(0);
            AtomicInteger failedShards = new AtomicInteger(0);
            List<DefaultShardOperationFailedException> shardFailures = Collections.synchronizedList(new ArrayList<>());

            for (ShardRouting shardRouting : selectedShards) {
                try {
                    RemoteStoreMetadata metadata = getShardMetadata(shardRouting);
                    responses.add(metadata);
                    successfulShards.incrementAndGet();
                } catch (Exception e) {
                    failedShards.incrementAndGet();
                    shardFailures.add(
                        new DefaultShardOperationFailedException(shardRouting.shardId().getIndexName(), shardRouting.shardId().getId(), e)
                    );
                }
            }

            RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
                responses.toArray(new RemoteStoreMetadata[0]),
                selectedShards.size(),
                successfulShards.get(),
                failedShards.get(),
                shardFailures
            );

            listener.onResponse(response);

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ClusterBlockException checkBlocks(ClusterState state, RemoteStoreMetadataRequest request) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (globalBlock != null) {
            return globalBlock;
        }

        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    private List<ShardRouting> getSelectedShards(ClusterState clusterState, RemoteStoreMetadataRequest request, String[] concreteIndices) {
        return clusterState.routingTable()
            .allShards(concreteIndices)
            .getShardRoutings()
            .stream()
            .filter(
                shardRouting -> request.shards().length == 0
                    || Arrays.asList(request.shards()).contains(Integer.toString(shardRouting.shardId().id()))
            )
            .filter(
                shardRouting -> !request.local() || Objects.equals(shardRouting.currentNodeId(), clusterState.getNodes().getLocalNodeId())
            )
            .filter(
                shardRouting -> Boolean.parseBoolean(
                    clusterState.getMetadata().index(shardRouting.index()).getSettings().get(IndexMetadata.SETTING_REMOTE_STORE_ENABLED)
                )
            )
            .collect(Collectors.toList());
    }

    private RemoteStoreMetadata getShardMetadata(ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        String repoLocation = clusterService.localNode().getAttributes().get("remote_store.repository.my-repository.settings.location");

        String indexUUID = shardRouting.index().getUUID();
        int shardId = shardRouting.shardId().id();

        String segmentMetadataPath = String.format("%s/%s/%d/segments/metadata", repoLocation, indexUUID, shardId);
        String translogMetadataPath = String.format("%s/%s/%d/translog/metadata", repoLocation, indexUUID, shardId);

        Map<String, Object> segmentMetadata = readSegmentMetadata(segmentMetadataPath);
        Map<String, Object> translogMetadata = readTranslogMetadata(translogMetadataPath);

        return new RemoteStoreMetadata(segmentMetadata, translogMetadata, shardRouting);
    }

    private Map<String, Object> readSegmentMetadata(String metadataPath) throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        Path path = Path.of(metadataPath);
        if (!Files.exists(path)) return metadata;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "metadata__*")) {
            for (Path metadataFile : stream) {
                try (
                    InputStream in = Files.newInputStream(metadataFile);
                    IndexInput idxIn = new ByteArrayIndexInput(metadataFile.getFileName().toString(), in.readAllBytes())
                ) {

                    RemoteSegmentMetadata segMetadata = metadataStreamWrapper.readStream(idxIn);
                    Map<String, Object> fileMetadata = new HashMap<>();

                    Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = segMetadata.getMetadata();
                    if (uploadedSegments != null) {
                        Map<String, Map<String, Object>> segmentsInfo = new HashMap<>();
                        uploadedSegments.forEach((segmentFile, uploadedMetadata) -> {
                            Map<String, Object> segInfo = new HashMap<>();
                            segInfo.put("original_name", uploadedMetadata.getOriginalFilename());
                            segInfo.put("checksum", uploadedMetadata.getChecksum());
                            segInfo.put("length", uploadedMetadata.getLength());
                            segmentsInfo.put(segmentFile, segInfo);
                        });
                        fileMetadata.put("uploaded_segments", segmentsInfo);
                    }

                    if (segMetadata.getReplicationCheckpoint() != null) {
                        var cp = segMetadata.getReplicationCheckpoint();
                        fileMetadata.put(
                            "replication_checkpoint",
                            Map.of(
                                "shard_id",
                                cp.getShardId().toString(),
                                "primary_term",
                                cp.getPrimaryTerm(),
                                "generation",
                                cp.getSegmentsGen(),
                                "version",
                                cp.getSegmentInfosVersion(),
                                "length",
                                cp.getLength(),
                                "codec",
                                cp.getCodec(),
                                "created_timestamp",
                                cp.getCreatedTimeStamp()
                            )
                        );
                    }

                    fileMetadata.put("generation", segMetadata.getGeneration());
                    fileMetadata.put("primary_term", segMetadata.getPrimaryTerm());
                    metadata.put(metadataFile.getFileName().toString(), fileMetadata);
                }
            }
        }
        return metadata;
    }

    private Map<String, Object> readTranslogMetadata(String metadataPath) throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        Path path = Path.of(metadataPath);
        if (!Files.exists(path)) return metadata;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "metadata__*")) {
            for (Path metadataFile : stream) {
                try (
                    InputStream inputStream = Files.newInputStream(metadataFile);
                    BytesStreamInput input = new BytesStreamInput(inputStream.readAllBytes())
                ) {

                    Map<String, Object> fileMetadata = new HashMap<>();
                    String[] parts = metadataFile.getFileName().toString().split(TranslogTransferMetadata.METADATA_SEPARATOR);
                    fileMetadata.put("primary_term", Long.parseLong(parts[1]));
                    fileMetadata.put("generation", Long.parseLong(parts[2]));
                    fileMetadata.put("timestamp", Long.parseLong(parts[3]));
                    fileMetadata.put("node_id_hash", parts[4]);
                    fileMetadata.put("min_translog_gen", Long.parseLong(parts[5]));
                    fileMetadata.put("min_primary_term", parts[6]);
                    fileMetadata.put("version", parts[7]);

                    long primaryTerm = input.readLong();
                    long generation = input.readLong();
                    long minTranslogGen = input.readLong();
                    Map<String, String> genToTermMap = input.readMap(StreamInput::readString, StreamInput::readString);
                    fileMetadata.put(
                        "content",
                        Map.of(
                            "primary_term",
                            primaryTerm,
                            "generation",
                            generation,
                            "min_translog_generation",
                            minTranslogGen,
                            "generation_to_term_mapping",
                            genToTermMap
                        )
                    );
                    metadata.put(metadataFile.getFileName().toString(), fileMetadata);
                }
            }
        }
        return metadata;
    }
}
