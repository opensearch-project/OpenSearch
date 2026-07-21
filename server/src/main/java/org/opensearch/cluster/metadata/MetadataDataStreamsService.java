/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateRequest;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.service.ClusterManagerTask.MODIFY_DATA_STREAM;

/**
 * Adds or removes backing indices of data streams in a single atomic cluster-state update. These are metadata-only
 * mutations; they never create, delete, restore, open, close, or relocate indices. The write index (and hence the
 * generation) is preserved: added indices become non-write backing indices, and the write index cannot be removed.
 * Added indices need not follow the {@code .ds-<dataStream>-NNNNNN} naming convention, which allows migrating
 * pre-existing regular indices into a data stream.
 * <p>
 * This service is injectable, so components such as cross-cluster replication can call {@link #modifyDataStream}
 * directly rather than through {@code ModifyDataStreamsAction}'s transport layer.
 *
 * @opensearch.internal
 */
public class MetadataDataStreamsService {

    private static final Logger logger = LogManager.getLogger(MetadataDataStreamsService.class);

    private final ClusterService clusterService;
    private final ClusterManagerTaskThrottler.ThrottlingKey modifyDataStreamTaskKey;

    public MetadataDataStreamsService(ClusterService clusterService) {
        this.clusterService = clusterService;
        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        this.modifyDataStreamTaskKey = clusterService.registerClusterManagerTask(MODIFY_DATA_STREAM, true);
    }

    /**
     * Convenience entry point for in-process callers (e.g. cross-cluster replication) that want to apply a batch of
     * data-stream actions without constructing a {@link ModifyDataStreamsClusterStateUpdateRequest}.
     *
     * @param actions                   the metadata-only actions to apply atomically
     * @param clusterManagerNodeTimeout timeout for reaching the cluster-manager node
     * @param ackTimeout                timeout for the cluster-state update to be acknowledged
     * @param listener                  notified when the update completes
     */
    public void modifyDataStream(
        final List<DataStreamAction> actions,
        final TimeValue clusterManagerNodeTimeout,
        final TimeValue ackTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        modifyDataStream(new ModifyDataStreamsClusterStateUpdateRequest(actions, clusterManagerNodeTimeout, ackTimeout), listener);
    }

    public void modifyDataStream(
        final ModifyDataStreamsClusterStateUpdateRequest request,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        if (request.actions.isEmpty()) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }
        ActionListener<ClusterStateUpdateResponse> wrappedListener = ActionListener.map(
            listener,
            response -> new AcknowledgedResponse(response.isAcknowledged())
        );
        clusterService.submitStateUpdateTask(
            "update-data-streams",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.HIGH, request, wrappedListener) {
                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return modifyDataStreamTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return modifyDataStream(currentState, request.actions);
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            }
        );
    }

    /**
     * Applies the given actions to the current cluster state and returns the updated state. Visible for testing.
     */
    static ClusterState modifyDataStream(ClusterState currentState, Iterable<DataStreamAction> actions) {
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        // Track streams we've mutated so multiple actions against the same stream compose within this single update.
        Map<String, DataStream> updated = new LinkedHashMap<>();
        for (DataStreamAction action : actions) {
            String dataStreamName = action.dataStream();
            DataStream dataStream = updated.getOrDefault(dataStreamName, currentState.metadata().dataStreams().get(dataStreamName));
            if (dataStream == null) {
                throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
            }
            switch (action.type()) {
                case ADD_BACKING_INDEX:
                    dataStream = applyAddBackingIndex(metadataBuilder, dataStream, action.index());
                    break;
                case REMOVE_BACKING_INDEX:
                    dataStream = applyRemoveBackingIndex(dataStream, action.index());
                    break;
                default:
                    throw new IllegalArgumentException("unsupported data stream action type [" + action.type() + "]");
            }
            updated.put(dataStreamName, dataStream);
        }

        for (DataStream dataStream : updated.values()) {
            logger.info("updating data stream [{}]", dataStream.getName());
            metadataBuilder.put(dataStream);
        }
        validateNoSharedBackingIndices(currentState.metadata().dataStreams(), updated);
        // Metadata.build() additionally rejects a stream whose prefix matches an index with a counter above its
        // generation.
        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
    }

    /**
     * Rejects a resulting state in which any index is a backing index of more than one data stream, checking the full
     * final membership so the outcome does not depend on action order.
     */
    private static void validateNoSharedBackingIndices(Map<String, DataStream> currentStreams, Map<String, DataStream> updatedStreams) {
        Map<String, DataStream> finalStreams = new HashMap<>(currentStreams);
        finalStreams.putAll(updatedStreams);
        Map<String, String> indexToStream = new HashMap<>();
        for (DataStream dataStream : finalStreams.values()) {
            for (Index index : dataStream.getIndices()) {
                String previousOwner = indexToStream.putIfAbsent(index.getName(), dataStream.getName());
                if (previousOwner != null && previousOwner.equals(dataStream.getName()) == false) {
                    throw new IllegalArgumentException(
                        "index ["
                            + index.getName()
                            + "] cannot be a backing index of more than one data stream, but is claimed by ["
                            + previousOwner
                            + "] and ["
                            + dataStream.getName()
                            + "]"
                    );
                }
            }
        }
    }

    private static DataStream applyAddBackingIndex(Metadata.Builder metadataBuilder, DataStream dataStream, String indexName) {
        IndexMetadata indexMetadata = metadataBuilder.get(indexName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        Index index = indexMetadata.getIndex();
        if (dataStream.getIndices().contains(index)) {
            // Idempotent: already a member.
            return dataStream;
        }

        // Backing indices are hidden; mark the index hidden if it is not already, matching data stream creation and
        // rollover. put(IndexMetadata.Builder) bumps the index metadata version; the top-level Metadata version is
        // bumped by the cluster-manager service on publish.
        if (IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings()) == false) {
            IndexMetadata.Builder hiddenIndexMetadata = IndexMetadata.builder(indexMetadata)
                .settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
                .settingsVersion(indexMetadata.getSettingsVersion() + 1);
            metadataBuilder.put(hiddenIndexMetadata);
            index = hiddenIndexMetadata.build().getIndex();
        }

        // Metadata validation rejects any convention-named index above the current generation, so the added index is
        // always a non-write index: keep generation unchanged and re-sort so the write index stays last.
        List<Index> updatedIndices = new ArrayList<>(dataStream.getIndices());
        updatedIndices.add(index);
        updatedIndices.sort(Comparator.comparingLong(i -> backingIndexSortKey(dataStream.getName(), i.getName())));

        return new DataStream(dataStream.getName(), dataStream.getTimeStampField(), updatedIndices, dataStream.getGeneration());
    }

    /**
     * Orders backing indices oldest-to-newest: convention-named indices ({@code .ds-<dataStream>-NNNNNN}) sort by their
     * counter, while arbitrary-named indices have no counter and sort first, keeping the write index last.
     */
    private static long backingIndexSortKey(String dataStreamName, String indexName) {
        String expectedPrefix = DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-";
        if (indexName.startsWith(expectedPrefix)) {
            String counter = indexName.substring(expectedPrefix.length());
            if (Metadata.NUMBER_PATTERN.matcher(counter).matches()) {
                return Long.parseLong(counter);
            }
        }
        return Long.MIN_VALUE;
    }

    private static DataStream applyRemoveBackingIndex(DataStream dataStream, String indexName) {
        Index toRemove = null;
        for (Index index : dataStream.getIndices()) {
            if (index.getName().equals(indexName)) {
                toRemove = index;
                break;
            }
        }
        if (toRemove == null) {
            throw new IllegalArgumentException("index [" + indexName + "] is not part of data stream [" + dataStream.getName() + "]");
        }
        if (dataStream.getIndices().size() == 1) {
            throw new IllegalArgumentException(
                "cannot remove backing index ["
                    + indexName
                    + "] of data stream ["
                    + dataStream.getName()
                    + "] because it is the last backing index; delete the data stream instead"
            );
        }
        // The write index (last, highest-generation) cannot be removed: it would orphan the stream's generation.
        Index writeIndex = dataStream.getIndices().get(dataStream.getIndices().size() - 1);
        if (writeIndex.equals(toRemove)) {
            throw new IllegalArgumentException(
                "cannot remove backing index ["
                    + indexName
                    + "] of data stream ["
                    + dataStream.getName()
                    + "] because it is the write index"
            );
        }
        return dataStream.removeBackingIndex(toRemove);
    }

    /**
     * A cluster-state update request carrying the batch of data-stream actions to apply.
     *
     * @opensearch.internal
     */
    public static final class ModifyDataStreamsClusterStateUpdateRequest extends ClusterStateUpdateRequest {

        private final List<DataStreamAction> actions;

        public ModifyDataStreamsClusterStateUpdateRequest(
            List<DataStreamAction> actions,
            TimeValue clusterManagerNodeTimeout,
            TimeValue ackTimeout
        ) {
            this.actions = actions;
            clusterManagerNodeTimeout(clusterManagerNodeTimeout);
            ackTimeout(ackTimeout);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }
    }
}
