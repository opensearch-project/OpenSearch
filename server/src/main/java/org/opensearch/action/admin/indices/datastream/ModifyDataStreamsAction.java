/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateDataStreamService;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.opensearch.cluster.service.ClusterManagerTask.MODIFY_DATA_STREAM;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Transport action for modifying the backing indices of one or more data streams. The supported actions are adding and
 * removing backing indices. Multiple actions are applied atomically in a single cluster state update.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class ModifyDataStreamsAction extends ActionType<AcknowledgedResponse> {

    public static final ModifyDataStreamsAction INSTANCE = new ModifyDataStreamsAction();
    public static final String NAME = "indices:admin/data_stream/modify";

    private ModifyDataStreamsAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request for modifying one or more data streams.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.8.0")
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        private final List<DataStreamAction> actions;

        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(
            false,
            true,
            false,
            true,
            false,
            false,
            true,
            false
        );

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_actions",
            args -> new Request((List<DataStreamAction>) args[0])
        );
        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> DataStreamAction.fromXContent(p), new ParseField("actions"));
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public Request(List<DataStreamAction> actions) {
            this.actions = Collections.unmodifiableList(actions);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (actions.isEmpty()) {
                return addValidationError("must specify at least one data stream modification action", null);
            }
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.actions = Collections.unmodifiableList(in.readList(DataStreamAction::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(actions);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }

        @Override
        public String[] indices() {
            // resolve to the backing indices being added/removed (not the data streams). This allows the API to repair a
            // broken data stream definition by referencing indices that may no longer be members of any data stream, and
            // a DataStreamAction does not support wildcards.
            return actions.stream().map(DataStreamAction::getIndex).toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("actions");
            for (DataStreamAction action : actions) {
                action.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(actions, request.actions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actions);
        }
    }

    /**
     * Transport action for modifying data streams.
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, AcknowledgedResponse> {

        private final IndicesService indicesService;
        private final ClusterManagerTaskThrottler.ThrottlingKey modifyDataStreamTaskKey;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.indicesService = indicesService;
            // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
            modifyDataStreamTaskKey = clusterService.registerClusterManagerTask(MODIFY_DATA_STREAM, true);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse read(StreamInput in) throws IOException {
            return new AcknowledgedResponse(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
            clusterService.submitStateUpdateTask("update-backing-indices", new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return request.clusterManagerNodeTimeout();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return modifyDataStreamTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws IOException {
                    return modifyDataStream(currentState, request.getActions(), indicesService::createIndexMapperService);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

    /**
     * Applies the given data stream modification actions to the cluster state. All actions are applied to a single
     * {@link Metadata.Builder} so that the request is processed atomically: if any action fails, the whole request
     * fails and no partial changes are published.
     *
     * @param mapperServiceFactory builds a throwaway {@link MapperService} for a candidate index so that the
     *                             {@code _data_stream_timestamp} meta field can be merged into its mapping when the
     *                             index is added as a backing index.
     */
    static ClusterState modifyDataStream(
        ClusterState currentState,
        Iterable<DataStreamAction> actions,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) throws IOException {
        Metadata.Builder updatedMetadata = Metadata.builder(currentState.metadata());
        for (DataStreamAction action : actions) {
            switch (action.getType()) {
                case ADD_BACKING_INDEX:
                    addBackingIndex(updatedMetadata, action.getDataStream(), action.getIndex(), mapperServiceFactory);
                    break;
                case REMOVE_BACKING_INDEX:
                    removeBackingIndex(updatedMetadata, action.getDataStream(), action.getIndex());
                    break;
                default:
                    throw new IllegalStateException("unsupported data stream action type [" + action.getType() + "]");
            }
        }
        return ClusterState.builder(currentState).metadata(updatedMetadata).build();
    }

    private static void addBackingIndex(
        Metadata.Builder builder,
        String dataStreamName,
        String indexName,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) throws IOException {
        final DataStream dataStream = validateDataStream(builder, dataStreamName);
        final IndexMetadata index = validateIndex(builder, indexName);

        // an index that is already a backing index of this data stream cannot be added again
        if (dataStream.getIndices().contains(index.getIndex())) {
            throw new IllegalArgumentException(
                "index [" + indexName + "] is already a backing index of data stream [" + dataStreamName + "] and cannot be added"
            );
        }
        // an index that already belongs to a different data stream cannot be added, as that would leave the index
        // referenced by two data streams and corrupt the cluster state's indices lookup
        for (DataStream other : builder.dataStreams().values()) {
            if (other.getName().equals(dataStreamName) == false && other.getIndices().contains(index.getIndex())) {
                throw new IllegalArgumentException(
                    "index ["
                        + indexName
                        + "] is already a backing index of data stream ["
                        + other.getName()
                        + "] and cannot be added to data stream ["
                        + dataStreamName
                        + "]"
                );
            }
        }
        // an index with aliases cannot become a backing index, as aliases and data streams cannot point at the same index
        if (index.getAliases().isEmpty() == false) {
            throw new IllegalArgumentException(
                "cannot add index ["
                    + indexName
                    + "] to data stream ["
                    + dataStreamName
                    + "] because it has aliases "
                    + index.getAliases().keySet()
            );
        }

        // auto-adapt the index so it is a valid backing index: hide it and merge in the _data_stream_timestamp meta field,
        // then validate the timestamp mapping (mirrors how a backing index is created from a data stream template)
        final IndexMetadata updatedIndex = prepareBackingIndex(index, dataStream.getTimeStampField().getName(), mapperServiceFactory);
        builder.put(updatedIndex, true);
        builder.put(dataStream.addBackingIndex(updatedIndex.getIndex()));
    }

    /**
     * Returns an updated copy of {@code index} that is a valid backing index for a data stream: it is hidden and its
     * mapping contains an enabled {@code _data_stream_timestamp} meta field for the given timestamp field.
     */
    private static IndexMetadata prepareBackingIndex(
        IndexMetadata index,
        String timestampFieldName,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) throws IOException {
        final IndexMetadata hiddenIndex = IndexMetadata.builder(index)
            .settings(Settings.builder().put(index.getSettings()).put(SETTING_INDEX_HIDDEN, true))
            .build();

        final MapperService mapperService = mapperServiceFactory.apply(hiddenIndex);
        try {
            if (hiddenIndex.mapping() != null) {
                mapperService.merge(hiddenIndex, MapperService.MergeReason.MAPPING_RECOVERY);
            }
            mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField(timestampFieldName))
                    .getDataStreamMappingSnippet(),
                MapperService.MergeReason.MAPPING_UPDATE
            );
            MetadataCreateDataStreamService.validateTimestampFieldMapping(mapperService);

            final IndexMetadata.Builder updated = IndexMetadata.builder(hiddenIndex);

            // Only bump the settings version if hiding the index actually changed its settings; re-adding an index that was
            // already hidden leaves the settings unchanged and the version must not move (IndexService.updateMetadata asserts this).
            if (index.getSettings().equals(hiddenIndex.getSettings()) == false) {
                updated.settingsVersion(1 + hiddenIndex.getSettingsVersion());
            }

            // Likewise, only update the mapping (and bump the mapping version) if merging the _data_stream_timestamp meta field
            // actually changed the mapping; re-adding an index that was already a backing index leaves the mapping unchanged
            // (MapperService.updateMapping asserts that a mapping version bump implies a mapping change).
            final MappingMetadata mergedMapping = new MappingMetadata(mapperService.documentMapper().mappingSource());
            final MappingMetadata currentMapping = hiddenIndex.mapping();
            if (currentMapping == null || currentMapping.source().equals(mergedMapping.source()) == false) {
                updated.putMapping(mergedMapping).mappingVersion(1 + hiddenIndex.getMappingVersion());
            }

            return updated.build();
        } finally {
            IOUtils.close(mapperService);
        }
    }

    private static void removeBackingIndex(Metadata.Builder builder, String dataStreamName, String indexName) {
        final DataStream dataStream = validateDataStream(builder, dataStreamName);
        final IndexMetadata index = validateIndex(builder, indexName);

        builder.put(dataStream.removeBackingIndex(index.getIndex()));
        // un-hide the index that was removed from the data stream so that it becomes a regular standalone index again
        if (INDEX_HIDDEN_SETTING.get(index.getSettings())) {
            builder.put(
                IndexMetadata.builder(index)
                    .settings(Settings.builder().put(index.getSettings()).put(SETTING_INDEX_HIDDEN, false))
                    .settingsVersion(1 + index.getSettingsVersion())
                    .build(),
                true
            );
        }
    }

    private static DataStream validateDataStream(Metadata.Builder builder, String dataStreamName) {
        DataStream dataStream = builder.dataStream(dataStreamName);
        if (dataStream == null) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return dataStream;
    }

    private static IndexMetadata validateIndex(Metadata.Builder builder, String indexName) {
        IndexMetadata index = builder.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return index;
    }
}
