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

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.ActiveShardsObserver;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateRequest;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ObjectPath;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Creates a data stream of metadata
 *
 * @opensearch.internal
 */
public class MetadataCreateDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataCreateDataStreamService.class);

    private final ClusterService clusterService;
    private final ActiveShardsObserver activeShardsObserver;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final ClusterManagerTaskThrottler.ThrottlingKey createDataStreamTaskKey;

    public MetadataCreateDataStreamService(
        ThreadPool threadPool,
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        this.clusterService = clusterService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.metadataCreateIndexService = metadataCreateIndexService;
        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        createDataStreamTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.CREATE_DATA_STREAM_KEY, true);
    }

    public void createDataStream(CreateDataStreamClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> finalListener) {
        AtomicReference<String> firstBackingIndexRef = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                String firstBackingIndexName = firstBackingIndexRef.get();
                assert firstBackingIndexName != null;
                activeShardsObserver.waitForActiveShards(
                    new String[] { firstBackingIndexName },
                    ActiveShardCount.DEFAULT,
                    request.clusterManagerNodeTimeout(),
                    shardsAcked -> {
                        finalListener.onResponse(new AcknowledgedResponse(true));
                    },
                    finalListener::onFailure
                );
            } else {
                finalListener.onResponse(new AcknowledgedResponse(false));
            }
        }, finalListener::onFailure);
        clusterService.submitStateUpdateTask(
            "create-data-stream [" + request.name + "]",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.HIGH, request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = createDataStream(metadataCreateIndexService, currentState, request);
                    firstBackingIndexRef.set(clusterState.metadata().dataStreams().get(request.name).getIndices().get(0).getName());
                    return clusterState;
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return createDataStreamTaskKey;
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            }
        );
    }

    public ClusterState createDataStream(CreateDataStreamClusterStateUpdateRequest request, ClusterState current) throws Exception {
        return createDataStream(metadataCreateIndexService, current, request);
    }

    /**
     * A request to create a data stream cluster state update
     *
     * @opensearch.internal
     */
    public static final class CreateDataStreamClusterStateUpdateRequest extends ClusterStateUpdateRequest {

        private final String name;

        public CreateDataStreamClusterStateUpdateRequest(String name, TimeValue masterNodeTimeout, TimeValue timeout) {
            this.name = name;
            clusterManagerNodeTimeout(masterNodeTimeout);
            ackTimeout(timeout);
        }
    }

    static ClusterState createDataStream(
        MetadataCreateIndexService metadataCreateIndexService,
        ClusterState currentState,
        CreateDataStreamClusterStateUpdateRequest request
    ) throws Exception {

        if (currentState.metadata().dataStreams().containsKey(request.name)) {
            throw new ResourceAlreadyExistsException("data_stream [" + request.name + "] already exists");
        }

        MetadataCreateIndexService.validateIndexOrAliasName(
            request.name,
            (s1, s2) -> new IllegalArgumentException("data_stream [" + s1 + "] " + s2)
        );

        if (request.name.toLowerCase(Locale.ROOT).equals(request.name) == false) {
            throw new IllegalArgumentException("data_stream [" + request.name + "] must be lowercase");
        }
        if (request.name.startsWith(".")) {
            throw new IllegalArgumentException("data_stream [" + request.name + "] must not start with '.'");
        }

        ComposableIndexTemplate template = lookupTemplateForDataStream(request.name, currentState.metadata());

        String firstBackingIndexName = DataStream.getDefaultBackingIndexName(request.name, 1);
        CreateIndexClusterStateUpdateRequest createIndexRequest = new CreateIndexClusterStateUpdateRequest(
            "initialize_data_stream",
            firstBackingIndexName,
            firstBackingIndexName
        ).dataStreamName(request.name).settings(Settings.builder().put("index.hidden", true).build());
        try {
            currentState = metadataCreateIndexService.applyCreateIndexRequest(currentState, createIndexRequest, false);
        } catch (ResourceAlreadyExistsException e) {
            // Rethrow as OpenSearchStatusException, so that bulk transport action doesn't ignore it during
            // auto index/data stream creation.
            // (otherwise bulk execution fails later, because data stream will also not have been created)
            throw new OpenSearchStatusException(
                "data stream could not be created because backing index [{}] already exists",
                RestStatus.BAD_REQUEST,
                e,
                firstBackingIndexName
            );
        }
        IndexMetadata firstBackingIndex = currentState.metadata().index(firstBackingIndexName);
        assert firstBackingIndex != null;
        assert firstBackingIndex.mapping() != null : "no mapping found for backing index [" + firstBackingIndexName + "]";

        String fieldName = template.getDataStreamTemplate().getTimestampField().getName();
        DataStream.TimestampField timestampField = new DataStream.TimestampField(fieldName);
        DataStream newDataStream = new DataStream(request.name, timestampField, Collections.singletonList(firstBackingIndex.getIndex()));
        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(newDataStream);
        logger.info("adding data stream [{}]", request.name);
        return ClusterState.builder(currentState).metadata(builder).build();
    }

    public static ComposableIndexTemplate lookupTemplateForDataStream(String dataStreamName, Metadata metadata) {
        final String v2Template = MetadataIndexTemplateService.findV2Template(metadata, dataStreamName, false);
        if (v2Template == null) {
            throw new IllegalArgumentException("no matching index template found for data stream [" + dataStreamName + "]");
        }
        ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(v2Template);
        if (composableIndexTemplate.getDataStreamTemplate() == null) {
            throw new IllegalArgumentException(
                "matching index template [" + v2Template + "] for data stream [" + dataStreamName + "] has no data stream template"
            );
        }
        return composableIndexTemplate;
    }

    public static void validateTimestampFieldMapping(MapperService mapperService) throws IOException {
        MetadataFieldMapper fieldMapper = (MetadataFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper("_data_stream_timestamp");
        assert fieldMapper != null : "[_data_stream_timestamp] meta field mapper must exist";

        Map<String, Object> parsedTemplateMapping = MapperService.parseMapping(
            NamedXContentRegistry.EMPTY,
            mapperService.documentMapper().mappingSource().string()
        );
        Boolean enabled = ObjectPath.eval("_doc._data_stream_timestamp.enabled", parsedTemplateMapping);
        // Sanity check: if this fails then somehow the mapping for _data_stream_timestamp has been overwritten and
        // that would be a bug.
        if (enabled == null || enabled == false) {
            throw new IllegalStateException("[_data_stream_timestamp] meta field has been disabled");
        }

        // Sanity check (this validation logic should already have been executed when merging mappings):
        fieldMapper.validate(mapperService.documentMapper().mappers());
    }

}
