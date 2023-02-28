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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.internal.io.Streams;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import static org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;

/**
 * Service that can store task results.
 *
 * @opensearch.internal
 */
public class TaskResultsService {

    private static final Logger logger = LogManager.getLogger(TaskResultsService.class);

    public static final String TASK_INDEX = ".tasks";

    public static final String TASK_RESULT_INDEX_MAPPING_FILE = "task-index-mapping.json";

    public static final String TASK_RESULT_MAPPING_VERSION_META_FIELD = "version";

    public static final int TASK_RESULT_MAPPING_VERSION = 4; // must match version in task-index-mapping.json

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    static final BackoffPolicy STORE_BACKOFF_POLICY = BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

    private final Client client;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    @Inject
    public TaskResultsService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void storeResult(TaskResult taskResult, ActionListener<Void> listener) {

        ClusterState state = clusterService.state();

        if (state.routingTable().hasIndex(TASK_INDEX) == false) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(taskResultIndexSettings());
            createIndexRequest.index(TASK_INDEX);
            createIndexRequest.mapping(taskResultIndexMapping());
            createIndexRequest.cause("auto(task api)");

            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    doStoreResult(taskResult, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            doStoreResult(taskResult, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            IndexMetadata metadata = state.getMetadata().index(TASK_INDEX);
            if (getTaskResultMappingVersion(metadata) < TASK_RESULT_MAPPING_VERSION) {
                // The index already exists but doesn't have our mapping
                client.admin()
                    .indices()
                    .preparePutMapping(TASK_INDEX)
                    .setSource(taskResultIndexMapping(), XContentType.JSON)
                    .execute(ActionListener.delegateFailure(listener, (l, r) -> doStoreResult(taskResult, listener)));
            } else {
                doStoreResult(taskResult, listener);
            }
        }
    }

    private int getTaskResultMappingVersion(IndexMetadata metadata) {
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null) {
            return 0;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
        if (meta == null || meta.containsKey(TASK_RESULT_MAPPING_VERSION_META_FIELD) == false) {
            return 1; // The mapping was created before meta field was introduced
        }
        return (int) meta.get(TASK_RESULT_MAPPING_VERSION_META_FIELD);
    }

    private void doStoreResult(TaskResult taskResult, ActionListener<Void> listener) {
        IndexRequestBuilder index = client.prepareIndex(TASK_INDEX).setId(taskResult.getTask().getTaskId().toString());
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            taskResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
            index.setSource(builder);
        } catch (IOException e) {
            throw new OpenSearchException("Couldn't convert task result to XContent for [{}]", e, taskResult.getTask());
        }
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder index, ActionListener<Void> listener) {
        index.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (false == (e instanceof OpenSearchRejectedExecutionException) || false == backoff.hasNext()) {
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store task result, retrying in [{}]", wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, index, listener), wait, ThreadPool.Names.SAME);
                }
            }
        });
    }

    private Settings taskResultIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }

    public String taskResultIndexMapping() {
        try (InputStream is = getClass().getResourceAsStream(TASK_RESULT_INDEX_MAPPING_FILE)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toString(StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            logger.error(
                () -> new ParameterizedMessage("failed to create tasks results index template [{}]", TASK_RESULT_INDEX_MAPPING_FILE),
                e
            );
            throw new IllegalStateException("failed to create tasks results index template [" + TASK_RESULT_INDEX_MAPPING_FILE + "]", e);
        }

    }
}
