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

package org.opensearch.transport.client;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.opensearch.action.admin.indices.datastream.GetDataStreamAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.flush.FlushRequestBuilder;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsRequest;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsRequestBuilder;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.scale.searchonly.ScaleIndexRequestBuilder;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.opensearch.action.admin.indices.shards.IndicesShardStoreRequestBuilder;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequestBuilder;
import org.opensearch.action.admin.indices.shrink.ResizeResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.DeleteViewAction;
import org.opensearch.action.admin.indices.view.GetViewAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.common.Nullable;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#indices()
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface IndicesAdminClient extends OpenSearchClient {

    /**
     * Indices Exists.
     *
     * @param request The indices exists request
     * @return The result future
     * @see Requests#indicesExistsRequest(String...)
     */
    ActionFuture<IndicesExistsResponse> exists(IndicesExistsRequest request);

    /**
     * The status of one or more indices.
     *
     * @param request  The indices status request
     * @param listener A listener to be notified with a result
     * @see Requests#indicesExistsRequest(String...)
     */
    void exists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener);

    /**
     * Indices exists.
     */
    IndicesExistsRequestBuilder prepareExists(String... indices);

    /**
     * Indices stats.
     */
    ActionFuture<IndicesStatsResponse> stats(IndicesStatsRequest request);

    /**
     * Indices stats.
     */
    void stats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener);

    /**
     * Indices stats.
     */
    IndicesStatsRequestBuilder prepareStats(String... indices);

    /**
     * Indices recoveries
     */
    ActionFuture<RecoveryResponse> recoveries(RecoveryRequest request);

    /**
     *Indices recoveries
     */
    void recoveries(RecoveryRequest request, ActionListener<RecoveryResponse> listener);

    /**
     *Indices segment replication
     */
    ActionFuture<SegmentReplicationStatsResponse> segmentReplicationStats(SegmentReplicationStatsRequest request);

    /**
     *Indices segment replication
     */
    void segmentReplicationStats(SegmentReplicationStatsRequest request, ActionListener<SegmentReplicationStatsResponse> listener);

    /**
     * Indices recoveries
     */
    RecoveryRequestBuilder prepareRecoveries(String... indices);

    /**
     * Indices segment replication
     */
    SegmentReplicationStatsRequestBuilder prepareSegmentReplicationStats(String... indices);

    /**
     * The segments of one or more indices.
     *
     * @param request The indices segments request
     * @return The result future
     * @see Requests#indicesSegmentsRequest(String...)
     */
    ActionFuture<IndicesSegmentResponse> segments(IndicesSegmentsRequest request);

    /**
     * The segments of one or more indices.
     *
     * @param request  The indices segments request
     * @param listener A listener to be notified with a result
     * @see Requests#indicesSegmentsRequest(String...)
     */
    void segments(IndicesSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener);

    /**
     * The segments of one or more indices.
     */
    IndicesSegmentsRequestBuilder prepareSegments(String... indices);

    /**
     * The shard stores info of one or more indices.
     *
     * @param request The indices shard stores request
     * @return The result future
     * @see Requests#indicesShardStoresRequest(String...)
     */
    ActionFuture<IndicesShardStoresResponse> shardStores(IndicesShardStoresRequest request);

    /**
     * The shard stores info of one or more indices.
     *
     * @param request The indices shard stores request
     * @param listener A listener to be notified with a result
     * @see Requests#indicesShardStoresRequest(String...)
     */
    void shardStores(IndicesShardStoresRequest request, ActionListener<IndicesShardStoresResponse> listener);

    /**
     * The shard stores info of one or more indices.
     */
    IndicesShardStoreRequestBuilder prepareShardStores(String... indices);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request The create index request
     * @return The result future
     * @see Requests#createIndexRequest(String)
     */
    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request  The create index request
     * @param listener A listener to be notified with a result
     * @see Requests#createIndexRequest(String)
     */
    void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param index The index name to create
     */
    CreateIndexRequestBuilder prepareCreate(String index);

    /**
     * Deletes an index based on the index name.
     *
     * @param request The delete index request
     * @return The result future
     * @see Requests#deleteIndexRequest(String)
     */
    ActionFuture<AcknowledgedResponse> delete(DeleteIndexRequest request);

    /**
     * Deletes an index based on the index name.
     *
     * @param request  The delete index request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteIndexRequest(String)
     */
    void delete(DeleteIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes an index based on the index name.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    DeleteIndexRequestBuilder prepareDelete(String... indices);

    /**
     * Closes an index based on the index name.
     *
     * @param request The close index request
     * @return The result future
     * @see Requests#closeIndexRequest(String)
     */
    ActionFuture<CloseIndexResponse> close(CloseIndexRequest request);

    /**
     * Closes an index based on the index name.
     *
     * @param request  The close index request
     * @param listener A listener to be notified with a result
     * @see Requests#closeIndexRequest(String)
     */
    void close(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener);

    /**
     * Closes one or more indices based on their index name.
     *
     * @param indices The name of the indices to close
     */
    CloseIndexRequestBuilder prepareClose(String... indices);

    /**
     * Open an index based on the index name.
     *
     * @param request The close index request
     * @return The result future
     * @see Requests#openIndexRequest(String)
     */
    ActionFuture<OpenIndexResponse> open(OpenIndexRequest request);

    /**
     * Open an index based on the index name.
     *
     * @param request  The close index request
     * @param listener A listener to be notified with a result
     * @see Requests#openIndexRequest(String)
     */
    void open(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener);

    /**
     * Adds a block to an index
     *
     * @param block   The block to add
     * @param indices The name of the indices to add the block to
     */
    AddIndexBlockRequestBuilder prepareAddBlock(APIBlock block, String... indices);

    /**
     * Adds a block to an index
     *
     * @param request  The add index block request
     * @param listener A listener to be notified with a result
     * @see Requests#openIndexRequest(String)
     */
    void addBlock(AddIndexBlockRequest request, ActionListener<AddIndexBlockResponse> listener);

    /**
     * Opens one or more indices based on their index name.
     *
     * @param indices The name of the indices to close
     */
    OpenIndexRequestBuilder prepareOpen(String... indices);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request The refresh request
     * @return The result future
     * @see Requests#refreshRequest(String...)
     */
    ActionFuture<RefreshResponse> refresh(RefreshRequest request);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request  The refresh request
     * @param listener A listener to be notified with a result
     * @see Requests#refreshRequest(String...)
     */
    void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     */
    RefreshRequestBuilder prepareRefresh(String... indices);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request The flush request
     * @return A result future
     * @see Requests#flushRequest(String...)
     */
    ActionFuture<FlushResponse> flush(FlushRequest request);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request  The flush request
     * @param listener A listener to be notified with a result
     * @see Requests#flushRequest(String...)
     */
    void flush(FlushRequest request, ActionListener<FlushResponse> listener);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     */
    FlushRequestBuilder prepareFlush(String... indices);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     *
     * @param request The optimize request
     * @return A result future
     * @see Requests#forceMergeRequest(String...)
     */
    ActionFuture<ForceMergeResponse> forceMerge(ForceMergeRequest request);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     *
     * @param request  The force merge request
     * @param listener A listener to be notified with a result
     * @see Requests#forceMergeRequest(String...)
     */
    void forceMerge(ForceMergeRequest request, ActionListener<ForceMergeResponse> listener);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     */
    ForceMergeRequestBuilder prepareForceMerge(String... indices);

    /**
     * Explicitly upgrade one or more indices
     *
     * @param request The upgrade request
     * @return A result future
     * @see Requests#upgradeRequest(String...)
     */
    ActionFuture<UpgradeResponse> upgrade(UpgradeRequest request);

    /**
     * Explicitly upgrade one or more indices
     *
     * @param request  The upgrade request
     * @param listener A listener to be notified with a result
     * @see Requests#upgradeRequest(String...)
     */
    void upgrade(UpgradeRequest request, ActionListener<UpgradeResponse> listener);

    /**
     *  Explicitly upgrade one or more indices
     */
    UpgradeStatusRequestBuilder prepareUpgradeStatus(String... indices);

    /**
     * Check upgrade status of one or more indices
     *
     * @param request The upgrade request
     * @return A result future
     * @see Requests#upgradeRequest(String...)
     */
    ActionFuture<UpgradeStatusResponse> upgradeStatus(UpgradeStatusRequest request);

    /**
     * Check upgrade status of one or more indices
     *
     * @param request  The upgrade request
     * @param listener A listener to be notified with a result
     * @see Requests#upgradeRequest(String...)
     */
    void upgradeStatus(UpgradeStatusRequest request, ActionListener<UpgradeStatusResponse> listener);

    /**
     * Check upgrade status of one or more indices
     */
    UpgradeRequestBuilder prepareUpgrade(String... indices);

    /**
     * Get the complete mappings of one or more types
     */
    void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener);

    /**
     * Get the complete mappings of one or more types
     */
    ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request);

    /**
     * Get the complete mappings of one or more types
     */
    GetMappingsRequestBuilder prepareGetMappings(String... indices);

    /**
     * Get the mappings of specific fields
     */
    void getFieldMappings(GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener);

    /**
     * Get the mappings of specific fields
     */
    GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices);

    /**
     * Get the mappings of specific fields
     */
    ActionFuture<GetFieldMappingsResponse> getFieldMappings(GetFieldMappingsRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request The create mapping request
     * @return A result future
     * @see Requests#putMappingRequest(String...)
     */
    ActionFuture<AcknowledgedResponse> putMapping(PutMappingRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request  The create mapping request
     * @param listener A listener to be notified with a result
     * @see Requests#putMappingRequest(String...)
     */
    void putMapping(PutMappingRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     */
    PutMappingRequestBuilder preparePutMapping(String... indices);

    /**
     * Allows to add/remove aliases from indices.
     *
     * @param request The index aliases request
     * @return The result future
     * @see Requests#indexAliasesRequest()
     */
    ActionFuture<AcknowledgedResponse> aliases(IndicesAliasesRequest request);

    /**
     * Allows to add/remove aliases from indices.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     * @see Requests#indexAliasesRequest()
     */
    void aliases(IndicesAliasesRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Allows to add/remove aliases from indices.
     */
    IndicesAliasesRequestBuilder prepareAliases();

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     *
     * @param request The result future
     */
    ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request);

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     */
    void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener);

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     */
    GetAliasesRequestBuilder prepareGetAliases(String... aliases);

    /**
     * Get index metadata for particular indices.
     *
     * @param request The result future
     */
    ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request);

    /**
     * Get index metadata for particular indices.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     */
    void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener);

    /**
     * Get index metadata for particular indices.
     */
    GetIndexRequestBuilder prepareGetIndex();

    /**
     * Clear indices cache.
     *
     * @param request The clear indices cache request
     * @return The result future
     * @see Requests#clearIndicesCacheRequest(String...)
     */
    ActionFuture<ClearIndicesCacheResponse> clearCache(ClearIndicesCacheRequest request);

    /**
     * Clear indices cache.
     *
     * @param request  The clear indices cache request
     * @param listener A listener to be notified with a result
     * @see Requests#clearIndicesCacheRequest(String...)
     */
    void clearCache(ClearIndicesCacheRequest request, ActionListener<ClearIndicesCacheResponse> listener);

    /**
     * Clear indices cache.
     */
    ClearIndicesCacheRequestBuilder prepareClearCache(String... indices);

    /**
     * Updates settings of one or more indices.
     *
     * @param request the update settings request
     * @return The result future
     */
    ActionFuture<AcknowledgedResponse> updateSettings(UpdateSettingsRequest request);

    /**
     * Updates settings of one or more indices.
     *
     * @param request  the update settings request
     * @param listener A listener to be notified with the response
     */
    void updateSettings(UpdateSettingsRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Update indices settings.
     */
    UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices);

    /**
     * Analyze text under the provided index.
     */
    ActionFuture<AnalyzeAction.Response> analyze(AnalyzeAction.Request request);

    /**
     * Analyze text under the provided index.
     */
    void analyze(AnalyzeAction.Request request, ActionListener<AnalyzeAction.Response> listener);

    /**
     * Analyze text under the provided index.
     *
     * @param index The index name
     * @param text  The text to analyze
     */
    AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text);

    /**
     * Analyze text.
     *
     * @param text The text to analyze
     */
    AnalyzeRequestBuilder prepareAnalyze(String text);

    /**
     * Analyze text/texts.
     *
     */
    AnalyzeRequestBuilder prepareAnalyze();

    /**
     * Puts an index template.
     */
    ActionFuture<AcknowledgedResponse> putTemplate(PutIndexTemplateRequest request);

    /**
     * Puts an index template.
     */
    void putTemplate(PutIndexTemplateRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Puts an index template.
     *
     * @param name The name of the template.
     */
    PutIndexTemplateRequestBuilder preparePutTemplate(String name);

    /**
     * Deletes index template.
     */
    ActionFuture<AcknowledgedResponse> deleteTemplate(DeleteIndexTemplateRequest request);

    /**
     * Deletes an index template.
     */
    void deleteTemplate(DeleteIndexTemplateRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes an index template.
     *
     * @param name The name of the template.
     */
    DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name);

    /**
     * Gets index template.
     */
    ActionFuture<GetIndexTemplatesResponse> getTemplates(GetIndexTemplatesRequest request);

    /**
     * Gets an index template.
     */
    void getTemplates(GetIndexTemplatesRequest request, ActionListener<GetIndexTemplatesResponse> listener);

    /**
     * Gets an index template (optional).
     */
    GetIndexTemplatesRequestBuilder prepareGetTemplates(String... name);

    /**
     * Validate a query for correctness.
     *
     * @param request The count request
     * @return The result future
     */
    ActionFuture<ValidateQueryResponse> validateQuery(ValidateQueryRequest request);

    /**
     * Validate a query for correctness.
     *
     * @param request  The count request
     * @param listener A listener to be notified of the result
     */
    void validateQuery(ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener);

    /**
     * Validate a query for correctness.
     */
    ValidateQueryRequestBuilder prepareValidateQuery(String... indices);

    /**
     * Executed a per index settings get request and returns the settings for the indices specified.
     * Note: this is a per index request and will not include settings that are set on the cluster
     * level. This request is not exhaustive, it will not return default values for setting.
     */
    void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener);

    /**
     * Executed a per index settings get request.
     * @see #getSettings(GetSettingsRequest)
     */
    ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request);

    /**
     * Returns a builder for a per index settings get request.
     * @param indices the indices to fetch the setting for.
     * @see #getSettings(GetSettingsRequest)
     */
    GetSettingsRequestBuilder prepareGetSettings(String... indices);

    /**
     * Resize an index using an explicit request allowing to specify the settings, mappings and aliases of the target index of the index.
     */
    ResizeRequestBuilder prepareResizeIndex(String sourceIndex, String targetIndex);

    /**
     * Resize an index using an explicit request allowing to specify the settings, mappings and aliases of the target index of the index.
     */
    ActionFuture<ResizeResponse> resizeIndex(ResizeRequest request);

    /**
     * Shrinks an index using an explicit request allowing to specify the settings, mappings and aliases of the target index of the index.
     */
    void resizeIndex(ResizeRequest request, ActionListener<ResizeResponse> listener);

    /**
     * Swaps the index pointed to by an alias given all provided conditions are satisfied
     */
    RolloverRequestBuilder prepareRolloverIndex(String sourceAlias);

    /**
     * Swaps the index pointed to by an alias given all provided conditions are satisfied
     */
    ActionFuture<RolloverResponse> rolloverIndex(RolloverRequest request);

    /**
     * Swaps the index pointed to by an alias given all provided conditions are satisfied
     */
    void rolloverIndex(RolloverRequest request, ActionListener<RolloverResponse> listener);

    /**
     * Store a data stream
     */
    void createDataStream(CreateDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Store a data stream
     */
    ActionFuture<AcknowledgedResponse> createDataStream(CreateDataStreamAction.Request request);

    /**
     * Delete a data stream
     */
    void deleteDataStream(DeleteDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a data stream
     */
    ActionFuture<AcknowledgedResponse> deleteDataStream(DeleteDataStreamAction.Request request);

    /**
     * Get data streams
     */
    void getDataStreams(GetDataStreamAction.Request request, ActionListener<GetDataStreamAction.Response> listener);

    /**
     * Get data streams
     */
    ActionFuture<GetDataStreamAction.Response> getDataStreams(GetDataStreamAction.Request request);

    /**
     * Resolves names and wildcard expressions to indices, aliases, and data streams
     */
    void resolveIndex(ResolveIndexAction.Request request, ActionListener<ResolveIndexAction.Response> listener);

    /**
     * Resolves names and wildcard expressions to indices, aliases, and data streams
     */
    ActionFuture<ResolveIndexAction.Response> resolveIndex(ResolveIndexAction.Request request);

    /** Create a view */
    void createView(CreateViewAction.Request request, ActionListener<GetViewAction.Response> listener);

    /** Create a view */
    ActionFuture<GetViewAction.Response> createView(CreateViewAction.Request request);

    /** Get the details of a view */
    void getView(GetViewAction.Request request, ActionListener<GetViewAction.Response> listener);

    /** Get the details of a view */
    ActionFuture<GetViewAction.Response> getView(GetViewAction.Request request);

    /** Delete a view */
    void deleteView(DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener);

    /** Delete a view */
    ActionFuture<AcknowledgedResponse> deleteView(DeleteViewAction.Request request);

    /** Update a view */
    void updateView(CreateViewAction.Request request, ActionListener<GetViewAction.Response> listener);

    /** Update a view */
    ActionFuture<GetViewAction.Response> updateView(CreateViewAction.Request request);

    /** Pause ingestion */
    ActionFuture<PauseIngestionResponse> pauseIngestion(PauseIngestionRequest request);

    /** Pause ingestion */
    void pauseIngestion(PauseIngestionRequest request, ActionListener<PauseIngestionResponse> listener);

    /** Resume ingestion */
    ActionFuture<ResumeIngestionResponse> resumeIngestion(ResumeIngestionRequest request);

    /** Resume ingestion */
    void resumeIngestion(ResumeIngestionRequest request, ActionListener<ResumeIngestionResponse> listener);

    /** Get ingestion state */
    ActionFuture<GetIngestionStateResponse> getIngestionState(GetIngestionStateRequest request);

    /** Get ingestion state */
    void getIngestionState(GetIngestionStateRequest request, ActionListener<GetIngestionStateResponse> listener);

    /**
     * Prepares a request to scale an index between normal and search-only modes.
     *
     * @param index      The name of the index to scale
     * @param searchOnly Whether to scale to search-only mode (true) or back to normal mode (false)
     * @return The request builder configured with the specified scaling direction
     */
    ScaleIndexRequestBuilder prepareScaleSearchOnly(String index, boolean searchOnly);

    /** Indices Exists - CompletionStage version */
    default CompletionStage<IndicesExistsResponse> existsAsync(IndicesExistsRequest request) {
        CompletableFuture<IndicesExistsResponse> future = new CompletableFuture<>();
        exists(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Indices stats - CompletionStage version */
    default CompletionStage<IndicesStatsResponse> statsAsync(IndicesStatsRequest request) {
        CompletableFuture<IndicesStatsResponse> future = new CompletableFuture<>();
        stats(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Recoveries - CompletionStage version */
    default CompletionStage<RecoveryResponse> recoveriesAsync(RecoveryRequest request) {
        CompletableFuture<RecoveryResponse> future = new CompletableFuture<>();
        recoveries(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Segment replication stats - CompletionStage version */
    default CompletionStage<SegmentReplicationStatsResponse> segmentReplicationStatsAsync(SegmentReplicationStatsRequest request) {
        CompletableFuture<SegmentReplicationStatsResponse> future = new CompletableFuture<>();
        segmentReplicationStats(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Segments - CompletionStage version */
    default CompletionStage<IndicesSegmentResponse> segmentsAsync(IndicesSegmentsRequest request) {
        CompletableFuture<IndicesSegmentResponse> future = new CompletableFuture<>();
        segments(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Shard stores - CompletionStage version */
    default CompletionStage<IndicesShardStoresResponse> shardStoresAsync(IndicesShardStoresRequest request) {
        CompletableFuture<IndicesShardStoresResponse> future = new CompletableFuture<>();
        shardStores(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Create index - CompletionStage version */
    default CompletionStage<CreateIndexResponse> createAsync(CreateIndexRequest request) {
        CompletableFuture<CreateIndexResponse> future = new CompletableFuture<>();
        create(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Delete index - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> deleteAsync(DeleteIndexRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        delete(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Close index - CompletionStage version */
    default CompletionStage<CloseIndexResponse> closeAsync(CloseIndexRequest request) {
        CompletableFuture<CloseIndexResponse> future = new CompletableFuture<>();
        close(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Open index - CompletionStage version */
    default CompletionStage<OpenIndexResponse> openAsync(OpenIndexRequest request) {
        CompletableFuture<OpenIndexResponse> future = new CompletableFuture<>();
        open(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Add index block - CompletionStage version */
    default CompletionStage<AddIndexBlockResponse> addBlockAsync(AddIndexBlockRequest request) {
        CompletableFuture<AddIndexBlockResponse> future = new CompletableFuture<>();
        addBlock(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Refresh - CompletionStage version */
    default CompletionStage<RefreshResponse> refreshAsync(RefreshRequest request) {
        CompletableFuture<RefreshResponse> future = new CompletableFuture<>();
        refresh(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Flush - CompletionStage version */
    default CompletionStage<FlushResponse> flushAsync(FlushRequest request) {
        CompletableFuture<FlushResponse> future = new CompletableFuture<>();
        flush(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Force-merge - CompletionStage version */
    default CompletionStage<ForceMergeResponse> forceMergeAsync(ForceMergeRequest request) {
        CompletableFuture<ForceMergeResponse> future = new CompletableFuture<>();
        forceMerge(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Upgrade - CompletionStage version */
    default CompletionStage<UpgradeResponse> upgradeAsync(UpgradeRequest request) {
        CompletableFuture<UpgradeResponse> future = new CompletableFuture<>();
        upgrade(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Upgrade status - CompletionStage version */
    default CompletionStage<UpgradeStatusResponse> upgradeStatusAsync(UpgradeStatusRequest request) {
        CompletableFuture<UpgradeStatusResponse> future = new CompletableFuture<>();
        upgradeStatus(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get mappings - CompletionStage version */
    default CompletionStage<GetMappingsResponse> getMappingsAsync(GetMappingsRequest request) {
        CompletableFuture<GetMappingsResponse> future = new CompletableFuture<>();
        getMappings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get field mappings - CompletionStage version */
    default CompletionStage<GetFieldMappingsResponse> getFieldMappingsAsync(GetFieldMappingsRequest request) {
        CompletableFuture<GetFieldMappingsResponse> future = new CompletableFuture<>();
        getFieldMappings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Put mapping - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> putMappingAsync(PutMappingRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        putMapping(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Aliases - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> aliasesAsync(IndicesAliasesRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        aliases(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get aliases - CompletionStage version */
    default CompletionStage<GetAliasesResponse> getAliasesAsync(GetAliasesRequest request) {
        CompletableFuture<GetAliasesResponse> future = new CompletableFuture<>();
        getAliases(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get index - CompletionStage version */
    default CompletionStage<GetIndexResponse> getIndexAsync(GetIndexRequest request) {
        CompletableFuture<GetIndexResponse> future = new CompletableFuture<>();
        getIndex(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Clear cache - CompletionStage version */
    default CompletionStage<ClearIndicesCacheResponse> clearCacheAsync(ClearIndicesCacheRequest request) {
        CompletableFuture<ClearIndicesCacheResponse> future = new CompletableFuture<>();
        clearCache(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Update settings - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> updateSettingsAsync(UpdateSettingsRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        updateSettings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get settings - CompletionStage version */
    default CompletionStage<GetSettingsResponse> getSettingsAsync(GetSettingsRequest request) {
        CompletableFuture<GetSettingsResponse> future = new CompletableFuture<>();
        getSettings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Analyze - CompletionStage version */
    default CompletionStage<AnalyzeAction.Response> analyzeAsync(AnalyzeAction.Request request) {
        CompletableFuture<AnalyzeAction.Response> future = new CompletableFuture<>();
        analyze(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Put template - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> putTemplateAsync(PutIndexTemplateRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        putTemplate(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Delete template - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> deleteTemplateAsync(DeleteIndexTemplateRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        deleteTemplate(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get templates - CompletionStage version */
    default CompletionStage<GetIndexTemplatesResponse> getTemplatesAsync(GetIndexTemplatesRequest request) {
        CompletableFuture<GetIndexTemplatesResponse> future = new CompletableFuture<>();
        getTemplates(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Validate query - CompletionStage version */
    default CompletionStage<ValidateQueryResponse> validateQueryAsync(ValidateQueryRequest request) {
        CompletableFuture<ValidateQueryResponse> future = new CompletableFuture<>();
        validateQuery(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Resize index - CompletionStage version */
    default CompletionStage<ResizeResponse> resizeIndexAsync(ResizeRequest request) {
        CompletableFuture<ResizeResponse> future = new CompletableFuture<>();
        resizeIndex(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Rollover index - CompletionStage version */
    default CompletionStage<RolloverResponse> rolloverIndexAsync(RolloverRequest request) {
        CompletableFuture<RolloverResponse> future = new CompletableFuture<>();
        rolloverIndex(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Create data stream - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> createDataStreamAsync(CreateDataStreamAction.Request request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        createDataStream(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Delete data stream - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> deleteDataStreamAsync(DeleteDataStreamAction.Request request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        deleteDataStream(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get data streams - CompletionStage version */
    default CompletionStage<GetDataStreamAction.Response> getDataStreamsAsync(GetDataStreamAction.Request request) {
        CompletableFuture<GetDataStreamAction.Response> future = new CompletableFuture<>();
        getDataStreams(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Resolve index - CompletionStage version */
    default CompletionStage<ResolveIndexAction.Response> resolveIndexAsync(ResolveIndexAction.Request request) {
        CompletableFuture<ResolveIndexAction.Response> future = new CompletableFuture<>();
        resolveIndex(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Create view - CompletionStage version */
    default CompletionStage<GetViewAction.Response> createViewAsync(CreateViewAction.Request request) {
        CompletableFuture<GetViewAction.Response> future = new CompletableFuture<>();
        createView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get view - CompletionStage version */
    default CompletionStage<GetViewAction.Response> getViewAsync(GetViewAction.Request request) {
        CompletableFuture<GetViewAction.Response> future = new CompletableFuture<>();
        getView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Delete view - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> deleteViewAsync(DeleteViewAction.Request request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        deleteView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Update view - CompletionStage version */
    default CompletionStage<GetViewAction.Response> updateViewAsync(CreateViewAction.Request request) {
        CompletableFuture<GetViewAction.Response> future = new CompletableFuture<>();
        updateView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Pause ingestion - CompletionStage version */
    default CompletionStage<PauseIngestionResponse> pauseIngestionAsync(PauseIngestionRequest request) {
        CompletableFuture<PauseIngestionResponse> future = new CompletableFuture<>();
        pauseIngestion(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Resume ingestion - CompletionStage version */
    default CompletionStage<ResumeIngestionResponse> resumeIngestionAsync(ResumeIngestionRequest request) {
        CompletableFuture<ResumeIngestionResponse> future = new CompletableFuture<>();
        resumeIngestion(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get ingestion state - CompletionStage version */
    default CompletionStage<GetIngestionStateResponse> getIngestionStateAsync(GetIngestionStateRequest request) {
        CompletableFuture<GetIngestionStateResponse> future = new CompletableFuture<>();
        getIngestionState(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

}
