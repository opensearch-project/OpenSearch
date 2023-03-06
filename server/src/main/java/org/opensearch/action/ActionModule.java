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

package org.opensearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.opensearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.get.TransportGetDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.TransportDeleteDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.TransportDecommissionAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.opensearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.opensearch.action.admin.cluster.node.info.NodesInfoAction;
import org.opensearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.opensearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.opensearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.opensearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageAction;
import org.opensearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.opensearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreAction;
import org.opensearch.action.admin.cluster.remotestore.restore.TransportRestoreRemoteStoreAction;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.opensearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.opensearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.opensearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.opensearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.TransportDeleteWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.TransportGetWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterAddWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.TransportAddWeightedRoutingAction;
import org.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.opensearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.opensearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.TransportClusterStateAction;
import org.opensearch.action.admin.cluster.stats.ClusterStatsAction;
import org.opensearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.opensearch.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.opensearch.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.opensearch.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.opensearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.opensearch.action.admin.indices.alias.get.GetAliasesAction;
import org.opensearch.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.opensearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.opensearch.action.admin.indices.close.CloseIndexAction;
import org.opensearch.action.admin.indices.close.TransportCloseIndexAction;
import org.opensearch.action.admin.indices.create.AutoCreateAction;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.TransportCreateIndexAction;
import org.opensearch.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.opensearch.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.datastream.DataStreamsStatsAction;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.opensearch.action.admin.indices.datastream.GetDataStreamAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.opensearch.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.opensearch.action.admin.indices.flush.FlushAction;
import org.opensearch.action.admin.indices.flush.TransportFlushAction;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.opensearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.opensearch.action.admin.indices.get.GetIndexAction;
import org.opensearch.action.admin.indices.get.TransportGetIndexAction;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.opensearch.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.opensearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.opensearch.action.admin.indices.open.OpenIndexAction;
import org.opensearch.action.admin.indices.open.TransportOpenIndexAction;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockAction;
import org.opensearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.opensearch.action.admin.indices.recovery.RecoveryAction;
import org.opensearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.opensearch.action.admin.indices.refresh.RefreshAction;
import org.opensearch.action.admin.indices.refresh.TransportRefreshAction;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.admin.indices.rollover.RolloverAction;
import org.opensearch.action.admin.indices.rollover.TransportRolloverAction;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsAction;
import org.opensearch.action.admin.indices.replication.TransportSegmentReplicationStatsAction;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.opensearch.action.admin.indices.segments.PitSegmentsAction;
import org.opensearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.opensearch.action.admin.indices.segments.TransportPitSegmentsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.opensearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.opensearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.admin.indices.shrink.TransportResizeAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.opensearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.opensearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.opensearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.opensearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.opensearch.action.admin.indices.template.get.TransportGetComponentTemplateAction;
import org.opensearch.action.admin.indices.template.get.TransportGetComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.opensearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.opensearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.opensearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.opensearch.action.admin.indices.template.post.TransportSimulateTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.opensearch.action.admin.indices.upgrade.get.TransportUpgradeStatusAction;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.opensearch.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.opensearch.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.opensearch.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.TransportDeleteAction;
import org.opensearch.action.explain.ExplainAction;
import org.opensearch.action.explain.TransportExplainAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesAction;
import org.opensearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.opensearch.action.fieldcaps.TransportFieldCapabilitiesIndexAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.TransportGetAction;
import org.opensearch.action.get.TransportMultiGetAction;
import org.opensearch.action.get.TransportShardMultiGetAction;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.TransportIndexAction;
import org.opensearch.action.ingest.DeletePipelineAction;
import org.opensearch.action.ingest.DeletePipelineTransportAction;
import org.opensearch.action.ingest.GetPipelineAction;
import org.opensearch.action.ingest.GetPipelineTransportAction;
import org.opensearch.action.ingest.PutPipelineAction;
import org.opensearch.action.ingest.PutPipelineTransportAction;
import org.opensearch.action.ingest.SimulatePipelineAction;
import org.opensearch.action.ingest.SimulatePipelineTransportAction;
import org.opensearch.action.main.MainAction;
import org.opensearch.action.main.TransportMainAction;
import org.opensearch.action.search.ClearScrollAction;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.TransportClearScrollAction;
import org.opensearch.action.search.TransportCreatePitAction;
import org.opensearch.action.search.TransportDeletePitAction;
import org.opensearch.action.search.TransportGetAllPitsAction;
import org.opensearch.action.search.TransportMultiSearchAction;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.search.TransportSearchScrollAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.termvectors.MultiTermVectorsAction;
import org.opensearch.action.termvectors.TermVectorsAction;
import org.opensearch.action.termvectors.TransportMultiTermVectorsAction;
import org.opensearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.opensearch.action.termvectors.TransportTermVectorsAction;
import org.opensearch.action.update.TransportUpdateAction;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.NamedRegistry;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.multibindings.MapBinder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.extensions.action.ExtensionProxyAction;
import org.opensearch.extensions.action.ExtensionTransportAction;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.seqno.RetentionLeaseActions;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.persistent.CompletionPersistentTaskAction;
import org.opensearch.persistent.RemovePersistentTaskAction;
import org.opensearch.persistent.StartPersistentTaskAction;
import org.opensearch.persistent.UpdatePersistentTaskStatusAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.rest.action.RestFieldCapabilitiesAction;
import org.opensearch.rest.action.RestMainAction;
import org.opensearch.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.opensearch.rest.action.admin.cluster.RestCancelTasksAction;
import org.opensearch.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
import org.opensearch.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestClusterAllocationExplainAction;
import org.opensearch.rest.action.admin.cluster.RestClusterDeleteWeightedRoutingAction;
import org.opensearch.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.opensearch.rest.action.admin.cluster.RestClusterGetWeightedRoutingAction;
import org.opensearch.rest.action.admin.cluster.RestClusterHealthAction;
import org.opensearch.rest.action.admin.cluster.RestClusterPutWeightedRoutingAction;
import org.opensearch.rest.action.admin.cluster.RestClusterRerouteAction;
import org.opensearch.rest.action.admin.cluster.RestClusterSearchShardsAction;
import org.opensearch.rest.action.admin.cluster.RestClusterStateAction;
import org.opensearch.rest.action.admin.cluster.RestClusterStatsAction;
import org.opensearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.opensearch.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteDecommissionStateAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.opensearch.rest.action.admin.cluster.RestGetDecommissionStateAction;
import org.opensearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.opensearch.rest.action.admin.cluster.RestGetScriptContextAction;
import org.opensearch.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.opensearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.opensearch.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.opensearch.rest.action.admin.cluster.RestGetTaskAction;
import org.opensearch.rest.action.admin.cluster.RestListTasksAction;
import org.opensearch.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.opensearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.opensearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.opensearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.opensearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.opensearch.rest.action.admin.cluster.RestDecommissionAction;
import org.opensearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.opensearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.opensearch.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.opensearch.rest.action.admin.cluster.RestRestoreRemoteStoreAction;
import org.opensearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.opensearch.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.opensearch.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.opensearch.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.opensearch.rest.action.admin.cluster.dangling.RestListDanglingIndicesAction;
import org.opensearch.rest.action.admin.indices.RestAddIndexBlockAction;
import org.opensearch.rest.action.admin.indices.RestAnalyzeAction;
import org.opensearch.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.opensearch.rest.action.admin.indices.RestCloseIndexAction;
import org.opensearch.rest.action.admin.indices.RestCreateDataStreamAction;
import org.opensearch.rest.action.admin.indices.RestCreateIndexAction;
import org.opensearch.rest.action.admin.indices.RestDataStreamsStatsAction;
import org.opensearch.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.opensearch.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestDeleteDataStreamAction;
import org.opensearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.opensearch.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestFlushAction;
import org.opensearch.rest.action.admin.indices.RestForceMergeAction;
import org.opensearch.rest.action.admin.indices.RestGetAliasesAction;
import org.opensearch.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.opensearch.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestGetDataStreamsAction;
import org.opensearch.rest.action.admin.indices.RestGetFieldMappingAction;
import org.opensearch.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestGetIndicesAction;
import org.opensearch.rest.action.admin.indices.RestGetMappingAction;
import org.opensearch.rest.action.admin.indices.RestGetSettingsAction;
import org.opensearch.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.opensearch.rest.action.admin.indices.RestIndexPutAliasAction;
import org.opensearch.rest.action.admin.indices.RestIndicesAliasesAction;
import org.opensearch.rest.action.admin.indices.RestIndicesSegmentsAction;
import org.opensearch.rest.action.admin.indices.RestIndicesShardStoresAction;
import org.opensearch.rest.action.admin.indices.RestIndicesStatsAction;
import org.opensearch.rest.action.admin.indices.RestOpenIndexAction;
import org.opensearch.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.opensearch.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestPutMappingAction;
import org.opensearch.rest.action.admin.indices.RestRecoveryAction;
import org.opensearch.rest.action.admin.indices.RestRefreshAction;
import org.opensearch.rest.action.admin.indices.RestResizeHandler;
import org.opensearch.rest.action.admin.indices.RestResolveIndexAction;
import org.opensearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.opensearch.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.opensearch.rest.action.admin.indices.RestSimulateTemplateAction;
import org.opensearch.rest.action.admin.indices.RestSyncedFlushAction;
import org.opensearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.opensearch.rest.action.admin.indices.RestUpgradeAction;
import org.opensearch.rest.action.admin.indices.RestUpgradeStatusAction;
import org.opensearch.rest.action.admin.indices.RestValidateQueryAction;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.rest.action.cat.RestAliasAction;
import org.opensearch.rest.action.cat.RestAllocationAction;
import org.opensearch.rest.action.cat.RestCatAction;
import org.opensearch.rest.action.cat.RestCatRecoveryAction;
import org.opensearch.rest.action.cat.RestCatSegmentReplicationAction;
import org.opensearch.rest.action.cat.RestFielddataAction;
import org.opensearch.rest.action.cat.RestHealthAction;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.action.cat.RestClusterManagerAction;
import org.opensearch.rest.action.cat.RestNodeAttrsAction;
import org.opensearch.rest.action.cat.RestNodesAction;
import org.opensearch.rest.action.cat.RestPitSegmentsAction;
import org.opensearch.rest.action.cat.RestPluginsAction;
import org.opensearch.rest.action.cat.RestRepositoriesAction;
import org.opensearch.rest.action.cat.RestSegmentsAction;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestSnapshotAction;
import org.opensearch.rest.action.cat.RestTasksAction;
import org.opensearch.rest.action.cat.RestTemplatesAction;
import org.opensearch.rest.action.cat.RestThreadPoolAction;
import org.opensearch.rest.action.document.RestBulkAction;
import org.opensearch.rest.action.document.RestDeleteAction;
import org.opensearch.rest.action.document.RestGetAction;
import org.opensearch.rest.action.document.RestGetSourceAction;
import org.opensearch.rest.action.document.RestIndexAction;
import org.opensearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.opensearch.rest.action.document.RestIndexAction.CreateHandler;
import org.opensearch.rest.action.document.RestMultiGetAction;
import org.opensearch.rest.action.document.RestMultiTermVectorsAction;
import org.opensearch.rest.action.document.RestTermVectorsAction;
import org.opensearch.rest.action.document.RestUpdateAction;
import org.opensearch.rest.action.ingest.RestDeletePipelineAction;
import org.opensearch.rest.action.ingest.RestGetPipelineAction;
import org.opensearch.rest.action.ingest.RestPutPipelineAction;
import org.opensearch.rest.action.ingest.RestSimulatePipelineAction;
import org.opensearch.rest.action.search.RestClearScrollAction;
import org.opensearch.rest.action.search.RestCountAction;
import org.opensearch.rest.action.search.RestCreatePitAction;
import org.opensearch.rest.action.search.RestDeletePitAction;
import org.opensearch.rest.action.search.RestExplainAction;
import org.opensearch.rest.action.search.RestGetAllPitsAction;
import org.opensearch.rest.action.search.RestMultiSearchAction;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.rest.action.search.RestSearchScrollAction;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.usage.UsageService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 *
 * @opensearch.internal
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final Map<String, ActionHandler<?, ?>> actions;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;

    public ActionModule(
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        ThreadPool threadPool,
        List<ActionPlugin> actionPlugins,
        NodeClient nodeClient,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        SystemIndices systemIndices
    ) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        autoCreateIndex = new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(new RestHeaderDefinition(Task.X_OPAQUE_ID, false))
        ).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext());
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList())
        );
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList())
        );

        restController = new RestController(headers, restWrapper, nodeClient, circuitBreakerService, usageService);
    }

    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions
            ) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(RemoteInfoAction.INSTANCE, TransportRemoteInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(NodesUsageAction.INSTANCE, TransportNodesUsageAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class);
        actions.register(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(CleanupRepositoryAction.INSTANCE, TransportCleanupRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(CloneSnapshotAction.INSTANCE, TransportCloneSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        actions.register(ClusterAddWeightedRoutingAction.INSTANCE, TransportAddWeightedRoutingAction.class);
        actions.register(ClusterGetWeightedRoutingAction.INSTANCE, TransportGetWeightedRoutingAction.class);
        actions.register(ClusterDeleteWeightedRoutingAction.INSTANCE, TransportDeleteWeightedRoutingAction.class);
        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(AddIndexBlockAction.INSTANCE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(
            GetFieldMappingsAction.INSTANCE,
            TransportGetFieldMappingsAction.class,
            TransportGetFieldMappingsIndexAction.class
        );
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(AutoPutMappingAction.INSTANCE, TransportAutoPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(DeleteComponentTemplateAction.INSTANCE, TransportDeleteComponentTemplateAction.class);
        actions.register(PutComposableIndexTemplateAction.INSTANCE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(DeleteComposableIndexTemplateAction.INSTANCE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(
            MultiTermVectorsAction.INSTANCE,
            TransportMultiTermVectorsAction.class,
            TransportShardMultiTermsVectorAction.class
        );
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class, TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class, TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(SegmentReplicationStatsAction.INSTANCE, TransportSegmentReplicationStatsAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);

        // Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(
            FieldCapabilitiesAction.INSTANCE,
            TransportFieldCapabilitiesAction.class,
            TransportFieldCapabilitiesIndexAction.class
        );

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Data streams:
        actions.register(CreateDataStreamAction.INSTANCE, CreateDataStreamAction.TransportAction.class);
        actions.register(DeleteDataStreamAction.INSTANCE, DeleteDataStreamAction.TransportAction.class);
        actions.register(GetDataStreamAction.INSTANCE, GetDataStreamAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);
        actions.register(DataStreamsStatsAction.INSTANCE, DataStreamsStatsAction.TransportAction.class);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);

        // Dangling indices
        actions.register(ListDanglingIndicesAction.INSTANCE, TransportListDanglingIndicesAction.class);
        actions.register(ImportDanglingIndexAction.INSTANCE, TransportImportDanglingIndexAction.class);
        actions.register(DeleteDanglingIndexAction.INSTANCE, TransportDeleteDanglingIndexAction.class);
        actions.register(FindDanglingIndexAction.INSTANCE, TransportFindDanglingIndexAction.class);

        // point in time actions
        actions.register(CreatePitAction.INSTANCE, TransportCreatePitAction.class);
        actions.register(DeletePitAction.INSTANCE, TransportDeletePitAction.class);
        actions.register(PitSegmentsAction.INSTANCE, TransportPitSegmentsAction.class);
        actions.register(GetAllPitsAction.INSTANCE, TransportGetAllPitsAction.class);

        // Remote Store
        actions.register(RestoreRemoteStoreAction.INSTANCE, TransportRestoreRemoteStoreAction.class);

        if (FeatureFlags.isEnabled(FeatureFlags.EXTENSIONS)) {
            // ExtensionProxyAction
            actions.register(ExtensionProxyAction.INSTANCE, ExtensionTransportAction.class);
        }

        // Decommission actions
        actions.register(DecommissionAction.INSTANCE, TransportDecommissionAction.class);
        actions.register(GetDecommissionStateAction.INSTANCE, TransportGetDecommissionStateAction.class);
        actions.register(DeleteDecommissionStateAction.INSTANCE, TransportDeleteDecommissionStateAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet()))
        );
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = handler -> {
            if (handler instanceof AbstractCatAction) {
                catActions.add((AbstractCatAction) handler);
            }
            restController.registerHandler(handler);
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestMainAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesStatsAction());
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction());
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestGetIndicesAction());
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction());
        registerHandler.accept(new RestIndicesShardStoresAction());
        registerHandler.accept(new RestGetAliasesAction());
        registerHandler.accept(new RestIndexDeleteAliasesAction());
        registerHandler.accept(new RestIndexPutAliasAction());
        registerHandler.accept(new RestIndicesAliasesAction());
        registerHandler.accept(new RestCreateIndexAction());
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction());
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction());
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction());
        registerHandler.accept(new RestRolloverIndexAction());
        registerHandler.accept(new RestDeleteIndexAction());
        registerHandler.accept(new RestCloseIndexAction());
        registerHandler.accept(new RestOpenIndexAction());
        registerHandler.accept(new RestAddIndexBlockAction());

        registerHandler.accept(new RestClusterPutWeightedRoutingAction());
        registerHandler.accept(new RestClusterGetWeightedRoutingAction());
        registerHandler.accept(new RestClusterDeleteWeightedRoutingAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestGetIndexTemplateAction());
        registerHandler.accept(new RestPutIndexTemplateAction());
        registerHandler.accept(new RestDeleteIndexTemplateAction());
        registerHandler.accept(new RestPutComponentTemplateAction());
        registerHandler.accept(new RestGetComponentTemplateAction());
        registerHandler.accept(new RestDeleteComponentTemplateAction());
        registerHandler.accept(new RestPutComposableIndexTemplateAction());
        registerHandler.accept(new RestGetComposableIndexTemplateAction());
        registerHandler.accept(new RestDeleteComposableIndexTemplateAction());
        registerHandler.accept(new RestSimulateIndexTemplateAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction(threadPool));
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestSyncedFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestUpgradeAction());
        registerHandler.accept(new RestUpgradeStatusAction());
        registerHandler.accept(new RestClearIndicesCacheAction());

        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler(nodesInCluster));
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction());
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestMultiSearchAction(settings));

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction());
        registerHandler.accept(new RestPutStoredScriptAction());
        registerHandler.accept(new RestDeleteStoredScriptAction());
        registerHandler.accept(new RestGetScriptContextAction());
        registerHandler.accept(new RestGetScriptLanguageAction());

        registerHandler.accept(new RestFieldCapabilitiesAction());

        // Tasks API
        registerHandler.accept(new RestListTasksAction(nodesInCluster));
        registerHandler.accept(new RestGetTaskAction());
        registerHandler.accept(new RestCancelTasksAction(nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction());
        registerHandler.accept(new RestGetPipelineAction());
        registerHandler.accept(new RestDeletePipelineAction());
        registerHandler.accept(new RestSimulatePipelineAction());

        // Dangling indices API
        registerHandler.accept(new RestListDanglingIndicesAction());
        registerHandler.accept(new RestImportDanglingIndexAction());
        registerHandler.accept(new RestDeleteDanglingIndexAction());

        // Data Stream API
        registerHandler.accept(new RestCreateDataStreamAction());
        registerHandler.accept(new RestDeleteDataStreamAction());
        registerHandler.accept(new RestGetDataStreamsAction());
        registerHandler.accept(new RestResolveIndexAction());
        registerHandler.accept(new RestDataStreamsStatsAction());

        // CAT API
        registerHandler.accept(new RestAllocationAction());
        registerHandler.accept(new RestCatSegmentReplicationAction());
        registerHandler.accept(new RestShardsAction());
        registerHandler.accept(new RestClusterManagerAction());
        registerHandler.accept(new RestNodesAction());
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction());
        registerHandler.accept(new RestSegmentsAction());
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.opensearch.rest.action.cat.RestCountAction());
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction());
        registerHandler.accept(new RestHealthAction());
        registerHandler.accept(new org.opensearch.rest.action.cat.RestPendingClusterTasksAction());
        registerHandler.accept(new RestAliasAction());
        registerHandler.accept(new RestThreadPoolAction());
        registerHandler.accept(new RestPluginsAction());
        registerHandler.accept(new RestFielddataAction());
        registerHandler.accept(new RestNodeAttrsAction());
        registerHandler.accept(new RestRepositoriesAction());
        registerHandler.accept(new RestSnapshotAction());
        registerHandler.accept(new RestTemplatesAction());

        // Point in time API
        registerHandler.accept(new RestCreatePitAction());
        registerHandler.accept(new RestDeletePitAction());
        registerHandler.accept(new RestGetAllPitsAction(nodesInCluster));
        registerHandler.accept(new RestPitSegmentsAction(nodesInCluster));
        registerHandler.accept(new RestDeleteDecommissionStateAction());

        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            )) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));
        registerHandler.accept(new RestDecommissionAction());
        registerHandler.accept(new RestGetDecommissionStateAction());

        // Remote Store APIs
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE)) {
            registerHandler.accept(new RestRestoreRemoteStoreAction());
        }
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {
        }).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {
        }).toInstance(indicesAliasesRequestRequestValidators);

        // Supporting classes
        bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
        bind(TransportLivenessAction.class).asEagerSingleton();

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder = MapBinder.newMapBinder(
            binder(),
            ActionType.class,
            TransportAction.class
        );
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public RestController getRestController() {
        return restController;
    }
}
