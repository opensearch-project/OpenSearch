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

package org.opensearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.opensearch.action.admin.indices.close.TransportCloseIndexAction;
import org.opensearch.action.search.CreatePitController;
import org.opensearch.action.search.SearchRequestSlowLog;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.bootstrap.BootstrapSettings;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.applicationtemplates.SystemTemplatesService;
import org.opensearch.cluster.coordination.ClusterBootstrapService;
import org.opensearch.cluster.coordination.ClusterFormationFailureHelper;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.coordination.ElectionSchedulerFactory;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.JoinHelper;
import org.opensearch.cluster.coordination.LagDetector;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.coordination.Reconfigurator;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ConcurrentRecoveriesAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.NodeLoadAwareAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.discovery.DiscoveryModule;
import org.opensearch.discovery.HandshakingTransportAddressConnector;
import org.opensearch.discovery.PeerFinder;
import org.opensearch.discovery.SeedHostsResolver;
import org.opensearch.discovery.SettingsBasedSeedHostsProvider;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.DanglingIndicesState;
import org.opensearch.gateway.GatewayService;
import org.opensearch.gateway.PersistedClusterStateService;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.opensearch.gateway.remote.RemoteClusterStateCleanupManager;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.RemoteIndexMetadataManager;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexingPressure;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.ShardIndexingPressureMemoryManager;
import org.opensearch.index.ShardIndexingPressureSettings;
import org.opensearch.index.ShardIndexingPressureStore;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.remote.RemoteStorePressureSettings;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.PublishCheckpointAction;
import org.opensearch.indices.store.IndicesStore;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.fs.FsHealthService;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmGcMonitorService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.monitor.process.ProcessService;
import org.opensearch.node.Node;
import org.opensearch.node.Node.DiscoverySettings;
import org.opensearch.node.NodeRoleSettings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.persistent.PersistentTasksClusterService;
import org.opensearch.persistent.decider.EnableAssignmentDecider;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.backpressure.settings.NodeDuressSettings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.opensearch.snapshots.InternalSnapshotsInfoService;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.tasks.TaskCancellationMonitoringSettings;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.consumer.TopNSearchTasksLogger;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProxyConnectionStrategy;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.RemoteConnectionStrategy;
import org.opensearch.transport.SniffConnectionStrategy;
import org.opensearch.transport.TransportSettings;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING;
import static org.opensearch.gateway.remote.RemoteIndexMetadataManager.INDEX_METADATA_UPLOAD_TIMEOUT_SETTING;
import static org.opensearch.gateway.remote.RemoteManifestManager.METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING;

/**
 * Encapsulates all valid cluster level settings.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ClusterSettings extends AbstractScopedSettings {

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet) {
        this(nodeSettings, settingsSet, Collections.emptySet());
    }

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet, final Set<SettingUpgrader<?>> settingUpgraders) {
        super(nodeSettings, settingsSet, settingUpgraders, Property.NodeScope);
        addSettingsUpdater(new LoggingSettingUpdater(nodeSettings));
    }

    private static final class LoggingSettingUpdater implements SettingUpdater<Settings> {
        final Predicate<String> loggerPredicate = Loggers.LOG_LEVEL_SETTING::match;
        private final Settings settings;

        LoggingSettingUpdater(Settings settings) {
            this.settings = settings;
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            return current.filter(loggerPredicate).equals(previous.filter(loggerPredicate)) == false;
        }

        @Override
        public Settings getValue(Settings current, Settings previous) {
            Settings.Builder builder = Settings.builder();
            builder.put(current.filter(loggerPredicate));
            for (String key : previous.keySet()) {
                if (loggerPredicate.test(key) && builder.keys().contains(key) == false) {
                    if (Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).exists(settings) == false) {
                        builder.putNull(key);
                    } else {
                        builder.put(key, Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).get(settings).toString());
                    }
                }
            }
            return builder.build();
        }

        @Override
        public void apply(Settings value, Settings current, Settings previous) {
            for (String key : value.keySet()) {
                assert loggerPredicate.test(key);
                String component = key.substring("logger.".length());
                if ("level".equals(component)) {
                    continue;
                }
                if ("_root".equals(component)) {
                    final String rootLevel = value.get(key);
                    if (rootLevel == null) {
                        Loggers.setLevel(LogManager.getRootLogger(), Loggers.LOG_DEFAULT_LEVEL_SETTING.get(settings));
                    } else {
                        Loggers.setLevel(LogManager.getRootLogger(), rootLevel);
                    }
                } else {
                    Loggers.setLevel(LogManager.getLogger(component), value.get(key));
                }
            }
        }
    }

    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
                AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING,
                BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING,
                BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING,
                BalancedShardsAllocator.PRIMARY_SHARD_REBALANCE_BUFFER,
                BalancedShardsAllocator.PREFER_PRIMARY_SHARD_BALANCE,
                BalancedShardsAllocator.PREFER_PRIMARY_SHARD_REBALANCE,
                BalancedShardsAllocator.SHARD_MOVE_PRIMARY_FIRST_SETTING,
                BalancedShardsAllocator.SHARD_MOVEMENT_STRATEGY_SETTING,
                BalancedShardsAllocator.THRESHOLD_SETTING,
                BalancedShardsAllocator.IGNORE_THROTTLE_FOR_REMOTE_RESTORE,
                BalancedShardsAllocator.ALLOCATOR_TIMEOUT_SETTING,
                BalancedShardsAllocator.FOLLOW_UP_REROUTE_PRIORITY_SETTING,
                BalancedShardsAllocator.PRIMARY_CONSTRAINT_THRESHOLD_SETTING,
                BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING,
                BreakerSettings.CIRCUIT_BREAKER_OVERHEAD_SETTING,
                BreakerSettings.CIRCUIT_BREAKER_TYPE,
                ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
                ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
                ConcurrentRecoveriesAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING,
                DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING,
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
                EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING,
                ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE,
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
                FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING,
                FsRepository.REPOSITORIES_COMPRESS_SETTING,
                FsRepository.REPOSITORIES_LOCATION_SETTING,
                IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
                IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING,
                IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING,
                IndicesService.CLUSTER_DEFAULT_INDEX_MAX_MERGE_AT_ONCE_SETTING,
                IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING,
                IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING,
                IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING,
                IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING,
                IndicesService.CLUSTER_REPLICATION_TYPE_SETTING,
                MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
                MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING,
                Metadata.SETTING_READ_ONLY_SETTING,
                Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING,
                Metadata.DEFAULT_REPLICA_COUNT_SETTING,
                Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING,
                ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE,
                ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER,
                ShardLimitValidator.SETTING_CLUSTER_IGNORE_DOT_INDEXES,
                RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
                RecoverySettings.INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING,
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
                RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
                RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
                RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
                RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING,
                RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING,
                RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_REMOTE_STORE_STREAMS_SETTING,
                RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT,
                RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
                DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
                DiskThresholdSettings.CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING,
                SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING,
                ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING,
                InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING,
                InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING,
                InternalSnapshotsInfoService.INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING,
                DestructiveOperations.REQUIRES_NAME_SETTING,
                NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING,  // deprecated
                NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING,
                GatewayService.EXPECTED_DATA_NODES_SETTING,
                GatewayService.RECOVER_AFTER_DATA_NODES_SETTING,
                GatewayService.RECOVER_AFTER_TIME_SETTING,
                ShardsBatchGatewayAllocator.GATEWAY_ALLOCATOR_BATCH_SIZE,
                ShardsBatchGatewayAllocator.PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING,
                ShardsBatchGatewayAllocator.REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING,
                ShardsBatchGatewayAllocator.FOLLOW_UP_REROUTE_PRIORITY_SETTING,
                PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD,
                NetworkModule.HTTP_DEFAULT_TYPE_SETTING,
                NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING,
                NetworkModule.HTTP_TYPE_SETTING,
                NetworkModule.TRANSPORT_TYPE_SETTING,
                NetworkModule.TRANSPORT_SSL_DUAL_MODE_ENABLED,
                NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION,
                NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME,
                NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_SETTING,
                HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS,
                HttpTransportSettings.SETTING_CORS_ENABLED,
                HttpTransportSettings.SETTING_CORS_MAX_AGE,
                HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
                HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN,
                HttpTransportSettings.SETTING_HTTP_HOST,
                HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST,
                HttpTransportSettings.SETTING_HTTP_BIND_HOST,
                HttpTransportSettings.SETTING_HTTP_PORT,
                HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT,
                HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS,
                HttpTransportSettings.SETTING_HTTP_COMPRESSION,
                HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL,
                HttpTransportSettings.SETTING_CORS_ALLOW_METHODS,
                HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS,
                HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
                HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED,
                HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH,
                HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE,
                HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE,
                HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT,
                HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE,
                HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH,
                HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT,
                HttpTransportSettings.SETTING_HTTP_CONNECT_TIMEOUT,
                HttpTransportSettings.SETTING_HTTP_RESET_COOKIES,
                HttpTransportSettings.OLD_SETTING_HTTP_TCP_NO_DELAY,
                HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY,
                HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE,
                HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE,
                HttpTransportSettings.SETTING_HTTP_TCP_KEEP_INTERVAL,
                HttpTransportSettings.SETTING_HTTP_TCP_KEEP_COUNT,
                HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS,
                HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
                HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
                HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE,
                HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE,
                HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING,
                HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                IndexModule.NODE_STORE_ALLOW_MMAP,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                ClusterService.USER_DEFINED_METADATA,
                ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                IngestService.MAX_NUMBER_OF_INGEST_PROCESSORS,
                SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING,
                SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS,
                TransportSearchAction.SHARD_COUNT_LIMIT_SETTING,
                TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING,
                TransportSearchAction.SEARCH_PHASE_TOOK_ENABLED,
                SearchRequestStats.SEARCH_REQUEST_STATS_ENABLED,
                RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE,
                SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER,
                RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING,
                RemoteClusterService.REMOTE_NODE_ATTRIBUTE,
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE,
                RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
                RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
                ProxyConnectionStrategy.PROXY_ADDRESS,
                ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
                ProxyConnectionStrategy.SERVER_NAME,
                ProxyConnectionStrategy.SERVER_NAME,
                SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY,
                SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
                SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
                TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING,
                ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
                ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING,
                NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING,
                HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING,
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING,
                TransportReplicationAction.REPLICATION_INITIAL_RETRY_BACKOFF_BOUND,
                TransportReplicationAction.REPLICATION_RETRY_TIMEOUT,
                PublishCheckpointAction.PUBLISH_CHECK_POINT_RETRY_TIMEOUT,
                TransportSettings.HOST,
                TransportSettings.PUBLISH_HOST,
                TransportSettings.PUBLISH_HOST_PROFILE,
                TransportSettings.BIND_HOST,
                TransportSettings.BIND_HOST_PROFILE,
                TransportSettings.OLD_PORT,
                TransportSettings.PORT,
                TransportSettings.PORT_PROFILE,
                TransportSettings.PUBLISH_PORT,
                TransportSettings.PUBLISH_PORT_PROFILE,
                TransportSettings.OLD_TRANSPORT_COMPRESS,
                TransportSettings.TRANSPORT_COMPRESS,
                TransportSettings.PING_SCHEDULE,
                TransportSettings.TCP_CONNECT_TIMEOUT,
                TransportSettings.CONNECT_TIMEOUT,
                TransportSettings.DEFAULT_FEATURES_SETTING,
                TransportSettings.OLD_TCP_NO_DELAY,
                TransportSettings.TCP_NO_DELAY,
                TransportSettings.OLD_TCP_NO_DELAY_PROFILE,
                TransportSettings.TCP_NO_DELAY_PROFILE,
                TransportSettings.TCP_KEEP_ALIVE,
                TransportSettings.OLD_TCP_KEEP_ALIVE_PROFILE,
                TransportSettings.TCP_KEEP_ALIVE_PROFILE,
                TransportSettings.TCP_KEEP_IDLE,
                TransportSettings.TCP_KEEP_IDLE_PROFILE,
                TransportSettings.TCP_KEEP_INTERVAL,
                TransportSettings.TCP_KEEP_INTERVAL_PROFILE,
                TransportSettings.TCP_KEEP_COUNT,
                TransportSettings.TCP_KEEP_COUNT_PROFILE,
                TransportSettings.TCP_REUSE_ADDRESS,
                TransportSettings.OLD_TCP_REUSE_ADDRESS_PROFILE,
                TransportSettings.TCP_REUSE_ADDRESS_PROFILE,
                TransportSettings.TCP_SEND_BUFFER_SIZE,
                TransportSettings.OLD_TCP_SEND_BUFFER_SIZE_PROFILE,
                TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE,
                TransportSettings.TCP_RECEIVE_BUFFER_SIZE,
                TransportSettings.OLD_TCP_RECEIVE_BUFFER_SIZE_PROFILE,
                TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE,
                TransportSettings.CONNECTIONS_PER_NODE_RECOVERY,
                TransportSettings.CONNECTIONS_PER_NODE_BULK,
                TransportSettings.CONNECTIONS_PER_NODE_REG,
                TransportSettings.CONNECTIONS_PER_NODE_STATE,
                TransportSettings.CONNECTIONS_PER_NODE_PING,
                TransportSettings.TRACE_LOG_EXCLUDE_SETTING,
                TransportSettings.TRACE_LOG_INCLUDE_SETTING,
                TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING,
                NetworkService.NETWORK_SERVER,
                NetworkService.GLOBAL_NETWORK_HOST_SETTING,
                NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING,
                NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING,
                NetworkService.TCP_NO_DELAY,
                NetworkService.TCP_KEEP_ALIVE,
                NetworkService.TCP_KEEP_IDLE,
                NetworkService.TCP_KEEP_INTERVAL,
                NetworkService.TCP_KEEP_COUNT,
                NetworkService.TCP_REUSE_ADDRESS,
                NetworkService.TCP_SEND_BUFFER_SIZE,
                NetworkService.TCP_RECEIVE_BUFFER_SIZE,
                NetworkService.TCP_CONNECT_TIMEOUT,
                IndexSettings.QUERY_STRING_ANALYZE_WILDCARD,
                IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD,
                IndexSettings.TIME_SERIES_INDEX_MERGE_POLICY,
                ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING,
                ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
                ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING,
                ScriptService.SCRIPT_CACHE_SIZE_SETTING,
                ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
                ScriptService.SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING,
                ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING,
                ScriptService.SCRIPT_MAX_SIZE_IN_BYTES,
                ScriptService.TYPES_ALLOWED_SETTING,
                ScriptService.CONTEXTS_ALLOWED_SETTING,
                IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING,
                IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
                IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
                IndicesRequestCache.INDICES_CACHE_QUERY_EXPIRE,
                IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING,
                IndicesRequestCache.INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING,
                IndicesRequestCache.INDICES_REQUEST_CACHE_MAX_SIZE_ALLOWED_IN_CACHE_SETTING,
                HunspellService.HUNSPELL_LAZY_LOAD,
                HunspellService.HUNSPELL_IGNORE_CASE,
                HunspellService.HUNSPELL_DICTIONARY_OPTIONS,
                IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT,
                Environment.PATH_DATA_SETTING,
                Environment.PATH_HOME_SETTING,
                Environment.PATH_LOGS_SETTING,
                Environment.PATH_REPO_SETTING,
                Environment.PATH_SHARED_DATA_SETTING,
                Environment.PIDFILE_SETTING,
                Environment.NODE_PIDFILE_SETTING,
                NodeEnvironment.NODE_ID_SEED_SETTING,
                DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING,
                DiscoveryModule.DISCOVERY_TYPE_SETTING,
                DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING,
                DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING,
                DiscoveryModule.ELECTION_STRATEGY_SETTING,
                SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING,
                SettingsBasedSeedHostsProvider.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
                SeedHostsResolver.DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING,
                SeedHostsResolver.DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING,
                SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING,
                SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT,
                SearchService.DEFAULT_KEEPALIVE_SETTING,
                SearchService.KEEPALIVE_INTERVAL_SETTING,
                SearchService.MAX_KEEPALIVE_SETTING,
                SearchService.ALLOW_EXPENSIVE_QUERIES,
                MultiBucketConsumerService.MAX_BUCKET_SETTING,
                SearchService.LOW_LEVEL_CANCELLATION_SETTING,
                SearchService.MAX_OPEN_SCROLL_CONTEXT,
                SearchService.MAX_OPEN_PIT_CONTEXT,
                SearchService.MAX_PIT_KEEPALIVE_SETTING,
                SearchService.MAX_AGGREGATION_REWRITE_FILTERS,
                SearchService.AGGREGATION_REWRITE_FILTER_SEGMENT_THRESHOLD,
                SearchService.INDICES_MAX_CLAUSE_COUNT_SETTING,
                SearchService.CARDINALITY_AGGREGATION_PRUNING_THRESHOLD,
                SearchService.KEYWORD_INDEX_OR_DOC_VALUES_ENABLED,
                CreatePitController.PIT_INIT_KEEP_ALIVE,
                Node.WRITE_PORTS_FILE_SETTING,
                Node.NODE_NAME_SETTING,
                Node.NODE_ATTRIBUTES,
                Node.NODE_LOCAL_STORAGE_SETTING,
                NodeRoleSettings.NODE_ROLES_SETTING,
                AutoCreateIndex.AUTO_CREATE_INDEX_SETTING,
                BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX,
                ClusterName.CLUSTER_NAME_SETTING,
                Client.CLIENT_TYPE_SETTING_S,
                ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING,
                OpenSearchExecutors.PROCESSORS_SETTING,
                OpenSearchExecutors.NODE_PROCESSORS_SETTING,
                ThreadContext.DEFAULT_HEADERS_SETTING,
                Loggers.LOG_DEFAULT_LEVEL_SETTING,
                Loggers.LOG_LEVEL_SETTING,
                NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING,
                NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING,
                OsService.REFRESH_INTERVAL_SETTING,
                ProcessService.REFRESH_INTERVAL_SETTING,
                JvmService.REFRESH_INTERVAL_SETTING,
                FsService.REFRESH_INTERVAL_SETTING,
                JvmGcMonitorService.ENABLED_SETTING,
                JvmGcMonitorService.REFRESH_INTERVAL_SETTING,
                JvmGcMonitorService.GC_SETTING,
                JvmGcMonitorService.GC_OVERHEAD_WARN_SETTING,
                JvmGcMonitorService.GC_OVERHEAD_INFO_SETTING,
                JvmGcMonitorService.GC_OVERHEAD_DEBUG_SETTING,
                PageCacheRecycler.LIMIT_HEAP_SETTING,
                PageCacheRecycler.WEIGHT_BYTES_SETTING,
                PageCacheRecycler.WEIGHT_INT_SETTING,
                PageCacheRecycler.WEIGHT_LONG_SETTING,
                PageCacheRecycler.WEIGHT_OBJECTS_SETTING,
                PageCacheRecycler.TYPE_SETTING,
                PluginsService.MANDATORY_SETTING,
                BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING,
                BootstrapSettings.MEMORY_LOCK_SETTING,
                BootstrapSettings.SYSTEM_CALL_FILTER_SETTING,
                BootstrapSettings.CTRLHANDLER_SETTING,
                KeyStoreWrapper.SEED_SETTING,
                IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
                IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING,
                IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING,
                IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING,
                IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING,
                ResourceWatcherService.ENABLED,
                ResourceWatcherService.RELOAD_INTERVAL_HIGH,
                ResourceWatcherService.RELOAD_INTERVAL_MEDIUM,
                ResourceWatcherService.RELOAD_INTERVAL_LOW,
                ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING,
                FastVectorHighlighter.SETTING_TV_HIGHLIGHT_MULTI_VALUE,
                Node.BREAKER_TYPE_KEY,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                OperationRouting.IGNORE_AWARENESS_ATTRIBUTES_SETTING,
                OperationRouting.WEIGHTED_ROUTING_DEFAULT_WEIGHT,
                OperationRouting.WEIGHTED_ROUTING_FAILOPEN_ENABLED,
                OperationRouting.STRICT_WEIGHTED_SHARD_ROUTING_ENABLED,
                OperationRouting.IGNORE_WEIGHTED_SHARD_ROUTING,
                OperationRouting.STRICT_SEARCH_REPLICA_ROUTING_ENABLED,
                IndexGraveyard.SETTING_MAX_TOMBSTONES,
                PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING,
                EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING,
                PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING,
                PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_DURING_DECOMMISSION_SETTING,
                PeerFinder.DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING,
                ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING,
                ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING,
                ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING,
                ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING,
                ElectionSchedulerFactory.ELECTION_DURATION_SETTING,
                Coordinator.PUBLISH_TIMEOUT_SETTING,
                Coordinator.PUBLISH_INFO_TIMEOUT_SETTING,
                JoinHelper.JOIN_TIMEOUT_SETTING,
                FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING,
                FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING,
                FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING,
                LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING,
                LeaderChecker.LEADER_CHECK_INTERVAL_SETTING,
                LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING,
                Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION,
                TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING,
                ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING,  // deprecated
                ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING,
                ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING,
                LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING,
                HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING,
                HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING,
                SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING,
                SnapshotsService.MAX_SHARDS_ALLOWED_IN_STATUS_API,
                FsHealthService.ENABLED_SETTING,
                FsHealthService.REFRESH_INTERVAL_SETTING,
                FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING,
                FsHealthService.HEALTHY_TIMEOUT_SETTING,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FLAT_SKEW_SETTING,
                ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED,
                ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED,
                ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW,
                ShardIndexingPressureSettings.SHARD_MIN_LIMIT,
                ShardIndexingPressureStore.MAX_COLD_STORE_SIZE,
                ShardIndexingPressureMemoryManager.LOWER_OPERATING_FACTOR,
                ShardIndexingPressureMemoryManager.OPTIMAL_OPERATING_FACTOR,
                ShardIndexingPressureMemoryManager.UPPER_OPERATING_FACTOR,
                ShardIndexingPressureMemoryManager.NODE_SOFT_LIMIT,
                ShardIndexingPressureMemoryManager.THROUGHPUT_DEGRADATION_LIMITS,
                ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT,
                ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS,
                IndexingPressure.MAX_INDEXING_BYTES,
                TaskResourceTrackingService.TASK_RESOURCE_TRACKING_ENABLED,
                TaskManager.TASK_RESOURCE_CONSUMERS_ENABLED,
                TopNSearchTasksLogger.LOG_TOP_QUERIES_SIZE_SETTING,
                TopNSearchTasksLogger.LOG_TOP_QUERIES_FREQUENCY_SETTING,
                ClusterManagerTaskThrottler.THRESHOLD_SETTINGS,
                ClusterManagerTaskThrottler.BASE_DELAY_SETTINGS,
                ClusterManagerTaskThrottler.MAX_DELAY_SETTINGS,
                // Settings related to search backpressure
                SearchBackpressureSettings.SETTING_MODE,

                NodeDuressSettings.SETTING_NUM_SUCCESSIVE_BREACHES,
                NodeDuressSettings.SETTING_CPU_THRESHOLD,
                NodeDuressSettings.SETTING_HEAP_THRESHOLD,
                SearchTaskSettings.SETTING_CANCELLATION_RATIO,
                SearchTaskSettings.SETTING_CANCELLATION_RATE,
                SearchTaskSettings.SETTING_CANCELLATION_BURST,
                SearchTaskSettings.SETTING_HEAP_PERCENT_THRESHOLD,
                SearchTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD,
                SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE,
                SearchTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD,
                SearchTaskSettings.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD,
                SearchTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD,
                SearchShardTaskSettings.SETTING_CANCELLATION_RATIO,
                SearchShardTaskSettings.SETTING_CANCELLATION_RATE,
                SearchShardTaskSettings.SETTING_CANCELLATION_BURST,
                SearchShardTaskSettings.SETTING_HEAP_PERCENT_THRESHOLD,
                SearchShardTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD,
                SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE,
                SearchShardTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD,
                SearchShardTaskSettings.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD,
                SearchShardTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD,
                SearchBackpressureSettings.SETTING_CANCELLATION_RATIO,  // deprecated
                SearchBackpressureSettings.SETTING_CANCELLATION_RATE,   // deprecated
                SearchBackpressureSettings.SETTING_CANCELLATION_BURST,   // deprecated
                SegmentReplicationPressureService.SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED,
                SegmentReplicationPressureService.MAX_INDEXING_CHECKPOINTS,
                SegmentReplicationPressureService.MAX_REPLICATION_TIME_BACKPRESSURE_SETTING,
                SegmentReplicationPressureService.MAX_REPLICATION_LIMIT_STALE_REPLICA_SETTING,
                SegmentReplicationPressureService.MAX_ALLOWED_STALE_SHARDS,

                // Settings related to resource trackers
                ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
                ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
                ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING,

                // Settings related to Searchable Snapshots
                Node.NODE_SEARCH_CACHE_SIZE_SETTING,
                FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING,

                // Settings related to Remote Refresh Segment Pressure
                RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED,
                RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR,
                RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR,
                RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT,

                // Settings related to Remote Store stats
                RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE,

                // Related to monitoring of task cancellation
                TaskCancellationMonitoringSettings.IS_ENABLED_SETTING,
                TaskCancellationMonitoringSettings.DURATION_MILLIS_SETTING,

                // Search request slow log settings
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL,

                // Remote cluster state settings
                RemoteClusterStateCleanupManager.REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING,
                RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING,
                RemoteClusterStateService.REMOTE_PUBLICATION_SETTING,
                RemoteClusterStateService.REMOTE_STATE_DOWNLOAD_TO_SERVE_READ_API,

                INDEX_METADATA_UPLOAD_TIMEOUT_SETTING,
                GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING,
                METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING,
                RemoteClusterStateService.REMOTE_STATE_READ_TIMEOUT_SETTING,
                RemoteClusterStateService.CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX,
                RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_PATH_TYPE_SETTING,
                RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_PATH_HASH_ALGO_SETTING,
                RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
                RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING,
                IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING,
                IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING,
                RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING,
                RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING,
                RemoteClusterStateService.REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING,
                RemoteRoutingTableBlobStore.CLUSTER_REMOTE_STORE_ROUTING_TABLE_PATH_PREFIX,

                // Admission Control Settings
                AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE,
                CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT,
                CpuBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT,
                CpuBasedAdmissionControllerSettings.CLUSTER_ADMIN_CPU_USAGE_LIMIT,
                IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE,
                IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT,
                IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT,

                // Concurrent segment search settings
                SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING, // deprecated
                SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING,
                SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE,

                RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING,
                RemoteStoreSettings.CLUSTER_REMOTE_MAX_TRANSLOG_READERS,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX,
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX,

                // Snapshot related Settings
                BlobStoreRepository.SNAPSHOT_SHARD_PATH_PREFIX_SETTING,
                BlobStoreRepository.SNAPSHOT_REPOSITORY_DATA_CACHE_THRESHOLD,

                SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING,

                // Composite index settings
                CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING,
                CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,

                SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED,

                // WorkloadManagement settings
                WorkloadManagementSettings.NODE_LEVEL_CPU_REJECTION_THRESHOLD,
                WorkloadManagementSettings.NODE_LEVEL_CPU_CANCELLATION_THRESHOLD,
                WorkloadManagementSettings.NODE_LEVEL_MEMORY_REJECTION_THRESHOLD,
                WorkloadManagementSettings.NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD,
                WorkloadManagementSettings.WLM_MODE_SETTING,
                WorkloadManagementSettings.QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING,
                WorkloadManagementSettings.QUERYGROUP_SERVICE_DURESS_STREAK_SETTING,

                // Settings to be used for limiting rest requests
                ResponseLimitSettings.CAT_INDICES_RESPONSE_LIMIT_SETTING,
                ResponseLimitSettings.CAT_SHARDS_RESPONSE_LIMIT_SETTING,
                ResponseLimitSettings.CAT_SEGMENTS_RESPONSE_LIMIT_SETTING,

                // Thread pool Settings
                ThreadPool.CLUSTER_THREAD_POOL_SIZE_SETTING,

                // Tiered caching settings
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE),
                OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ),
                OpenSearchOnHeapCacheSettings.EXPIRE_AFTER_ACCESS_SETTING.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ),

                // Setting related to refresh optimisations
                IndicesService.CLUSTER_REFRESH_FIXED_INTERVAL_SCHEDULE_ENABLED_SETTING
            )
        )
    );

    public static List<SettingUpgrader<?>> BUILT_IN_SETTING_UPGRADERS = Collections.emptyList();

    /**
     * Map of feature flag name to feature-flagged cluster settings. Once each feature
     * is ready for production release, the feature flag can be removed, and the
     * setting should be moved to {@link #BUILT_IN_CLUSTER_SETTINGS}.
     */
    public static final Map<List<String>, List<Setting>> FEATURE_FLAGGED_CLUSTER_SETTINGS = Map.of(
        List.of(FeatureFlags.TELEMETRY),
        List.of(
            TelemetrySettings.TRACER_ENABLED_SETTING,
            TelemetrySettings.TRACER_SAMPLER_PROBABILITY,
            TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING,
            TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING,
            TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING
        )
    );
}
