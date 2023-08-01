/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicyFactory;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A factory to create an EngineConfig based on custom plugin overrides
 *
 * @opensearch.internal
 */
public class EngineConfigFactory {
    private final CodecServiceFactory codecServiceFactory;
    private final TranslogDeletionPolicyFactory translogDeletionPolicyFactory;

    /** default ctor primarily used for tests without plugins */
    public EngineConfigFactory(IndexSettings idxSettings) {
        this(Collections.emptyList(), idxSettings);
    }

    /**
     * Construct a factory using the plugin service and provided index settings
     */
    public EngineConfigFactory(PluginsService pluginsService, IndexSettings idxSettings) {
        this(pluginsService.filterPlugins(EnginePlugin.class), idxSettings);
    }

    /* private constructor to construct the factory from specific EnginePlugins and IndexSettings */
    EngineConfigFactory(Collection<EnginePlugin> enginePlugins, IndexSettings idxSettings) {
        Optional<CodecService> codecService = Optional.empty();
        String codecServiceOverridingPlugin = null;
        Optional<CodecServiceFactory> codecServiceFactory = Optional.empty();
        String codecServiceFactoryOverridingPlugin = null;
        Optional<TranslogDeletionPolicyFactory> translogDeletionPolicyFactory = Optional.empty();
        String translogDeletionPolicyOverridingPlugin = null;
        for (EnginePlugin enginePlugin : enginePlugins) {
            // get overriding codec service from EnginePlugin
            if (codecService.isPresent() == false) {
                codecService = enginePlugin.getCustomCodecService(idxSettings);
                codecServiceOverridingPlugin = enginePlugin.getClass().getName();
            } else if (enginePlugin.getCustomCodecService(idxSettings).isPresent()) {
                throw new IllegalStateException(
                    "existing codec service already overridden in: "
                        + codecServiceOverridingPlugin
                        + " attempting to override again by: "
                        + enginePlugin.getClass().getName()
                );
            }
            if (translogDeletionPolicyFactory.isPresent() == false) {
                translogDeletionPolicyFactory = enginePlugin.getCustomTranslogDeletionPolicyFactory();
                translogDeletionPolicyOverridingPlugin = enginePlugin.getClass().getName();
            } else if (enginePlugin.getCustomTranslogDeletionPolicyFactory().isPresent()) {
                throw new IllegalStateException(
                    "existing TranslogDeletionPolicyFactory is already overridden in: "
                        + translogDeletionPolicyOverridingPlugin
                        + " attempting to override again by: "
                        + enginePlugin.getClass().getName()
                );
            }
            // get overriding CodecServiceFactory from EnginePlugin
            if (codecServiceFactory.isPresent() == false) {
                codecServiceFactory = enginePlugin.getCustomCodecServiceFactory(idxSettings);
                codecServiceFactoryOverridingPlugin = enginePlugin.getClass().getName();
            } else if (enginePlugin.getCustomCodecServiceFactory(idxSettings).isPresent()) {
                throw new IllegalStateException(
                    "existing codec service factory already overridden in: "
                        + codecServiceFactoryOverridingPlugin
                        + " attempting to override again by: "
                        + enginePlugin.getClass().getName()
                );
            }
        }

        if (codecService.isPresent() && codecServiceFactory.isPresent()) {
            throw new IllegalStateException(
                "both codec service and codec service factory are present, codec service provided by: "
                    + codecServiceOverridingPlugin
                    + " conflicts with codec service factory provided by: "
                    + codecServiceFactoryOverridingPlugin
            );
        }

        final CodecService instance = codecService.orElse(null);
        this.codecServiceFactory = (instance != null) ? (config) -> instance : codecServiceFactory.orElse(null);
        this.translogDeletionPolicyFactory = translogDeletionPolicyFactory.orElse((idxs, rtls) -> null);
    }

    /**
     * Instantiates a new EngineConfig from the provided custom overrides
     */
    public EngineConfig newEngineConfig(
        ShardId shardId,
        ThreadPool threadPool,
        IndexSettings indexSettings,
        Engine.Warmer warmer,
        Store store,
        MergePolicy mergePolicy,
        Analyzer analyzer,
        Similarity similarity,
        CodecService codecService,
        Engine.EventListener eventListener,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        TranslogConfig translogConfig,
        TimeValue flushMergesAfter,
        List<ReferenceManager.RefreshListener> externalRefreshListener,
        List<ReferenceManager.RefreshListener> internalRefreshListener,
        Sort indexSort,
        CircuitBreakerService circuitBreakerService,
        LongSupplier globalCheckpointSupplier,
        Supplier<RetentionLeases> retentionLeasesSupplier,
        LongSupplier primaryTermSupplier,
        EngineConfig.TombstoneDocSupplier tombstoneDocSupplier,
        boolean isReadOnlyReplica,
        BooleanSupplier primaryModeSupplier,
        TranslogFactory translogFactory,
        Comparator<LeafReader> leafSorter
    ) {
        CodecService codecServiceToUse = codecService;
        if (codecService == null && this.codecServiceFactory != null) {
            codecServiceToUse = newCodecServiceOrDefault(indexSettings, null, null, null);
        }

        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .warmer(warmer)
            .store(store)
            .mergePolicy(mergePolicy)
            .analyzer(analyzer)
            .similarity(similarity)
            .codecService(codecServiceToUse)
            .eventListener(eventListener)
            .queryCache(queryCache)
            .queryCachingPolicy(queryCachingPolicy)
            .translogConfig(translogConfig)
            .translogDeletionPolicyFactory(translogDeletionPolicyFactory)
            .flushMergesAfter(flushMergesAfter)
            .externalRefreshListener(externalRefreshListener)
            .internalRefreshListener(internalRefreshListener)
            .indexSort(indexSort)
            .circuitBreakerService(circuitBreakerService)
            .globalCheckpointSupplier(globalCheckpointSupplier)
            .retentionLeasesSupplier(retentionLeasesSupplier)
            .primaryTermSupplier(primaryTermSupplier)
            .tombstoneDocSupplier(tombstoneDocSupplier)
            .readOnlyReplica(isReadOnlyReplica)
            .primaryModeSupplier(primaryModeSupplier)
            .translogFactory(translogFactory)
            .leafSorter(leafSorter)
            .build();
    }

    public CodecService newCodecServiceOrDefault(
        IndexSettings indexSettings,
        @Nullable MapperService mapperService,
        Logger logger,
        CodecService defaultCodecService
    ) {
        return this.codecServiceFactory != null
            ? this.codecServiceFactory.createCodecService(new CodecServiceConfig(indexSettings, mapperService, logger))
            : defaultCodecService;
    }
}
