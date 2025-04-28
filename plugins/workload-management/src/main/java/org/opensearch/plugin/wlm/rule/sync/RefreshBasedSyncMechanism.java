/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class pulls the latest rules from the RULES system index to update the in-memory view
 */
public class RefreshBasedSyncMechanism extends AbstractLifecycleComponent {
    public static final String RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME = "wlm.rule.sync_refresh_interval";
    public static final long RULE_SYNC_REFRESH_INTERVAL_DEFAULT = 5000;

    /**
     * Setting to control the run interval of Query Group Service
     */
    public static final Setting<Long> RULE_SYNC_REFRESH_INTERVAL_SETTING = Setting.longSetting(
        RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME,
        RULE_SYNC_REFRESH_INTERVAL_DEFAULT,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final int MIN_SYNC_REFRESH_INTERVAL = 1000;

    private final Client client;
    private final ThreadPool threadPool;
    private long refreshInterval;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final RuleEntityParser parser;
    private final InMemoryRuleProcessingService ruleProcessingService;
    private final FeatureType featureType;
    private static final Logger logger = LogManager.getLogger(RefreshBasedSyncMechanism.class);

    public RefreshBasedSyncMechanism(
        Client client,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings,
        RuleEntityParser parser,
        InMemoryRuleProcessingService ruleProcessingService,
        FeatureType featureType
    ) {
        this.client = client;
        this.threadPool = threadPool;
        refreshInterval = RULE_SYNC_REFRESH_INTERVAL_SETTING.get(settings);
        this.parser = parser;
        this.ruleProcessingService = ruleProcessingService;
        this.featureType = featureType;
        clusterSettings.addSettingsUpdateConsumer(RULE_SYNC_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
    }

    void doRun() {
        clearRulesFromInMemoryService();
        client.prepareSearch(WorkloadManagementPlugin.INDEX_NAME).execute(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Arrays.stream(searchResponse.getHits().getHits())
                    .forEach(hit -> { ruleProcessingService.add(parser.parse(hit.getSourceAsString())); });
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("Failed to refresh rules from in-memory service.");
            }
        });
    }

    private void clearRulesFromInMemoryService() {
        final AttributeValueStoreFactory attributeValueStoreFactory = ruleProcessingService.getAttributeValueStoreFactory();
        featureType.getAllowedAttributesRegistry().values().stream().forEach(attribute -> {
            final AttributeValueStore<String, String> attributeValueStore = attributeValueStoreFactory.getAttributeValueStore(attribute);
            if (attributeValueStore != null) {
                attributeValueStore.clear();
            }
        });
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(
            this::doRun,
            TimeValue.timeValueMillis(refreshInterval),
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    /**
     * sets the refresh interval for the rule sync mechanism
     * @param refreshInterval
     */
    public void setRefreshInterval(long refreshInterval) {
        if (refreshInterval < MIN_SYNC_REFRESH_INTERVAL) {
            logger.warn("Refresh interval must be at least 1000ms. Neglecting this change");
            throw new IllegalArgumentException("Refresh interval must be at least 1000ms");
        }
        this.refreshInterval = refreshInterval;
    }
}
