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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.wlm.WlmClusterSettingValuesProvider;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEvent;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WlmMode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class pulls the latest rules from the RULES system index to update the in-memory view
 */
public class RefreshBasedSyncMechanism extends AbstractLifecycleComponent {
    /**
     * Setting name to control the refresh interval of synchronization service
     */
    public static final String RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME = "wlm.rule.sync_refresh_interval_ms";
    /**
     * Default value for refresh interval
     */
    public static final long RULE_SYNC_REFRESH_INTERVAL_DEFAULT_MS = 5000;
    /**
     * Minimum value for refresh interval
     */
    public static final int MIN_SYNC_REFRESH_INTERVAL_MS = 1000;

    /**
     * Setting to control the run interval of synchronization service
     */
    public static final Setting<Long> RULE_SYNC_REFRESH_INTERVAL_SETTING = Setting.longSetting(
        RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME,
        RULE_SYNC_REFRESH_INTERVAL_DEFAULT_MS,
        MIN_SYNC_REFRESH_INTERVAL_MS,
        Setting.Property.NodeScope
    );

    private final ThreadPool threadPool;
    private long refreshInterval;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final RulePersistenceService rulePersistenceService;
    private final RuleEventClassifier ruleEventClassifier;
    private final FeatureType featureType;
    private final WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;
    // This var keeps the Rules which were present during last run of this service
    private Set<Rule> lastRunIndexedRules;
    private static final Logger logger = LogManager.getLogger(RefreshBasedSyncMechanism.class);

    /**
     * Constructor
     *
     * @param threadPool
     * @param settings
     * @param featureType
     * @param rulePersistenceService
     * @param ruleEventClassifier
     * @param nonPluginSettingValuesProvider
     */
    public RefreshBasedSyncMechanism(
        ThreadPool threadPool,
        Settings settings,
        FeatureType featureType,
        RulePersistenceService rulePersistenceService,
        RuleEventClassifier ruleEventClassifier,
        WlmClusterSettingValuesProvider nonPluginSettingValuesProvider
    ) {
        this.threadPool = threadPool;
        refreshInterval = RULE_SYNC_REFRESH_INTERVAL_SETTING.get(settings);
        this.featureType = featureType;
        this.rulePersistenceService = rulePersistenceService;
        this.lastRunIndexedRules = new HashSet<>();
        this.ruleEventClassifier = ruleEventClassifier;
        this.nonPluginSettingValuesProvider = nonPluginSettingValuesProvider;
    }

    /**
     * synchronized check is needed in case two scheduled runs happen concurrently though highly improbable
     * but theoretically possible
     */
    synchronized void doRun() {
        if (nonPluginSettingValuesProvider.getWlmMode() != WlmMode.ENABLED) {
            return;
        }

        fetchAllRules();
    }

    /**
     * Fetches all rules page by page, following the {@code search_after} token until no more pages remain,
     * and only then applies the complete rule set to the in-memory view. If any page fails, the whole sync
     * run is skipped: applying a partial set would look like deletions of the rules on the missing pages.
     * Pagination is an iterative loop, not recursion: the persistence service invokes the listener on the
     * calling thread, so a recursive fetch would grow the stack by one frame per page. The loop also stops
     * if a page returns the same {@code search_after} token it was called with, so a backend that stops
     * making progress cannot spin forever.
     */
    private void fetchAllRules() {
        Set<Rule> accumulatedRules = new HashSet<>();
        String searchAfter = null;
        try {
            while (true) {
                PlainActionFuture<GetRuleResponse> future = PlainActionFuture.newFuture();
                rulePersistenceService.getRule(
                    new GetRuleRequest(null, Collections.emptyMap(), searchAfter, featureType),
                    future
                );
                GetRuleResponse response = future.actionGet();
                accumulatedRules.addAll(response.getRules());
                String nextSearchAfter = response.getSearchAfter();
                if (nextSearchAfter == null) {
                    break;
                }
                if (nextSearchAfter.equals(searchAfter)) {
                    logger.warn("search_after token [{}] did not advance, stopping rule sync", nextSearchAfter);
                    break;
                }
                searchAfter = nextSearchAfter;
            }
        } catch (IndexNotFoundException e) {
            logger.debug("Rule index not found, skipping rule processing.");
            return;
        } catch (Exception e) {
            logger.warn("Failed to get rules from persistence service, skipping this sync run", e);
            return;
        }

        ruleEventClassifier.setPreviousRules(lastRunIndexedRules);

        ruleEventClassifier.getRuleEvents(accumulatedRules).forEach(RuleEvent::process);

        lastRunIndexedRules = accumulatedRules;
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
}
