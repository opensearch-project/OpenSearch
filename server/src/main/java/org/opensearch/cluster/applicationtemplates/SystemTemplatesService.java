/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service class to orchestrate execution around available templates' management.
 */
@ExperimentalApi
public class SystemTemplatesService implements LocalNodeClusterManagerListener {

    public static final Setting<Boolean> SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED = Setting.boolSetting(
        "cluster.application_templates.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final List<SystemTemplatesPlugin> systemTemplatesPluginList;
    private final ThreadPool threadPool;

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private volatile boolean enabledTemplates;

    private volatile Stats latestStats;

    private static final Logger logger = LogManager.getLogger(SystemTemplatesService.class);

    public SystemTemplatesService(
        List<SystemTemplatesPlugin> systemTemplatesPluginList,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.systemTemplatesPluginList = systemTemplatesPluginList;
        this.threadPool = threadPool;
        setEnabledTemplates(settings.getAsBoolean(SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), false));
        clusterSettings.addSettingsUpdateConsumer(SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED, this::setEnabledTemplates);
    }

    @Override
    public void onClusterManager() {
        threadPool.generic().execute(() -> refreshTemplates(false));
    }

    @Override
    public void offClusterManager() {
        // do nothing
    }

    public void verifyRepositories() {
        refreshTemplates(true);
    }

    public Stats stats() {
        return latestStats;
    }

    void refreshTemplates(boolean verification) {
        int templatesLoaded = 0;
        int failedLoadingTemplates = 0;
        int failedLoadingRepositories = 0;
        List<Exception> exceptions = new ArrayList<>();

        if (loaded.compareAndSet(false, true) && enabledTemplates) {
            for (SystemTemplatesPlugin plugin : systemTemplatesPluginList) {
                try (SystemTemplateRepository repository = plugin.loadRepository()) {

                    final TemplateRepositoryMetadata repositoryMetadata = repository.metadata();
                    logger.debug(
                        "Loading templates from repository: {} at version {}",
                        repositoryMetadata.id(),
                        repositoryMetadata.version()
                    );

                    for (SystemTemplateMetadata templateMetadata : repository.listTemplates()) {
                        try {
                            final SystemTemplate template = repository.getTemplate(templateMetadata);

                            // Load plugin if not in verification phase.
                            if (!verification && plugin.loaderFor(templateMetadata).loadTemplate(template)) {
                                templatesLoaded++;
                            }

                        } catch (Exception ex) {
                            exceptions.add(ex);
                            logger.error(
                                "Failed loading template  {} from repository: {}",
                                templateMetadata.fullyQualifiedName(),
                                repositoryMetadata.id(),
                                ex
                            );
                            failedLoadingTemplates++;
                        }
                    }
                } catch (Exception ex) {
                    exceptions.add(ex);
                    failedLoadingRepositories++;
                    logger.error("Failed loading repository from plugin: {}", plugin.getClass().getName(), ex);
                }
            }

            logger.debug(
                "Stats: Total Loaded Templates: [{}], Failed Loading Templates: [{}], Failed Loading Repositories: [{}]",
                templatesLoaded,
                failedLoadingTemplates,
                failedLoadingRepositories
            );

            // End exceptionally if invoked in verification context
            if (verification && (failedLoadingRepositories > 0 || failedLoadingTemplates > 0)) {
                latestStats = new Stats(templatesLoaded, failedLoadingTemplates, failedLoadingRepositories);
                throw new IllegalStateException("Some of the repositories could not be loaded or are corrupted: " + exceptions);
            }
        }

        latestStats = new Stats(templatesLoaded, failedLoadingTemplates, failedLoadingRepositories);
    }

    private void setEnabledTemplates(boolean enabled) {
        if (!FeatureFlags.isEnabled(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING)) {
            throw new IllegalArgumentException(
                "Application Based Configuration Templates is under an experimental feature and can be activated only by enabling "
                    + FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING.getKey()
                    + " feature flag."
            );
        }
        enabledTemplates = enabled;
    }

    @ExperimentalApi
    public static class Stats {
        private final long templatesLoaded;
        private final long failedLoadingTemplates;
        private final long failedLoadingRepositories;

        public Stats(long templatesLoaded, long failedLoadingTemplates, long failedLoadingRepositories) {
            this.templatesLoaded = templatesLoaded;
            this.failedLoadingTemplates = failedLoadingTemplates;
            this.failedLoadingRepositories = failedLoadingRepositories;
        }

        public long getTemplatesLoaded() {
            return templatesLoaded;
        }

        public long getFailedLoadingTemplates() {
            return failedLoadingTemplates;
        }

        public long getFailedLoadingRepositories() {
            return failedLoadingRepositories;
        }
    }
}
