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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service class to orchestrate execution around available templates' management.
 */
@ExperimentalApi
public class SystemTemplatesService implements LocalNodeClusterManagerListener {

    public static final int APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT = 50;

    public static final Setting<Integer> SETTING_APPLICATION_BASED_CONFIGURATION_MAX_TEMPLATES_LOAD = Setting.intSetting(
        "cluster.application_templates.max_load",
        APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT,
        0,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED = Setting.boolSetting(
        "cluster.application_templates.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final List<SystemTemplatesPlugin> systemTemplatesPluginList;

    private final Settings settings;

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private volatile int templatesToLoad;

    private volatile boolean enabledTemplates;

    private static final Logger logger = LogManager.getLogger(SystemTemplatesService.class);

    public SystemTemplatesService(List<SystemTemplatesPlugin> systemTemplatesPluginList,
                                  ClusterSettings clusterSettings,
                                  Settings settings) {
        this.systemTemplatesPluginList = systemTemplatesPluginList;
        this.settings = settings;
        setEnabledTemplates(settings.getAsBoolean(SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), false));
        setMaxTemplatesToLoad(settings.getAsInt(SETTING_APPLICATION_BASED_CONFIGURATION_MAX_TEMPLATES_LOAD.getKey(), APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT));
        clusterSettings.addSettingsUpdateConsumer(SETTING_APPLICATION_BASED_CONFIGURATION_MAX_TEMPLATES_LOAD, this::setMaxTemplatesToLoad);
        clusterSettings.addSettingsUpdateConsumer(SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED, this::setEnabledTemplates);
    }

    @Override
    public void onClusterManager() {
        refreshTemplates(false);
    }

    @Override
    public void offClusterManager() {
        // do nothing
    }

    public void verifyRepositories() {
        refreshTemplates(true);
    }

    public void refreshTemplates(boolean verification) {
        if (loaded.compareAndSet(false, true)) {
            if (enabledTemplates && templatesToLoad > 0) {
                int countOfTemplatesToLoad = templatesToLoad;
                int templatesLoaded = 0;
                int failedLoadingTemplates = 0;
                int failedLoadingRepositories = 0;

                List<Exception> exceptions = new ArrayList<>();
                for (SystemTemplatesPlugin plugin : systemTemplatesPluginList) {
                    try (SystemTemplateRepository repository = plugin.loadRepository()) {
                        TemplateRepositoryMetadata repositoryInfo = repository.metadata();
                        logger.debug("Loading templates from repository: {} at version {}", repositoryInfo.id(), repositoryInfo.version());
                        for (SystemTemplateMetadata templateInfo : repository.listTemplates()) {

                            if (templatesLoaded == countOfTemplatesToLoad) {
                                logger.debug("Not loading template: {} from repository: {} as we've breached the max count [{}] allowed",
                                    templateInfo.fullyQualifiedName(), repositoryInfo.id(), countOfTemplatesToLoad);
                                break;
                            }
                            try {
                                SystemTemplate template = repository.getTemplate(templateInfo);

                                // Load plugin if not in verification phase.
                                if (!verification) {
                                    plugin.loaderFor(templateInfo).loadTemplate(template);
                                }
                                countOfTemplatesToLoad--;
                            } catch (Exception ex) {
                                exceptions.add(ex);
                                logger.error("Failed loading template  {} from repository: {}",
                                    templateInfo.fullyQualifiedName(), repositoryInfo.id(), ex);
                                failedLoadingTemplates++;
                            }
                        }

                        if (templatesLoaded == countOfTemplatesToLoad) {
                            logger.debug("Loaded maximum permitted templates");
                            break;
                        }
                    } catch (Exception ex) {
                        exceptions.add(ex);
                        failedLoadingRepositories++;
                        logger.error("Failed loading repository from plugin: {}", plugin.getClass().getName(), ex);
                    }
                }

                logger.debug("Stats: Total Loaded Templates: [{}], Failed Loading Templates: [{}], Failed Loading Repositories: [{}]",
                    templatesLoaded, failedLoadingTemplates, failedLoadingRepositories);

                if (verification) {
                    throw new IllegalStateException("Some of the repositories could not be loaded or are corrupted: " + exceptions);
                }
            }
        }
    }

    private void setMaxTemplatesToLoad(int templatesToLoad) {
        this.templatesToLoad = templatesToLoad;
    }

    private void setEnabledTemplates(boolean enabled) {
        enabledTemplates = enabled;
    }
}
