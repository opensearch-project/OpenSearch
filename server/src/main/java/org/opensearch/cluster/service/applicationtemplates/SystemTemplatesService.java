/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service class to orchestrate execution around available templates' management.
 */
public class SystemTemplatesService {

    public static final int APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT = 50;

    public static final Setting<Integer> SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD = Setting.intSetting(
        "cluster.application_templates.load",
        APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT,
        0,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final List<SystemTemplatesPlugin> systemTemplatesPluginList;

    private final Settings settings;

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private volatile int templatesToLoad = APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD_DEFAULT_COUNT;

    private static final Logger logger = LogManager.getLogger(SystemTemplatesService.class);

    public SystemTemplatesService(List<SystemTemplatesPlugin> systemTemplatesPluginList,
                                  ClusterSettings clusterSettings,
                                  Settings settings) {
        this.systemTemplatesPluginList = systemTemplatesPluginList;
        clusterSettings.addSettingsUpdateConsumer(SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD, this::setTemplatesToLoad);
        this.settings = settings;
    }

    public void refreshTemplates() {
        if (loaded.compareAndSet(false, true)) {
            if (SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD.get(settings) > 0) {
                int countOfTemplatesToLoad = SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_LOAD.get(settings);
                int templatesLoaded = 0;
                int failedLoadingTemplates = 0;
                int failedLoadingRepositories = 0;

                for (SystemTemplatesPlugin plugin : systemTemplatesPluginList) {
                    try (TemplateRepository repository = plugin.loadRepository()) {
                        TemplateRepositoryInfo repositoryInfo = repository.info();
                        logger.debug("Loading templates from repository: {} at version {}", repositoryInfo.id(), repositoryInfo.version());
                        for (SystemTemplateInfo templateInfo : repository.listTemplates()) {

                            if (templatesLoaded == countOfTemplatesToLoad) {
                                logger.debug("Not loading template: {} from repository: {} as we've breached the max count [{}] allowed",
                                    templateInfo.fullyQualifiedName(), repositoryInfo.id(), countOfTemplatesToLoad);
                                break;
                            }
                            try {
                                SystemTemplate template = repository.fetchTemplate(templateInfo);
                                plugin.loaderFor(templateInfo).loadTemplate(template);
                                countOfTemplatesToLoad--;
                            } catch (Exception ex) {
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
                        failedLoadingRepositories++;
                        logger.error("Failed loading repository from plugin: {}", plugin.getClass().getName(), ex);
                    }
                }

                logger.debug("Stats: Total Loaded Templates: [{}], Failed Loading Templates: [{}], Failed Loading Repositories: [{}]",
                    templatesLoaded, failedLoadingTemplates, failedLoadingRepositories);
            }
        }
    }

    private void setTemplatesToLoad(int templatesToLoad) {
        this.templatesToLoad = templatesToLoad;
    }
}
