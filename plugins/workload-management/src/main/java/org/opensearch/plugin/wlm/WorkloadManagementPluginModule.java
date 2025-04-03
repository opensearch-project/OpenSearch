/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Singleton;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;;
import org.opensearch.plugin.wlm.rule.service.WlmRulePersistenceService;
import org.opensearch.plugin.wlm.rule.service.WlmRuleProcessingService;
import org.opensearch.plugin.wlm.rule.service.WlmRuleResponseBuilder;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.rule.service.RulePersistenceService;

/**
 * Guice Module to manage WorkloadManagement related objects
 */
public class WorkloadManagementPluginModule extends AbstractModule {

    /**
     * Constructor for WorkloadManagementPluginModule
     */
    public WorkloadManagementPluginModule() {}

    protected void configure() {
        // Bind WorkloadGroupPersistenceService as a singleton to ensure a single instance is used,
        // preventing multiple throttling key registrations in the constructor.
        bind(WorkloadGroupPersistenceService.class).in(Singleton.class);
        bind(RulePersistenceService.class).to(WlmRulePersistenceService.class).in(Singleton.class);
        bind(RuleProcessingService.class).to(WlmRuleProcessingService.class).in(Singleton.class);
        bind(RuleResponseBuilder.class).to(WlmRuleResponseBuilder.class).in(Singleton.class);
        bind(RuleService.class).in(Singleton.class);
        bind(RulePersistenceService.class).to(IndexStoredRulePersistenceService.class);
    }
}
