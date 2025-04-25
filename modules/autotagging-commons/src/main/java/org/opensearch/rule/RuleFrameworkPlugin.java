/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.action.TransportDeleteRuleAction;
import org.opensearch.rule.action.TransportGetRuleAction;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.rest.RestDeleteRuleAction;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.action.TransportCreateRuleAction;
import org.opensearch.rule.action.TransportGetRuleAction;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.rest.RestCreateRuleAction;
import org.opensearch.rule.rest.RestGetRuleAction;
import org.opensearch.rule.spi.RuleFrameworkExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * This plugin provides the central APIs which can provide CRUD support to all consumers of Rule framework
 * Plugins that define custom rule logic must implement {@link RuleFrameworkExtension}, which ensures
 * their feature types and persistence services are automatically registered and available to the Rule Framework.
 */
public class RuleFrameworkPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    /**
     * constructor for RuleFrameworkPlugin
     */
    public RuleFrameworkPlugin() {}

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry = new RulePersistenceServiceRegistry();
    private final List<RuleFrameworkExtension> ruleFrameworkExtensions = new ArrayList<>();

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        // We are consuming the extensions at this place to ensure that the RulePersistenceService is initialised
        ruleFrameworkExtensions.forEach(this::consumeFrameworkExtension);
        return List.of(
            new ActionPlugin.ActionHandler<>(GetRuleAction.INSTANCE, TransportGetRuleAction.class),
            new ActionPlugin.ActionHandler<>(DeleteRuleAction.INSTANCE, TransportDeleteRuleAction.class),
            new ActionPlugin.ActionHandler<>(CreateRuleAction.INSTANCE, TransportCreateRuleAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestGetRuleAction(), new RestDeleteRuleAction(), new RestCreateRuleAction());
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(b -> { b.bind(RulePersistenceServiceRegistry.class).toInstance(rulePersistenceServiceRegistry); });
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        ruleFrameworkExtensions.addAll(loader.loadExtensions(RuleFrameworkExtension.class));
    }

    private void consumeFrameworkExtension(RuleFrameworkExtension ruleFrameworkExtension) {
        AutoTaggingRegistry.registerFeatureType(ruleFrameworkExtension.getFeatureType());
        rulePersistenceServiceRegistry.register(
            ruleFrameworkExtension.getFeatureType(),
            ruleFrameworkExtension.getRulePersistenceServiceSupplier().get()
        );
    }
}
