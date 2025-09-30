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
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.action.TransportCreateRuleAction;
import org.opensearch.rule.action.TransportDeleteRuleAction;
import org.opensearch.rule.action.TransportGetRuleAction;
import org.opensearch.rule.action.TransportUpdateRuleAction;
import org.opensearch.rule.action.UpdateRuleAction;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.rest.RestCreateRuleAction;
import org.opensearch.rule.rest.RestDeleteRuleAction;
import org.opensearch.rule.rest.RestGetRuleAction;
import org.opensearch.rule.rest.RestUpdateRuleAction;
import org.opensearch.rule.spi.AttributesExtension;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;

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
     * The name of the thread pool dedicated to rule execution.
     */
    public static final String RULE_THREAD_POOL_NAME = "rule_serial_executor";
    /**
     * The number of threads allocated in the rule execution thread pool. This is set to 1 to ensure serial execution.
     */
    public static final int RULE_THREAD_COUNT = 1;
    /**
     * The maximum number of tasks that can be queued in the rule execution thread pool.
     */
    public static final int RULE_QUEUE_SIZE = 100;

    /**
     * constructor for RuleFrameworkPlugin
     */
    public RuleFrameworkPlugin() {}

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry = new RulePersistenceServiceRegistry();
    private final RuleRoutingServiceRegistry ruleRoutingServiceRegistry = new RuleRoutingServiceRegistry();
    private final List<RuleFrameworkExtension> ruleFrameworkExtensions = new ArrayList<>();

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        // We are consuming the extensions at this place to ensure that the RulePersistenceService is initialised
        ruleFrameworkExtensions.forEach(this::consumeFrameworkExtension);
        return List.of(
            new ActionPlugin.ActionHandler<>(GetRuleAction.INSTANCE, TransportGetRuleAction.class),
            new ActionPlugin.ActionHandler<>(DeleteRuleAction.INSTANCE, TransportDeleteRuleAction.class),
            new ActionPlugin.ActionHandler<>(CreateRuleAction.INSTANCE, TransportCreateRuleAction.class),
            new ActionPlugin.ActionHandler<>(UpdateRuleAction.INSTANCE, TransportUpdateRuleAction.class)
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
        return List.of(new RestGetRuleAction(), new RestDeleteRuleAction(), new RestCreateRuleAction(), new RestUpdateRuleAction());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(new FixedExecutorBuilder(settings, RULE_THREAD_POOL_NAME, RULE_THREAD_COUNT, RULE_QUEUE_SIZE, "rule-threadpool"));
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(RulePersistenceServiceRegistry.class).toInstance(rulePersistenceServiceRegistry);
            b.bind(RuleRoutingServiceRegistry.class).toInstance(ruleRoutingServiceRegistry);
        });
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        ruleFrameworkExtensions.addAll(loader.loadExtensions(RuleFrameworkExtension.class));
        Collection<AttributesExtension> attributesExtensions = loader.loadExtensions(AttributesExtension.class);
        List<Attribute> attributes = attributesExtensions.stream().map(AttributesExtension::getAttribute).toList();
        for (RuleFrameworkExtension ruleFrameworkExtension : ruleFrameworkExtensions) {
            ruleFrameworkExtension.setAttributes(attributes);
        }
    }

    private void consumeFrameworkExtension(RuleFrameworkExtension ruleFrameworkExtension) {
        FeatureType featureType = ruleFrameworkExtension.getFeatureTypeSupplier().get();
        AutoTaggingRegistry.registerFeatureType(featureType);
        rulePersistenceServiceRegistry.register(featureType, ruleFrameworkExtension.getRulePersistenceServiceSupplier().get());
        ruleRoutingServiceRegistry.register(featureType, ruleFrameworkExtension.getRuleRoutingServiceSupplier().get());

    }
}
