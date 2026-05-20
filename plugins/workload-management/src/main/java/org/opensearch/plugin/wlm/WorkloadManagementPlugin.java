/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureValueValidator;
import org.opensearch.plugin.wlm.rule.WorkloadGroupRuleRoutingService;
import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleRoutingService;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.rule.storage.IndexBasedRuleQueryMapper;
import org.opensearch.rule.storage.XContentRuleParser;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.rule.service.IndexStoredRulePersistenceService.MAX_WLM_RULES_SETTING;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin
    implements
        ActionPlugin,
        SystemIndexPlugin,
        DiscoveryPlugin,
        ExtensiblePlugin,
        RuleFrameworkExtension {

    /**
     * The name of the index where rules are stored.
     */
    public static final String INDEX_NAME = ".wlm_rules";
    /**
     * The maximum number of rules allowed per GET request.
     */
    public static final int MAX_RULES_PER_PAGE = 50;
    /**
     * Principal attribute name.
     */
    public static final String PRINCIPAL_ATTRIBUTE_NAME = "principal";
    private static FeatureType featureType;
    private static RulePersistenceService rulePersistenceService;
    private static RuleRoutingService ruleRoutingService;
    private static final Map<Attribute, Integer> orderedAttributes = new HashMap<>();
    private WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider;
    private AutoTaggingActionFilter autoTaggingActionFilter;
    private final Map<Attribute, AttributeExtractorExtension> attributeExtractorExtensions = new HashMap<>();

    /**
     * Default constructor
     */
    public WorkloadManagementPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        wlmClusterSettingValuesProvider = new WlmClusterSettingValuesProvider(
            clusterService.getSettings(),
            clusterService.getClusterSettings()
        );
        featureType = new WorkloadGroupFeatureType(new WorkloadGroupFeatureValueValidator(clusterService), orderedAttributes);
        RuleEntityParser parser = new XContentRuleParser(featureType);
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            featureType,
            DefaultAttributeValueStore::new
        );
        InMemoryRuleProcessingService ruleProcessingService = new InMemoryRuleProcessingService(
            attributeValueStoreFactory,
            featureType.getOrderedAttributes()
        );
        rulePersistenceService = new IndexStoredRulePersistenceService(
            INDEX_NAME,
            client,
            clusterService,
            MAX_RULES_PER_PAGE,
            parser,
            new IndexBasedRuleQueryMapper()
        );
        ruleRoutingService = new WorkloadGroupRuleRoutingService(client, clusterService);

        RefreshBasedSyncMechanism refreshMechanism = new RefreshBasedSyncMechanism(
            threadPool,
            clusterService.getSettings(),
            featureType,
            rulePersistenceService,
            new RuleEventClassifier(Collections.emptySet(), ruleProcessingService),
            wlmClusterSettingValuesProvider
        );

        autoTaggingActionFilter = new AutoTaggingActionFilter(
            ruleProcessingService,
            threadPool,
            attributeExtractorExtensions,
            wlmClusterSettingValuesProvider,
            featureType
        );
        return List.of(refreshMechanism, featureType, rulePersistenceService);
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        ((WorkloadGroupRuleRoutingService) ruleRoutingService).setTransportService(transportService);
        return Collections.emptyMap();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(autoTaggingActionFilter);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateWorkloadGroupAction.INSTANCE, TransportCreateWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetWorkloadGroupAction.INSTANCE, TransportGetWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteWorkloadGroupAction.INSTANCE, TransportDeleteWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateWorkloadGroupAction.INSTANCE, TransportUpdateWorkloadGroupAction.class)
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new SystemIndexDescriptor(INDEX_NAME, "System index used for storing workload_group rules"));
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
        return List.of(
            new RestCreateWorkloadGroupAction(wlmClusterSettingValuesProvider),
            new RestGetWorkloadGroupAction(),
            new RestDeleteWorkloadGroupAction(wlmClusterSettingValuesProvider),
            new RestUpdateWorkloadGroupAction(wlmClusterSettingValuesProvider)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT,
            RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING,
            MAX_WLM_RULES_SETTING
        );
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new WorkloadManagementPluginModule());
    }

    @Override
    public Supplier<RulePersistenceService> getRulePersistenceServiceSupplier() {
        return () -> rulePersistenceService;
    }

    @Override
    public Supplier<RuleRoutingService> getRuleRoutingServiceSupplier() {
        return () -> ruleRoutingService;
    }

    @Override
    public Supplier<FeatureType> getFeatureTypeSupplier() {
        return () -> featureType;
    }

    @Override
    public void setAttributes(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(PRINCIPAL_ATTRIBUTE_NAME)) {
                orderedAttributes.put(attribute, 1);
            }
        }
    }

    public void loadExtensions(ExtensionLoader loader) {
        for (AttributeExtractorExtension ext : loader.loadExtensions(AttributeExtractorExtension.class)) {
            attributeExtractorExtensions.put(ext.getAttributeExtractor().getAttribute(), ext);
        }
    }
}
