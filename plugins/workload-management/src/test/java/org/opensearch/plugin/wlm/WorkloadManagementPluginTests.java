/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WorkloadManagementPluginTests extends OpenSearchTestCase {
    WorkloadManagementPlugin plugin = new WorkloadManagementPlugin();
    ClusterService mockClusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(plugin.getSettings()));
        clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(mockClusterService.getSettings()).thenReturn(settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testGetActionsReturnsHandlers() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = plugin.getActions();
        assertEquals(4, actions.size());
        assertEquals(CreateWorkloadGroupAction.INSTANCE.name(), actions.get(0).getAction().name());
    }

    public void testGetRestHandlersReturnsHandlers() {
        List<RestHandler> handlers = plugin.getRestHandlers(
            Settings.EMPTY,
            mock(RestController.class),
            null,
            null,
            null,
            null,
            () -> mock(DiscoveryNodes.class)
        );

        assertEquals(4, handlers.size());
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestCreateWorkloadGroupAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestGetWorkloadGroupAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestDeleteWorkloadGroupAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestUpdateWorkloadGroupAction));
    }

    public void testCreateComponentsInitializesRulePersistenceService() {
        Client mockClient = mock(Client.class);
        plugin.createComponents(
            mockClient,
            mockClusterService,
            mock(ThreadPool.class),
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            mock(NamedXContentRegistry.class),
            mock(Environment.class),
            null,
            mock(NamedWriteableRegistry.class),
            mock(IndexNameExpressionResolver.class),
            () -> mock(RepositoriesService.class)
        );

        RulePersistenceService service = plugin.getRulePersistenceServiceSupplier().get();
        assertNotNull(service);
        assertTrue(service instanceof IndexStoredRulePersistenceService);
    }

    public void testGetSystemIndexDescriptorsReturnsCorrectDescriptor() {
        Collection<SystemIndexDescriptor> descriptors = plugin.getSystemIndexDescriptors(Settings.EMPTY);
        assertEquals(1, descriptors.size());
        SystemIndexDescriptor descriptor = descriptors.iterator().next();
        assertEquals(".wlm_rules", descriptor.getIndexPattern());
    }

    public void testGetFeatureTypeReturnsWorkloadGroupFeatureType() {
        plugin.createComponents(
            mock(Client.class),
            mockClusterService,
            mock(ThreadPool.class),
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            mock(NamedXContentRegistry.class),
            mock(Environment.class),
            null,
            mock(NamedWriteableRegistry.class),
            mock(IndexNameExpressionResolver.class),
            () -> mock(RepositoriesService.class)
        );
        FeatureType featureType = plugin.getFeatureTypeSupplier().get();
        assertEquals("workload_group", featureType.getName());
    }

    public void testGetSettingsIncludesMaxWorkloadGroupCount() {
        List<?> settings = plugin.getSettings();
        assertTrue(settings.contains(WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT));
    }

    public void testCreateGuiceModulesReturnsModule() {
        Collection<?> modules = plugin.createGuiceModules();
        assertEquals(1, modules.size());
        assertTrue(modules.iterator().next() instanceof WorkloadManagementPluginModule);
    }

    /**
     * Test case for createComponents method.
     * This test verifies that the createComponents method returns a collection
     * containing a single RefreshBasedSyncMechanism instance.
     */
    public void testCreateComponentsReturnsRefreshMechanism() {
        Client mockClient = mock(Client.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        ResourceWatcherService mockResourceWatcherService = mock(ResourceWatcherService.class);
        ScriptService mockScriptService = mock(ScriptService.class);
        NamedXContentRegistry mockNamedXContentRegistry = mock(NamedXContentRegistry.class);
        Environment mockEnvironment = mock(Environment.class);
        NamedWriteableRegistry mockNamedWriteableRegistry = mock(NamedWriteableRegistry.class);
        IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        Supplier<RepositoriesService> mockRepositoriesServiceSupplier = () -> mock(RepositoriesService.class);

        Collection<Object> components = plugin.createComponents(
            mockClient,
            mockClusterService,
            mockThreadPool,
            mockResourceWatcherService,
            mockScriptService,
            mockNamedXContentRegistry,
            mockEnvironment,
            null,
            mockNamedWriteableRegistry,
            mockIndexNameExpressionResolver,
            mockRepositoriesServiceSupplier
        );

        assertThat(components.stream().filter(c -> c instanceof RefreshBasedSyncMechanism).count(), equalTo(1L));
    }

    public void testSetAttributesWithMock() {
        WorkloadManagementPlugin plugin = mock(WorkloadManagementPlugin.class);
        Attribute attribute = mock(Attribute.class);
        when(attribute.getName()).thenReturn(PRINCIPAL_ATTRIBUTE_NAME);
        plugin.setAttributes(List.of(attribute));
        verify(plugin, times(1)).setAttributes(List.of(attribute));
    }

    @SuppressWarnings("unchecked")
    public void testLoadExtensionsWithMock() {
        WorkloadManagementPlugin plugin = spy(new WorkloadManagementPlugin());
        ExtensiblePlugin.ExtensionLoader loader = mock(ExtensiblePlugin.ExtensionLoader.class);
        AttributeExtractor<String> extractor = mock(AttributeExtractor.class);
        Attribute attribute = mock(Attribute.class);
        AttributeExtractorExtension extension = mock(AttributeExtractorExtension.class);

        when(attribute.getName()).thenReturn("mock_attr");
        when(extractor.getAttribute()).thenReturn(attribute);
        when(extension.getAttributeExtractor()).thenReturn(extractor);
        when(loader.loadExtensions(AttributeExtractorExtension.class)).thenReturn(List.of(extension));

        plugin.loadExtensions(loader);

        verify(loader, times(1)).loadExtensions(AttributeExtractorExtension.class);
        verify(extension, times(1)).getAttributeExtractor();
        verify(extractor, times(1)).getAttribute();
    }
}
