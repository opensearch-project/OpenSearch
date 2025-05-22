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
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

public class WorkloadManagementPluginTests extends OpenSearchTestCase {
    WorkloadManagementPlugin plugin = new WorkloadManagementPlugin();

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
            mock(ClusterService.class),
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
        FeatureType featureType = plugin.getFeatureType();
        assertEquals("workload_group", featureType.getName());
    }

    public void testGetSettingsIncludesMaxQueryGroupCount() {
        List<?> settings = plugin.getSettings();
        assertTrue(settings.contains(WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT));
    }

    public void testCreateGuiceModulesReturnsModule() {
        Collection<?> modules = plugin.createGuiceModules();
        assertEquals(1, modules.size());
        assertTrue(modules.iterator().next() instanceof WorkloadManagementPluginModule);
    }
}
