/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.main.MainAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionAction;
import org.opensearch.extensions.action.ExtensionTransportAction;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.extensions.rest.RestSendToExtensionAction;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class DynamicActionRegistryTests extends OpenSearchTestCase {

    public void testDynamicActionRegistry() {
        ActionFilters emptyFilters = new ActionFilters(Collections.emptySet());
        Map<ActionType, TransportAction> testMap = Map.of(TestAction.INSTANCE, new TestTransportAction("test-action", emptyFilters, null));

        DynamicActionRegistry dynamicActionRegistry = new DynamicActionRegistry();
        dynamicActionRegistry.registerUnmodifiableActionMap(testMap);

        // Should contain the immutable map entry
        assertNotNull(dynamicActionRegistry.get(TestAction.INSTANCE));
        // Should not contain anything not added
        assertNull(dynamicActionRegistry.get(MainAction.INSTANCE));

        // ExtensionsAction not yet registered
        ExtensionAction testExtensionAction = new ExtensionAction("extensionId", "actionName");
        ExtensionTransportAction testExtensionTransportAction = new ExtensionTransportAction("test-action", emptyFilters, null, null);
        assertNull(dynamicActionRegistry.get(testExtensionAction));

        // Register an extension action
        // Should insert without problem
        try {
            dynamicActionRegistry.registerDynamicAction(testExtensionAction, testExtensionTransportAction);
        } catch (Exception e) {
            fail("Should not have thrown exception registering action: " + e);
        }
        assertEquals(testExtensionTransportAction, dynamicActionRegistry.get(testExtensionAction));

        // Should fail inserting twice
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> dynamicActionRegistry.registerDynamicAction(testExtensionAction, testExtensionTransportAction)
        );
        assertEquals("action [actionName] already registered", ex.getMessage());
        // Should remove without problem
        try {
            dynamicActionRegistry.unregisterDynamicAction(testExtensionAction);
        } catch (Exception e) {
            fail("Should not have thrown exception unregistering action: " + e);
        }
        // Should have been removed
        assertNull(dynamicActionRegistry.get(testExtensionAction));

        // Should fail removing twice
        ex = assertThrows(IllegalArgumentException.class, () -> dynamicActionRegistry.unregisterDynamicAction(testExtensionAction));
        assertEquals("action [actionName] was not registered", ex.getMessage());
    }

    public void testDynamicActionRegistryWithNamedRoutes() {
        RestSendToExtensionAction action = mock(RestSendToExtensionAction.class);
        RestSendToExtensionAction action2 = mock(RestSendToExtensionAction.class);
        NamedRoute r1 = new NamedRoute.Builder().method(RestRequest.Method.GET).path("/foo").uniqueName("foo").build();
        NamedRoute r2 = new NamedRoute.Builder().method(RestRequest.Method.PUT).path("/bar").uniqueName("bar").build();

        DynamicActionRegistry registry = new DynamicActionRegistry();
        registry.registerDynamicRoute(r1, action);
        registry.registerDynamicRoute(r2, action2);

        assertTrue(registry.isActionRegistered("foo"));
        assertTrue(registry.isActionRegistered("bar"));

        registry.unregisterDynamicRoute(r2);

        assertTrue(registry.isActionRegistered("foo"));
        assertFalse(registry.isActionRegistered("bar"));
    }

    public void testDynamicActionRegistryWithNamedRoutesAndLegacyActionNames() {
        RestSendToExtensionAction action = mock(RestSendToExtensionAction.class);
        RestSendToExtensionAction action2 = mock(RestSendToExtensionAction.class);
        NamedRoute r1 = new NamedRoute.Builder().method(RestRequest.Method.GET)
            .path("/foo")
            .uniqueName("foo")
            .legacyActionNames(Set.of("cluster:admin/opensearch/abc/foo"))
            .build();
        NamedRoute r2 = new NamedRoute.Builder().method(RestRequest.Method.PUT)
            .path("/bar")
            .uniqueName("bar")
            .legacyActionNames(Set.of("cluster:admin/opensearch/xyz/bar"))
            .build();

        DynamicActionRegistry registry = new DynamicActionRegistry();
        registry.registerDynamicRoute(r1, action);
        registry.registerDynamicRoute(r2, action2);

        assertTrue(registry.isActionRegistered("cluster:admin/opensearch/abc/foo"));
        assertTrue(registry.isActionRegistered("cluster:admin/opensearch/xyz/bar"));

        registry.unregisterDynamicRoute(r2);

        assertTrue(registry.isActionRegistered("cluster:admin/opensearch/abc/foo"));
        assertFalse(registry.isActionRegistered("cluster:admin/opensearch/xyz/bar"));
    }

    private static final class TestAction extends ActionType<ActionResponse> {
        public static final TestAction INSTANCE = new TestAction();

        private TestAction() {
            super("test-action", new Writeable.Reader<ActionResponse>() {
                @Override
                public ActionResponse read(StreamInput in) throws IOException {
                    return null;
                }
            });
        }
    };

    private static final class TestTransportAction extends TransportAction<ActionRequest, ActionResponse> {
        protected TestTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
            super(actionName, actionFilters, taskManager);
        }

        @Override
        protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {}
    }
}
