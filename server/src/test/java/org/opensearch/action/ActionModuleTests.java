/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action;

import java.util.ArrayList;
import org.opensearch.action.main.MainAction;
import org.opensearch.action.main.TransportMainAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.IdentityService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ActionPlugin.ActionHandler;

import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.action.RestMainAction;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;

public class ActionModuleTests extends OpenSearchTestCase {
    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(
            ActionModule.setupActions(emptyList()),
            hasEntry(MainAction.INSTANCE.name(), new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class))
        );
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class));
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(singletonList(dupsMainAction)));
        assertEquals("action for name [" + MainAction.NAME + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
        class FakeTransportAction extends TransportAction<FakeRequest, ActionResponse> {
            protected FakeTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
                super(actionName, actionFilters, taskManager);
            }

            @Override
            protected void doExecute(Task task, FakeRequest request, ActionListener<ActionResponse> listener) {}
        }
        class FakeAction extends ActionType<ActionResponse> {
            protected FakeAction() {
                super("fake", null);
            }
        }
        FakeAction action = new FakeAction();
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(action, FakeTransportAction.class));
            }
        };
        assertThat(
            ActionModule.setupActions(singletonList(registersFakeAction)),
            hasEntry("fake", new ActionHandler<>(action, FakeTransportAction.class))
        );
    }

    public void testSetupRestHandlerContainsKnownBuiltin() throws IOException {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        UsageService usageService = new UsageService();
        ActionModule actionModule = new ActionModule(
            settings.getSettings(),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            settings.getIndexScopedSettings(),
            settings.getClusterSettings(),
            settings.getSettingsFilter(),
            null,
            emptyList(),
            null,
            null,
            usageService,
            null,
            new IdentityService(Settings.EMPTY, new ArrayList<>()),
            new ExtensionsManager(Set.of())
        );
        actionModule.initRestHandlers(null);
        // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> actionModule.getRestController().registerHandler(new RestHandler() {
                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}

                @Override
                public List<Route> routes() {
                    return singletonList(new Route(Method.GET, "/"));
                }
            })
        );
        assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() throws IOException {
        ActionPlugin dupsMainAction = new ActionPlugin() {
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
                return singletonList(new RestMainAction() {

                    @Override
                    public String getName() {
                        return "duplicated_" + super.getName();
                    }

                });
            }
        };
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule = new ActionModule(
                settings.getSettings(),
                new IndexNameExpressionResolver(threadPool.getThreadContext()),
                settings.getIndexScopedSettings(),
                settings.getClusterSettings(),
                settings.getSettingsFilter(),
                threadPool,
                singletonList(dupsMainAction),
                null,
                null,
                usageService,
                null,
                null,
                null
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testPluginCanRegisterRestHandler() {
        class FakeHandler implements RestHandler {
            @Override
            public List<Route> routes() {
                return singletonList(new Route(Method.GET, "/_dummy"));
            }

            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}
        }
        ActionPlugin registersFakeHandler = new ActionPlugin() {
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
                return singletonList(new FakeHandler());
            }
        };

        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule = new ActionModule(
                settings.getSettings(),
                new IndexNameExpressionResolver(threadPool.getThreadContext()),
                settings.getIndexScopedSettings(),
                settings.getClusterSettings(),
                settings.getSettingsFilter(),
                threadPool,
                singletonList(registersFakeHandler),
                null,
                null,
                usageService,
                null,
                null,
                null
            );
            actionModule.initRestHandlers(null);
            // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> actionModule.getRestController().registerHandler(new RestHandler() {
                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}

                    @Override
                    public List<Route> routes() {
                        return singletonList(new Route(Method.GET, "/_dummy"));
                    }
                })
            );
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_dummy] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }
}
