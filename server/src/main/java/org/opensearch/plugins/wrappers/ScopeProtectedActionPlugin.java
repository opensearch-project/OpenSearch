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

 package org.opensearch.plugins;

 import org.opensearch.action.ActionType;
 import org.opensearch.action.ActionRequest;
 import org.opensearch.action.ActionResponse;
 import org.opensearch.action.RequestValidators;
 import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
 import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
 import org.opensearch.action.support.ActionFilter;
 import org.opensearch.action.support.TransportAction;
 import org.opensearch.action.support.TransportActions;
 import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
 import org.opensearch.cluster.node.DiscoveryNodes;
 import org.opensearch.common.Strings;
 import org.opensearch.common.settings.ClusterSettings;
 import org.opensearch.common.settings.IndexScopedSettings;
 import org.opensearch.common.settings.Settings;
 import org.opensearch.common.settings.SettingsFilter;
 import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.RestController;
 import org.opensearch.rest.RestHandler;
 import org.opensearch.rest.RestHeaderDefinition;
 
 import java.util.Collection;
 import java.util.Collections;
 import java.util.List;
 import java.util.Objects;
 import java.util.function.Supplier;
 import java.util.function.UnaryOperator;
 import java.util.stream.Collectors;
 
/**
 * Only allowed plugins are able able to response
 * 
 * @opensearch.experimental
 */
public class ScopeProtectedActionPlugin implements ActionPlugin {
    private final ActionPlugin plugin;
    private final IdentityService identity;

    public ScopeProtectedActionPlugin(final ActionPlugin plugin, final IdentityService identity) {
        this.plugin = plugin;
        this.identity = identity;
    }

    private throwIfNotAllowed() {
        if (!identity.getInstance().getSubject().isAllowed(List.of(ExtensionPointScopes.ActionPlugin))) {
            throw new ExtensionPointScopeException(ExtensionPointScopes.ActionPlugin);
        }
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        throwIfNotAllowed();
        return plugin.getActions();
    }

    public List<ActionType<? extends ActionResponse>> getClientActions() {
        throwIfNotAllowed();
        return plugin.getClientActions();
    }

    public List<ActionFilter> getActionFilters() {
        throwIfNotAllowed();
        return plugin.getClientActions();
    }

    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        throwIfNotAllowed();
        return plugin.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter, indexNameExpressionResolver, nodesInCluster);
    }

    public Collection<RestHeaderDefinition> getRestHeaders() {
        throwIfNotAllowed();
        return plugin.getRestHeaders();
    }

    public Collection<String> getTaskHeaders() {
        throwIfNotAllowed();
        return plugin.getRestHeaders();
    }

    public UnaryOperator<RestHandler> getRestHandlerWrapper(final ThreadContext threadContext) {
        throwIfNotAllowed();
        return plugin.getRestHandlerWrapper(threadContext);

    }

    public Collection<RequestValidators.RequestValidator<PutMappingRequest>> mappingRequestValidators() {
        throwIfNotAllowed();
        return plugin.mappingRequestValidators();

    }

    public Collection<RequestValidators.RequestValidator<IndicesAliasesRequest>> indicesAliasesRequestValidators() {
        throwIfNotAllowed();
        return plugin.indicesAliasesRequestValidators();
    }

}
