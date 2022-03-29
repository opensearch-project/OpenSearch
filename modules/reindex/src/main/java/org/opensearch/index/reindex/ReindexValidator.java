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

package org.opensearch.index.reindex;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;

class ReindexValidator {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ReindexValidator.class);
    static final String SORT_DEPRECATED_MESSAGE = "The sort option in reindex is deprecated. "
        + "Instead consider using query filtering to find the desired subset of data.";

    private final CharacterRunAutomaton remoteAllowlist;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver resolver;
    private final AutoCreateIndex autoCreateIndex;

    ReindexValidator(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver,
        AutoCreateIndex autoCreateIndex
    ) {
        this.remoteAllowlist = buildRemoteAllowlist(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings));
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.autoCreateIndex = autoCreateIndex;
    }

    void initialValidation(ReindexRequest request) {
        checkRemoteAllowlist(remoteAllowlist, request.getRemoteInfo());
        ClusterState state = clusterService.state();
        validateAgainstAliases(
            request.getSearchRequest(),
            request.getDestination(),
            request.getRemoteInfo(),
            resolver,
            autoCreateIndex,
            state
        );
        SearchSourceBuilder searchSource = request.getSearchRequest().source();
        if (searchSource != null && searchSource.sorts() != null && searchSource.sorts().isEmpty() == false) {
            deprecationLogger.deprecate("reindex_sort", SORT_DEPRECATED_MESSAGE);
        }
    }

    static void checkRemoteAllowlist(CharacterRunAutomaton allowlist, RemoteInfo remoteInfo) {
        if (remoteInfo == null) {
            return;
        }
        String check = remoteInfo.getHost() + ':' + remoteInfo.getPort();
        if (allowlist.run(check)) {
            return;
        }
        String allowListKey = TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.getKey();
        throw new IllegalArgumentException('[' + check + "] not allowlisted in " + allowListKey);
    }

    /**
     * Build the {@link CharacterRunAutomaton} that represents the reindex-from-remote allowlist and make sure that it doesn't allowlist
     * the world.
     */
    static CharacterRunAutomaton buildRemoteAllowlist(List<String> allowlist) {
        if (allowlist.isEmpty()) {
            return new CharacterRunAutomaton(Automata.makeEmpty());
        }
        Automaton automaton = Regex.simpleMatchToAutomaton(allowlist.toArray(Strings.EMPTY_ARRAY));
        automaton = MinimizationOperations.minimize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        if (Operations.isTotal(automaton)) {
            throw new IllegalArgumentException(
                "Refusing to start because allowlist "
                    + allowlist
                    + " accepts all addresses. "
                    + "This would allow users to reindex-from-remote any URL they like effectively having OpenSearch make HTTP GETs "
                    + "for them."
            );
        }
        return new CharacterRunAutomaton(automaton);
    }

    /**
     * Throws an ActionRequestValidationException if the request tries to index
     * back into the same index or into an index that points to two indexes.
     * This cannot be done during request validation because the cluster state
     * isn't available then. Package private for testing.
     */
    static void validateAgainstAliases(
        SearchRequest source,
        IndexRequest destination,
        RemoteInfo remoteInfo,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AutoCreateIndex autoCreateIndex,
        ClusterState clusterState
    ) {
        if (remoteInfo != null) {
            return;
        }
        String target = destination.index();
        if (destination.isRequireAlias() && (false == clusterState.getMetadata().hasAlias(target))) {
            throw new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + target + "] is not an alias",
                target
            );
        }
        if (false == autoCreateIndex.shouldAutoCreate(target, clusterState)) {
            /*
             * If we're going to autocreate the index we don't need to resolve
             * it. This is the same sort of dance that TransportIndexRequest
             * uses to decide to autocreate the index.
             */
            target = indexNameExpressionResolver.concreteWriteIndex(clusterState, destination).getName();
        }
        for (String sourceIndex : indexNameExpressionResolver.concreteIndexNames(clusterState, source)) {
            if (sourceIndex.equals(target)) {
                ActionRequestValidationException e = new ActionRequestValidationException();
                e.addValidationError("reindex cannot write into an index its reading from [" + target + ']');
                throw e;
            }
        }
    }
}
