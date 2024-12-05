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

package org.opensearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Base handler for REST requests.
 * <p>
 * This handler makes sure that the headers &amp; context of the handled {@link RestRequest requests} are copied over to
 * the transport requests executed by the associated client. While the context is fully copied over, not all the headers
 * are copied, but a selected few. It is possible to control what headers are copied over by returning them in
 * {@link ActionPlugin#getRestHeaders()}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class BaseRestHandler implements RestHandler {

    public static final Setting<Boolean> MULTI_ALLOW_EXPLICIT_INDEX = Setting.boolSetting(
        "rest.action.multi.allow_explicit_index",
        true,
        Property.NodeScope
    );

    private final LongAdder usageCount = new LongAdder();
    /**
     * @deprecated declare your own logger.
     */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    public final long getUsageCount() {
        return usageCount.sum();
    }

    /**
     * @return the name of this handler. The name should be human readable and
     *         should describe the action that will performed when this API is
     *         called. This name is used in the response to the
     *         {@link RestNodesUsageAction}.
     */
    public abstract String getName();

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        // prepare the request for execution; has the side effect of touching the request parameters
        final RestChannelConsumer action = prepareRequest(request, client);

        // validate unconsumed params, but we must exclude params used to format the response
        // use a sorted set so the unconsumed parameters appear in a reliable sorted order
        final SortedSet<String> unconsumedParams = request.unconsumedParams()
            .stream()
            .filter(p -> !responseParams().contains(p))
            .collect(Collectors.toCollection(TreeSet::new));

        // validate the non-response params
        if (!unconsumedParams.isEmpty()) {
            final Set<String> candidateParams = new HashSet<>();
            candidateParams.addAll(request.consumedParams());
            candidateParams.addAll(responseParams());
            throw new IllegalArgumentException(unrecognized(request, unconsumedParams, candidateParams, "parameter"));
        }

        usageCount.increment();
        // execute the action
        action.accept(channel);
    }

    public static String unrecognizedStrings(
        final RestRequest request,
        final Set<String> invalids,
        final Set<String> candidates,
        final String detail
    ) {
        StringBuilder message = new StringBuilder(
            String.format(Locale.ROOT, "request [%s] contains unrecognized %s%s: ", request.path(), detail, invalids.size() > 1 ? "s" : "")
        );
        boolean first = true;
        for (final String invalid : invalids) {
            final LevenshteinDistance ld = new LevenshteinDistance();
            final List<Tuple<Float, String>> scoredParams = new ArrayList<>();
            for (final String candidate : candidates) {
                final float distance = ld.getDistance(invalid, candidate);
                if (distance > 0.5f) {
                    scoredParams.add(new Tuple<>(distance, candidate));
                }
            }
            CollectionUtil.timSort(scoredParams, (a, b) -> {
                // sort by distance in reverse order, then parameter name for equal distances
                int compare = a.v1().compareTo(b.v1());
                if (compare != 0) return -compare;
                else return a.v2().compareTo(b.v2());
            });
            if (first == false) {
                message.append(", ");
            }
            message.append("[").append(invalid).append("]");
            final List<String> keys = scoredParams.stream().map(Tuple::v2).collect(Collectors.toList());
            if (keys.isEmpty() == false) {
                message.append(" -> did you mean ");
                if (keys.size() == 1) {
                    message.append("[").append(keys.get(0)).append("]");
                } else {
                    message.append("any of ").append(keys.toString());
                }
                message.append("?");
            }
            first = false;
        }

        return message.toString();
    }

    /**
     * Returns a String message of the detail of any unrecognized error occurred. The string is intended for use in error messages to be returned to the user.
     *
     * @param request The request that caused the exception
     * @param invalids Strings from the request which were unable to be understood.
     * @param candidates A set of words that are most likely to be the valid strings determined invalid, to be suggested to the user.
     * @param detail The parameter contains the details of the exception.
     * @return a String that contains the message.
     */
    protected final String unrecognized(
        final RestRequest request,
        final Set<String> invalids,
        final Set<String> candidates,
        final String detail
    ) {
        return unrecognizedStrings(request, invalids, candidates, detail);
    }

    /**
     * REST requests are handled by preparing a channel consumer that represents the execution of
     * the request against a channel.
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "1.0.0")
    protected interface RestChannelConsumer extends CheckedConsumer<RestChannel, Exception> {}

    /**
     * Streaming REST requests are handled by preparing a streaming channel consumer that represents the execution of
     * the request against a channel.
     *
     * @opensearch.experimental
     */
    @FunctionalInterface
    @ExperimentalApi
    protected interface StreamingRestChannelConsumer extends CheckedConsumer<StreamingRestChannel, Exception> {}

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request. However, some params are only used in processing the response;
     * implementations can override {@link BaseRestHandler#responseParams()} to indicate such
     * params.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    protected abstract RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException;

    /**
     * Parameters used for controlling the response and thus might not be consumed during
     * preparation of the request execution in
     * {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)}.
     *
     * @return a set of parameters used to control the response and thus should not trip strict
     * URL parameter checks.
     */
    protected Set<String> responseParams() {
        return Collections.emptySet();
    }

    /**
     * Parse the deprecated request parameter 'master_timeout', and add deprecated log if the parameter is used.
     * It also validates whether the two parameters 'master_timeout' and 'cluster_manager_timeout' are not assigned together.
     * The method is temporarily added in 2.0 duing applying inclusive language. Remove the method along with MASTER_ROLE.
     * @param mnr the action request
     * @param request the REST request to handle
     * @param logger the logger that logs deprecation notices
     * @param logMsgKeyPrefix the key prefix of a deprecation message to avoid duplicate messages.
     */
    public static void parseDeprecatedMasterTimeoutParameter(
        ClusterManagerNodeRequest mnr,
        RestRequest request,
        DeprecationLogger logger,
        String logMsgKeyPrefix
    ) {
        final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
            "Parameter [master_timeout] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_timeout] instead.";
        final String DUPLICATE_PARAMETER_ERROR_MESSAGE =
            "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].";
        if (request.hasParam("master_timeout")) {
            logger.deprecate(logMsgKeyPrefix + "_master_timeout_parameter", MASTER_TIMEOUT_DEPRECATED_MESSAGE);
            if (request.hasParam("cluster_manager_timeout")) {
                throw new OpenSearchParseException(DUPLICATE_PARAMETER_ERROR_MESSAGE);
            }
            mnr.clusterManagerNodeTimeout(request.paramAsTime("master_timeout", mnr.clusterManagerNodeTimeout()));
        }
    }

    /**
     * A wrapper for the base handler.
     *
     * @opensearch.internal
     */
    public static class Wrapper extends BaseRestHandler {

        protected final BaseRestHandler delegate;

        public Wrapper(BaseRestHandler delegate) {
            this.delegate = Objects.requireNonNull(delegate, "BaseRestHandler delegate can not be null");
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public List<Route> routes() {
            return delegate.routes();
        }

        @Override
        public List<DeprecatedRoute> deprecatedRoutes() {
            return delegate.deprecatedRoutes();
        }

        @Override
        public List<ReplacedRoute> replacedRoutes() {
            return delegate.replacedRoutes();
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            return delegate.prepareRequest(request, client);
        }

        @Override
        protected Set<String> responseParams() {
            return delegate.responseParams();
        }

        @Override
        public boolean canTripCircuitBreaker() {
            return delegate.canTripCircuitBreaker();
        }

        @Override
        public boolean supportsContentStream() {
            return delegate.supportsContentStream();
        }

        @Override
        public boolean allowsUnsafeBuffers() {
            return delegate.allowsUnsafeBuffers();
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return delegate.allowSystemIndexAccessByDefault();
        }

        @Override
        public boolean supportsStreaming() {
            return delegate.supportsStreaming();
        }
    }

    /**
     * Return a task immediately when executing some long-running operations asynchronously, like reindex, resize, open, force merge
     */
    public RestChannelConsumer sendTask(String nodeId, Task task) {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", nodeId + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
