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

package org.opensearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Booleans;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport handler to resize indices
 *
 * @opensearch.api
 */
public abstract class RestResizeHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestResizeHandler.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    RestResizeHandler() {}

    @Override
    public abstract String getName();

    abstract ResizeType getResizeType();

    @Override
    public final RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ResizeRequest resizeRequest = new ResizeRequest(request.param("target"), request.param("index"));
        resizeRequest.setResizeType(getResizeType());
        // copy_settings should be removed in OpenSearch 1.0.0; cf. https://github.com/elastic/elasticsearch/issues/28347
        assert Version.CURRENT.major < 8;
        final String rawCopySettings = request.param("copy_settings");
        final Boolean copySettings;
        if (rawCopySettings == null) {
            copySettings = resizeRequest.getCopySettings();
        } else {
            if (rawCopySettings.isEmpty()) {
                copySettings = true;
            } else {
                copySettings = Booleans.parseBoolean(rawCopySettings);
                if (copySettings == false) {
                    throw new IllegalArgumentException("parameter [copy_settings] can not be explicitly set to [false]");
                }
            }
            deprecationLogger.deprecate(
                "resize_deprecated_parameter",
                "parameter [copy_settings] is deprecated and will be removed in 8.0.0"
            );
        }
        resizeRequest.setCopySettings(copySettings);
        request.applyContentParser(resizeRequest::fromXContent);
        resizeRequest.timeout(request.paramAsTime("timeout", resizeRequest.timeout()));
        resizeRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", resizeRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(resizeRequest, request, deprecationLogger, getName());
        resizeRequest.setWaitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> client.admin().indices().resizeIndex(resizeRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Shrink index action.
     *
     * @opensearch.internal
     */
    public static class RestShrinkIndexAction extends RestResizeHandler {

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(new Route(POST, "/{index}/_shrink/{target}"), new Route(PUT, "/{index}/_shrink/{target}")));
        }

        @Override
        public String getName() {
            return "shrink_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.SHRINK;
        }

    }

    /**
     * Split index action.
     *
     * @opensearch.internal
     */
    public static class RestSplitIndexAction extends RestResizeHandler {

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(new Route(POST, "/{index}/_split/{target}"), new Route(PUT, "/{index}/_split/{target}")));
        }

        @Override
        public String getName() {
            return "split_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.SPLIT;
        }

    }

    /**
     * Clone index action.
     *
     * @opensearch.internal
     */
    public static class RestCloneIndexAction extends RestResizeHandler {

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(new Route(POST, "/{index}/_clone/{target}"), new Route(PUT, "/{index}/_clone/{target}")));
        }

        @Override
        public String getName() {
            return "clone_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.CLONE;
        }

    }

}
