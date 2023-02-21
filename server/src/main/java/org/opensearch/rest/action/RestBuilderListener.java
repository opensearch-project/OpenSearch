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

package org.opensearch.rest.action;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;

/**
 * A REST action listener that builds an {@link XContentBuilder} based response.
 *
 * @opensearch.api
 */
public abstract class RestBuilderListener<Response> extends RestResponseListener<Response> {

    public RestBuilderListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public final RestResponse buildResponse(Response response) throws Exception {
        try (XContentBuilder builder = channel.newBuilder()) {
            final RestResponse restResponse = buildResponse(response, builder);
            assert assertBuilderClosed(builder);
            return restResponse;
        }
    }

    /**
     * Builds a response to send back over the channel. Implementors should ensure that they close the provided {@link XContentBuilder}
     * using the {@link XContentBuilder#close()} method.
     */
    public abstract RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception;

    // pkg private method that we can override for testing
    boolean assertBuilderClosed(XContentBuilder xContentBuilder) {
        assert xContentBuilder.generator().isClosed() : "callers should ensure the XContentBuilder is closed themselves";
        return true;
    }
}
