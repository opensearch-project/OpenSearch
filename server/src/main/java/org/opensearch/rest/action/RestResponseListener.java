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

import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;

/**
 * A REST enabled action listener that has a basic onFailure implementation, and requires
 * sub classes to only implement {@link #buildResponse(Object)}.
 *
 * @opensearch.api
 */
public abstract class RestResponseListener<Response> extends RestActionListener<Response> {

    protected RestResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected final void processResponse(Response response) throws Exception {
        channel.sendResponse(buildResponse(response));
    }

    /**
     * Builds the response to send back through the channel.
     */
    public abstract RestResponse buildResponse(Response response) throws Exception;
}
