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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;

import java.io.IOException;

/**
 * An action listener that requires {@link #processResponse(Object)} to be implemented
 * and will automatically handle failures.
 *
 * @opensearch.api
 */
public abstract class RestActionListener<Response> implements ActionListener<Response> {

    // we use static here so we won't have to pass the actual logger each time for a very rare case of logging
    // where the settings don't matter that much
    private static Logger logger = LogManager.getLogger(RestResponseListener.class);

    protected final RestChannel channel;

    protected RestActionListener(RestChannel channel) {
        this.channel = channel;
    }

    @Override
    public final void onResponse(Response response) {
        try {
            processResponse(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    protected abstract void processResponse(Response response) throws Exception;

    private BytesRestResponse from(Exception e) throws IOException {
        try {
            return new BytesRestResponse(channel, e);
        } catch (Exception inner) {
            try {
                return new BytesRestResponse(channel, inner);
            } finally {
                inner.addSuppressed(e);
                logger.error("failed to construct failure response", inner);
            }
        }
    }

    @Override
    public final void onFailure(Exception e) {
        try {
            channel.sendResponse(from(e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        }
    }
}
