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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.transport.TransportRequestOptions;

/**
 * A generic action. Should strive to make it a singleton.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ActionType<Response extends ActionResponse> {

    private final String name;
    private final Writeable.Reader<Response> responseReader;

    /**
     * @param name The name of the action, must be unique across actions.
     * @param responseReader A reader for the response type
     */
    public ActionType(String name, Writeable.Reader<Response> responseReader) {
        this.name = name;
        this.responseReader = responseReader;
    }

    /**
     * The name of the action. Must be unique across actions.
     */
    public String name() {
        return this.name;
    }

    /**
     * Get a reader that can create a new instance of the class from a {@link StreamInput}
     */
    public Writeable.Reader<Response> getResponseReader() {
        return responseReader;
    }

    /**
     * Optional request options for the action.
     */
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.EMPTY;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ActionType && name.equals(((ActionType<?>) o).name());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
