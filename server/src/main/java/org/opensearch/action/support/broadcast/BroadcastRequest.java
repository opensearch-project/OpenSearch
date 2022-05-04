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

package org.opensearch.action.support.broadcast;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Transport request for broadcast operations
 *
 * @opensearch.internal
 */
public class BroadcastRequest<Request extends BroadcastRequest<Request>> extends ActionRequest implements IndicesRequest.Replaceable {

    protected String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

    public BroadcastRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    protected BroadcastRequest(String... indices) {
        this.indices = indices;
    }

    protected BroadcastRequest(String[] indices, IndicesOptions indicesOptions) {
        this.indices = indices;
        this.indicesOptions = indicesOptions;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    private final TimeValue DEFAULT_TIMEOUT_SECONDS = TimeValue.timeValueSeconds(30);
    private TimeValue timeout;

    @SuppressWarnings("unchecked")
    @Override
    public final Request indices(String... indices) {
        this.indices = indices;
        return (Request) this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, DEFAULT_TIMEOUT_SECONDS, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @SuppressWarnings("unchecked")
    public final Request indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return (Request) this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
