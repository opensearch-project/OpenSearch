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

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;

import java.io.IOException;

/**
 * Transport request for retrieving indices segment information
 *
 * @opensearch.internal
 */
public class IndicesSegmentsRequest extends BroadcastRequest<IndicesSegmentsRequest> {

    protected boolean verbose = false;

    public IndicesSegmentsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        verbose = in.readBoolean();
    }

    public IndicesSegmentsRequest(String... indices) {
        super(indices);
    }

    /**
     * <code>true</code> if detailed information about each segment should be returned,
     * <code>false</code> otherwise.
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets the <code>verbose</code> option.
     * @see #verbose()
     */
    public void verbose(boolean v) {
        verbose = v;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(verbose);

    }
}
