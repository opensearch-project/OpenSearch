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

package org.opensearch.action.admin.indices.dangling.list;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport request for listing a dangling indices
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ListDanglingIndicesRequest extends BaseNodesRequest<ListDanglingIndicesRequest> {
    /**
     * Filter the response by index UUID. Leave as null to find all indices.
     */
    private final String indexUUID;

    public ListDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readOptionalString();
    }

    public ListDanglingIndicesRequest() {
        super(Strings.EMPTY_ARRAY);
        this.indexUUID = null;
    }

    public ListDanglingIndicesRequest(String indexUUID) {
        super(Strings.EMPTY_ARRAY);
        this.indexUUID = indexUUID;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    @Override
    public String toString() {
        return "ListDanglingIndicesRequest{indexUUID='" + indexUUID + "'}";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(this.indexUUID);
    }
}
