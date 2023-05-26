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

package org.opensearch.action.ingest;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;

import java.io.IOException;

/**
 * transport request to get a pipeline
 *
 * @opensearch.internal
 */
public class GetPipelineRequest extends ClusterManagerNodeReadRequest<GetPipelineRequest> {

    private String[] ids;

    public GetPipelineRequest(String... ids) {
        if (ids == null) {
            throw new IllegalArgumentException("ids cannot be null");
        }
        this.ids = ids;
    }

    GetPipelineRequest() {
        this.ids = Strings.EMPTY_ARRAY;
    }

    public GetPipelineRequest(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(ids);
    }

    public String[] getIds() {
        return ids;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
