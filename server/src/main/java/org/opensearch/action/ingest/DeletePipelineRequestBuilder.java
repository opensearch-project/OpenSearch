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

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

/**
 * Transport request builder to delete a pipeline
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DeletePipelineRequestBuilder extends ActionRequestBuilder<DeletePipelineRequest, AcknowledgedResponse> {

    public DeletePipelineRequestBuilder(OpenSearchClient client, DeletePipelineAction action) {
        super(client, action, new DeletePipelineRequest());
    }

    public DeletePipelineRequestBuilder(OpenSearchClient client, DeletePipelineAction action, String id) {
        super(client, action, new DeletePipelineRequest(id));
    }

    /**
     * Sets the id of the pipeline to delete.
     */
    public DeletePipelineRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

}
