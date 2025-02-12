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

package org.opensearch.index.reindex;

import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.script.Script;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Base builder class for bulk index by scrolls
 *
 * @opensearch.internal
 */
public abstract class AbstractBulkIndexByScrollRequestBuilder<
    Request extends AbstractBulkIndexByScrollRequest<Request>,
    Self extends AbstractBulkIndexByScrollRequestBuilder<Request, Self>> extends AbstractBulkByScrollRequestBuilder<Request, Self> {

    protected AbstractBulkIndexByScrollRequestBuilder(
        OpenSearchClient client,
        ActionType<BulkByScrollResponse> action,
        SearchRequestBuilder search,
        Request request
    ) {
        super(client, action, search, request);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self script(Script script) {
        request.setScript(script);
        return self();
    }
}
