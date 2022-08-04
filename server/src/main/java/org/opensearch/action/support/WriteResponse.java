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

package org.opensearch.action.support;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.index.IndexSettings;

/**
 * Interface implemented by responses for actions that modify the documents in an index like {@link IndexResponse}, {@link UpdateResponse},
 * and {@link BulkResponse}. Rather than implement this directly most implementers should extend {@link DocWriteResponse}.
 *
 * @opensearch.internal
 */
public interface WriteResponse {
    /**
     * Mark the response as having forced a refresh? Requests that set {@link WriteRequest#setRefreshPolicy(RefreshPolicy)} to
     * {@link RefreshPolicy#IMMEDIATE} should always mark this as true. Requests that set it to {@link RefreshPolicy#WAIT_UNTIL} will only
     * set this to true if they run out of refresh listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    void setForcedRefresh(boolean forcedRefresh);
}
