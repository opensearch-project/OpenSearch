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

package org.opensearch.action.admin.indices.upgrade.post;

import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * A request to upgrade one or more indices. In order to optimize on all the indices, pass an empty array or
 * {@code null} for the indices.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpgradeRequestBuilder extends BroadcastOperationRequestBuilder<UpgradeRequest, UpgradeResponse, UpgradeRequestBuilder> {

    public UpgradeRequestBuilder(OpenSearchClient client, UpgradeAction action) {
        super(client, action, new UpgradeRequest());
    }

    /**
     *  Should the upgrade only the ancient (older major version of Lucene) segments?
     */
    public UpgradeRequestBuilder setUpgradeOnlyAncientSegments(boolean upgradeOnlyAncientSegments) {
        request.upgradeOnlyAncientSegments(upgradeOnlyAncientSegments);
        return this;
    }
}
