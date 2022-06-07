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

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.action.support.clustermanager.MasterNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.util.ArrayUtils;

/**
 * Snapshots status request builder
 *
 * @opensearch.internal
 */
public class SnapshotsStatusRequestBuilder extends MasterNodeOperationRequestBuilder<
    SnapshotsStatusRequest,
    SnapshotsStatusResponse,
    SnapshotsStatusRequestBuilder> {

    /**
     * Constructs the new snapshot status request
     */
    public SnapshotsStatusRequestBuilder(OpenSearchClient client, SnapshotsStatusAction action) {
        super(client, action, new SnapshotsStatusRequest());
    }

    /**
     * Constructs the new snapshot status request with specified repository
     */
    public SnapshotsStatusRequestBuilder(OpenSearchClient client, SnapshotsStatusAction action, String repository) {
        super(client, action, new SnapshotsStatusRequest(repository));
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets list of snapshots to return
     *
     * @param snapshots list of snapshots
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setSnapshots(String... snapshots) {
        request.snapshots(snapshots);
        return this;
    }

    /**
     * Adds additional snapshots to the list of snapshots to return
     *
     * @param snapshots additional snapshots
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder addSnapshots(String... snapshots) {
        request.snapshots(ArrayUtils.concat(request.snapshots(), snapshots));
        return this;
    }

    /**
     * Set to <code>true</code> to ignore unavailable snapshots, instead of throwing an exception.
     * Defaults to <code>false</code>, which means unavailable snapshots cause an exception to be thrown.
     *
     * @param ignoreUnavailable whether to ignore unavailable snapshots.
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setIgnoreUnavailable(boolean ignoreUnavailable) {
        request.ignoreUnavailable(ignoreUnavailable);
        return this;
    }
}
