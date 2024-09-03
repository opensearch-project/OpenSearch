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

package org.opensearch.snapshots;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown if the number of shards across the requested resources (snapshot(s) or the index/indices of a particular snapshot)
 * breaches the limit of snapshot.max_shards_allowed_in_status_api cluster setting
 *
 * @opensearch.internal
 */
public class TooManyShardsInSnapshotsStatusException extends SnapshotException {

    public TooManyShardsInSnapshotsStatusException(
        final String repositoryName,
        final SnapshotId snapshotId,
        final String message,
        final Throwable cause
    ) {
        super(repositoryName, snapshotId, message, cause);
    }

    public TooManyShardsInSnapshotsStatusException(final String repositoryName, final String message, String... snapshotName) {
        super(repositoryName, String.join(", ", snapshotName), message);
    }

    public TooManyShardsInSnapshotsStatusException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.REQUEST_ENTITY_TOO_LARGE;
    }
}
