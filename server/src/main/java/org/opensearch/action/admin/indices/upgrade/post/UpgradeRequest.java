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

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.transport.client.Requests;

import java.io.IOException;

/**
 * A request to upgrade one or more indices. In order to update all indices, pass an empty array or
 * {@code null} for the indices.
 * @see Requests#upgradeRequest(String...)
 * @see IndicesAdminClient#upgrade(UpgradeRequest)
 * @see UpgradeResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpgradeRequest extends BroadcastRequest<UpgradeRequest> {

    /**
     * Default config for Upgrade Requests
     *
     * @opensearch.internal
     */
    public static final class Defaults {
        public static final boolean UPGRADE_ONLY_ANCIENT_SEGMENTS = false;
    }

    private boolean upgradeOnlyAncientSegments = Defaults.UPGRADE_ONLY_ANCIENT_SEGMENTS;

    /**
     * Constructs an optimization request over one or more indices.
     *
     * @param indices The indices to upgrade, no indices passed means all indices will be optimized.
     */
    public UpgradeRequest(String... indices) {
        super(indices);
    }

    public UpgradeRequest(StreamInput in) throws IOException {
        super(in);
        upgradeOnlyAncientSegments = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(upgradeOnlyAncientSegments);
    }

    /**
     * Should the upgrade only the ancient (older major version of Lucene) segments?
     * Defaults to {@code false}.
     */
    public boolean upgradeOnlyAncientSegments() {
        return upgradeOnlyAncientSegments;
    }

    /**
     * See {@link #upgradeOnlyAncientSegments()}
     */
    public UpgradeRequest upgradeOnlyAncientSegments(boolean upgradeOnlyAncientSegments) {
        this.upgradeOnlyAncientSegments = upgradeOnlyAncientSegments;
        return this;
    }

    @Override
    public String toString() {
        return "UpgradeRequest{" + "upgradeOnlyAncientSegments=" + upgradeOnlyAncientSegments + '}';
    }
}
