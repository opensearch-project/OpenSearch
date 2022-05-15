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

package org.opensearch.search.profile;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shard level profile results
 *
 * @opensearch.internal
 */
public class ProfileShardResult implements Writeable {

    private final List<QueryProfileShardResult> queryProfileResults;

    private final AggregationProfileShardResult aggProfileShardResult;

    private NetworkTime networkTime;

    public ProfileShardResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult,
        NetworkTime networkTime
    ) {
        this.aggProfileShardResult = aggProfileShardResult;
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.networkTime = networkTime;
    }

    public ProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>(profileSize);
        for (int i = 0; i < profileSize; i++) {
            QueryProfileShardResult result = new QueryProfileShardResult(in);
            queryProfileResults.add(result);
        }
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.aggProfileShardResult = new AggregationProfileShardResult(in);
        this.networkTime = new NetworkTime(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryProfileResults.size());
        for (QueryProfileShardResult queryShardResult : queryProfileResults) {
            queryShardResult.writeTo(out);
        }
        aggProfileShardResult.writeTo(out);
        networkTime.writeTo(out);
    }

    public List<QueryProfileShardResult> getQueryProfileResults() {
        return queryProfileResults;
    }

    public AggregationProfileShardResult getAggregationProfileResults() {
        return aggProfileShardResult;
    }

    public NetworkTime getNetworkTime() {
        return networkTime;
    }

    public void setNetworkTime(NetworkTime newTime) {
        networkTime.setInboundNetworkTime(newTime.getInboundNetworkTime());
        networkTime.setOutboundNetworkTime(newTime.getOutboundNetworkTime());
    }

}
