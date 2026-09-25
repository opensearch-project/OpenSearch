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

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shard level profile results
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ProfileShardResult implements Writeable {

    private final List<QueryProfileShardResult> queryProfileResults;

    private final AggregationProfileShardResult aggProfileShardResult;

    private final FetchProfileShardResult fetchProfileResult;

    private NetworkTime networkTime;

    private final long queueWaitNanos;

    public ProfileShardResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult,
        FetchProfileShardResult fetchProfileResult,
        NetworkTime networkTime
    ) {
        this(queryProfileResults, aggProfileShardResult, fetchProfileResult, networkTime, -1L);
    }

    public ProfileShardResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult,
        FetchProfileShardResult fetchProfileResult,
        NetworkTime networkTime,
        long queueWaitNanos
    ) {
        this.aggProfileShardResult = aggProfileShardResult;
        this.fetchProfileResult = fetchProfileResult;
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.networkTime = networkTime;
        this.queueWaitNanos = queueWaitNanos;
    }

    /**
     * Constructor for backwards compatibility.
     * @deprecated Use {@link #ProfileShardResult(List, AggregationProfileShardResult, FetchProfileShardResult, NetworkTime)} instead
     */
    @Deprecated
    public ProfileShardResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult,
        NetworkTime networkTime
    ) {
        this(queryProfileResults, aggProfileShardResult, new FetchProfileShardResult(Collections.emptyList()), networkTime);
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
        if (in.getVersion().onOrAfter(Version.V_3_2_0)) {
            this.fetchProfileResult = new FetchProfileShardResult(in);
        } else {
            this.fetchProfileResult = new FetchProfileShardResult(Collections.emptyList());
        }
        this.networkTime = new NetworkTime(in);
        this.queueWaitNanos = in.getVersion().onOrAfter(Version.V_3_9_0) ? in.readZLong() : -1L;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryProfileResults.size());
        for (QueryProfileShardResult queryShardResult : queryProfileResults) {
            queryShardResult.writeTo(out);
        }
        aggProfileShardResult.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_2_0)) {
            fetchProfileResult.writeTo(out);
        }
        networkTime.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_9_0)) {
            out.writeZLong(queueWaitNanos);
        }
    }

    public List<QueryProfileShardResult> getQueryProfileResults() {
        return queryProfileResults;
    }

    public AggregationProfileShardResult getAggregationProfileResults() {
        return aggProfileShardResult;
    }

    public FetchProfileShardResult getFetchProfileResult() {
        return fetchProfileResult;
    }

    /**
     * Time this shard's search task spent queued on the search thread pool before execution began, or -1 when
     * the task never went through a queueing executor or the result came from a node older than 3.9.
     */
    public long getQueueWaitNanos() {
        return queueWaitNanos;
    }

    public NetworkTime getNetworkTime() {
        return networkTime;
    }

    public void setNetworkTime(NetworkTime newTime) {
        networkTime.setInboundNetworkTime(newTime.getInboundNetworkTime());
        networkTime.setOutboundNetworkTime(newTime.getOutboundNetworkTime());
    }

}
