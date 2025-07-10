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

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks rest category class codes from search requests
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchResponseStatusStats implements Writeable, ToXContentFragment {
    final LongAdder[] searchResponseStatusCounter;

    public SearchResponseStatusStats() {
        searchResponseStatusCounter = new LongAdder[5];
        for (int i = 0; i < searchResponseStatusCounter.length; i++) {
            searchResponseStatusCounter[i] = new LongAdder();
        }
    }

    public SearchResponseStatusStats(StreamInput in) throws IOException {
        searchResponseStatusCounter = in.readArray(i -> {
            LongAdder adder = new LongAdder();
            adder.add(i.readLong());
            return adder;

        }, LongAdder[]::new);

        assert searchResponseStatusCounter.length == 5 : "Length of incoming array should be 5! Got " + searchResponseStatusCounter.length;
    }

    /**
     * Increment counter for status
     *
     * @param status {@link RestStatus}
     */
    public void inc(final RestStatus status) {
        add(status, 1L);
    }

    /**
     * Increment counter for status by count
     *
     * @param status {@link RestStatus}
     * @param delta The value to add
     */
    void add(final RestStatus status, final long delta) {
        searchResponseStatusCounter[status.getStatusFamilyCode() - 1].add(delta);
    }

    /**
     * Accumulate stats from the passed Object
     *
     * @param stats Instance storing {@link SearchResponseStatusStats}
     */
    public void add(final SearchResponseStatusStats stats) {
        if (null == stats) {
            return;
        }

        for (int i = 0; i < searchResponseStatusCounter.length; ++i) {
            searchResponseStatusCounter[i].add(stats.searchResponseStatusCounter[i].longValue());
        }
    }

    public LongAdder[] getSearchResponseStatusCounter() {
        return searchResponseStatusCounter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEARCH_RESPONSE_STATUS);

        Map<String, Long> errorTypeCounts = new HashMap<>();

        for (int i = 0; i < searchResponseStatusCounter.length; ++i) {
            long count = searchResponseStatusCounter[i].longValue();

            if (count > 0) {
                RestStatus familyStatus = RestStatus.fromCode((i + 1) * 100);
                String errorType = familyStatus.getErrorType();
                errorTypeCounts.put(errorType, errorTypeCounts.getOrDefault(errorType, (long) 0) + count);
            }
        }

        String successType = RestStatus.ACCEPTED.getErrorType();
        String userFailureType = RestStatus.BAD_REQUEST.getErrorType();
        String systemErrorType = RestStatus.INTERNAL_SERVER_ERROR.getErrorType();
        builder.field(successType, errorTypeCounts.getOrDefault(successType, (long) 0));
        builder.field(userFailureType, errorTypeCounts.getOrDefault(userFailureType, (long) 0));
        builder.field(systemErrorType, errorTypeCounts.getOrDefault(systemErrorType, (long) 0));

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray((o, v) -> o.writeLong(v.longValue()), searchResponseStatusCounter);
    }

    /**
     * For this function, I just want to retrieve a point in time snapshot of the SearchResponseStatusStats.
     */
    public SearchResponseStatusStats getSnapshot() {
        SearchResponseStatusStats curSnapshot = new SearchResponseStatusStats();
        curSnapshot.add(this);
        return curSnapshot;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEARCH_RESPONSE_STATUS = "search_response_status";
    }
}
