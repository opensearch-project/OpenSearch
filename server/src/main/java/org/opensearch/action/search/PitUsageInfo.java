/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * This holds usage information about a pit. Only successfully completed shard phases on the local cluster are counted:
 * {@code searchRequests} is the number of search requests that ran the query phase, exact when a single query is paginated
 * with search_after and a lower bound when different queries share the pit; {@code hitsReturned} is the number of
 * top-level hits loaded by the fetch phase.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.11.0")
public class PitUsageInfo implements ToXContentFragment, Writeable {
    private final String pitId;
    private final long searchRequests;
    private final long hitsReturned;

    public PitUsageInfo(String pitId, long searchRequests, long hitsReturned) {
        this.pitId = pitId;
        this.searchRequests = searchRequests;
        this.hitsReturned = hitsReturned;
    }

    public PitUsageInfo(StreamInput in) throws IOException {
        this.pitId = in.readString();
        this.searchRequests = in.readVLong();
        this.hitsReturned = in.readVLong();
    }

    public String getPitId() {
        return pitId;
    }

    public long getSearchRequests() {
        return searchRequests;
    }

    public long getHitsReturned() {
        return hitsReturned;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeVLong(searchRequests);
        out.writeVLong(hitsReturned);
    }

    private static final ParseField PIT_ID = new ParseField("pit_id");
    private static final ParseField SEARCH_REQUESTS = new ParseField("search_requests");
    private static final ParseField HITS_RETURNED = new ParseField("hits_returned");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.field(SEARCH_REQUESTS.getPreferredName(), searchRequests);
        builder.field(HITS_RETURNED.getPreferredName(), hitsReturned);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PitUsageInfo that = (PitUsageInfo) o;
        return pitId.equals(that.pitId) && searchRequests == that.searchRequests && hitsReturned == that.hitsReturned;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pitId, searchRequests, hitsReturned);
    }

    @Override
    public String toString() {
        return "PitUsageInfo{pitId='" + pitId + "', searchRequests=" + searchRequests + ", hitsReturned=" + hitsReturned + "}";
    }
}
