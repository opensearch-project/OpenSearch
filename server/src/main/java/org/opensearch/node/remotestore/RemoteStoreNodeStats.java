/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.RemoteStoreSettings;

import java.io.IOException;

/**
 * Remote store node level stats
 * @opensearch.internal
 */
public class RemoteStoreNodeStats implements Writeable, ToXContentFragment {

    /**
     * Time stamp for the last successful fetch of pinned timestamps by the RemoteStorePinnedTimestampService
     */
    private long lastSuccessfulFetchOfPinnedTimestamps;

    public RemoteStoreNodeStats(final long lastSuccessfulFetchOfPinnedTimestamps) {
        this.lastSuccessfulFetchOfPinnedTimestamps = lastSuccessfulFetchOfPinnedTimestamps;
    }

    public long getLastSuccessfulFetchOfPinnedTimestamps() {
        return this.lastSuccessfulFetchOfPinnedTimestamps;
    }

    public RemoteStoreNodeStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.CURRENT) && RemoteStoreSettings.isPinnedTimestampsEnabled()) {
            this.lastSuccessfulFetchOfPinnedTimestamps = in.readOptionalLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.CURRENT) && RemoteStoreSettings.isPinnedTimestampsEnabled()) {
            out.writeOptionalLong(this.lastSuccessfulFetchOfPinnedTimestamps);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("remote_store_node_stats");
        builder.field("last_successful_fetch_of_pinned_timestamps", this.lastSuccessfulFetchOfPinnedTimestamps);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "RemoteStoreNodeStats{ lastSuccessfulFetchOfPinnedTimestamps=" + lastSuccessfulFetchOfPinnedTimestamps + "}";
    }
}
