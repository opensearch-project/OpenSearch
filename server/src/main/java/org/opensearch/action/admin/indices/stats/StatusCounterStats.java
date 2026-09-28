/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * REST status statistics for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public class StatusCounterStats implements Writeable, ToXContentFragment {

    @Nullable
    private DocStatusStats docStatusStats;

    @Nullable
    private SearchResponseStatusStats searchResponseStatusStats;

    public StatusCounterStats() {
        docStatusStats = new DocStatusStats();
        searchResponseStatusStats = new SearchResponseStatusStats();
    }

    public StatusCounterStats(DocStatusStats docStatusStats, SearchResponseStatusStats searchResponseStatusStats) {
        this.docStatusStats = docStatusStats;
        this.searchResponseStatusStats = searchResponseStatusStats;
    }

    public StatusCounterStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_4_0)) {
            docStatusStats = in.readOptionalWriteable(DocStatusStats::new);
            searchResponseStatusStats = in.readOptionalWriteable(SearchResponseStatusStats::new);
        } else {
            docStatusStats = null;
            searchResponseStatusStats = null;
        }
    }

    public DocStatusStats getDocStatusStats() {
        return docStatusStats;
    }

    public SearchResponseStatusStats getSearchResponseStatusStats() {
        return searchResponseStatusStats;
    }

    /**
     * Gets a snapshot of the current state of the REST status counters.
     */
    public StatusCounterStats getSnapshot() {
        StatusCounterStats stats = new StatusCounterStats();
        stats.getDocStatusStats().add(docStatusStats);
        stats.getSearchResponseStatusStats().add(searchResponseStatusStats);
        return stats;
    }

    public void add(StatusCounterStats stats) {
        if (stats == null) {
            return;
        }
        if (docStatusStats != null) {
            docStatusStats.add(stats.docStatusStats);
        }
        if (searchResponseStatusStats != null) {
            searchResponseStatusStats.add(stats.searchResponseStatusStats);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
            out.writeOptionalWriteable(docStatusStats.getSnapshot());
            out.writeOptionalWriteable(searchResponseStatusStats.getSnapshot());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STATUS_COUNTER);
        docStatusStats.getSnapshot().toXContent(builder, params);
        searchResponseStatusStats.getSnapshot().toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String STATUS_COUNTER = "status_counter";
    }
}
