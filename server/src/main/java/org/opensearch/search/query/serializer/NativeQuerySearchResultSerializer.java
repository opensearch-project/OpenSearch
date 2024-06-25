/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.serializer;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.stream.DelayableWriteable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;

import static org.opensearch.common.lucene.Lucene.readTopDocs;
import static org.opensearch.common.lucene.Lucene.writeTopDocs;

/**
 * A serializer for {@link QuerySearchResult} that can read and write from a stream.
 */
@PublicApi(since = "3.0.0")
public class NativeQuerySearchResultSerializer implements QuerySearchResultSerializer<StreamInput, StreamOutput> {

    private int from;
    private int size;
    private TopDocsAndMaxScore topDocsAndMaxScore;
    private boolean hasScoreDocs;
    private TotalHits totalHits;
    private float maxScore = Float.NaN;
    private DocValueFormat[] sortValueFormats;
    /**
     * Aggregation results. We wrap them in
     * {@linkplain DelayableWriteable} because
     * {@link InternalAggregation} is usually made up of many small objects
     * which have a fairly high overhead in the JVM. So we delay deserializing
     * them until just before we need them.
     */
    private DelayableWriteable<InternalAggregations> aggregations;
    private boolean hasAggs;
    private Suggest suggest;
    private boolean searchTimedOut;
    private Boolean terminatedEarly = null;
    private ProfileShardResult profileShardResults;
    private boolean hasProfileResults;
    private long serviceTimeEWMA = -1;
    private int nodeQueueSize = -1;

    private boolean isNull;

    public NativeQuerySearchResultSerializer(boolean isNull) {
        this.isNull = isNull;
    }

    @Override
    public void readQuerySearchResult(StreamInput in, boolean isNull) throws IOException {
        if (isNull == false) {
            readFromWithId(in);
        }
    }

    @Override
    public void writeQuerySearchResult(StreamOutput out) throws IOException {
        writeToNoId(out);
    }

    public void readFromWithId(StreamInput in) throws IOException {
        from = in.readVInt();
        size = in.readVInt();
        int numSortFieldsPlus1 = in.readVInt();
        if (numSortFieldsPlus1 == 0) {
            sortValueFormats = null;
        } else {
            sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
            for (int i = 0; i < sortValueFormats.length; ++i) {
                sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
            }
        }
        setTopDocs(readTopDocs(in));
        if (hasAggs = in.readBoolean()) {
            aggregations = DelayableWriteable.delayed(InternalAggregations::readFrom, in);
        }
        if (in.readBoolean()) {
            suggest = new Suggest(in);
        }
        searchTimedOut = in.readBoolean();
        terminatedEarly = in.readOptionalBoolean();
        profileShardResults = in.readOptionalWriteable(ProfileShardResult::new);
        hasProfileResults = profileShardResults != null;
        serviceTimeEWMA = in.readZLong();
        nodeQueueSize = in.readInt();
    }

    public void writeToNoId(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        if (sortValueFormats == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1 + sortValueFormats.length);
            for (int i = 0; i < sortValueFormats.length; ++i) {
                out.writeNamedWriteable(sortValueFormats[i]);
            }
        }
        writeTopDocs(out, topDocsAndMaxScore);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileShardResults);
        out.writeZLong(serviceTimeEWMA);
        out.writeInt(nodeQueueSize);
    }

    private void setTopDocs(TopDocsAndMaxScore topDocsAndMaxScore) {
        this.topDocsAndMaxScore = topDocsAndMaxScore;
        this.totalHits = topDocsAndMaxScore.topDocs.totalHits;
        this.maxScore = topDocsAndMaxScore.maxScore;
        this.hasScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs.length > 0;
    }

    public void searchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public void terminatedEarly(boolean terminatedEarly) {
        this.terminatedEarly = terminatedEarly;
    }

    public Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public TotalHits getTotalHits() {
        return totalHits;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public TopDocsAndMaxScore topDocs() {
        if (topDocsAndMaxScore == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        return topDocsAndMaxScore;
    }

    /**
     * Returns <code>true</code> iff the top docs have already been consumed.
     */
    public boolean hasConsumedTopDocs() {
        return topDocsAndMaxScore == null;
    }

    /**
     * Returns and nulls out the top docs for this search results. This allows to free up memory once the top docs are consumed.
     * @throws IllegalStateException if the top docs have already been consumed.
     */
    public TopDocsAndMaxScore consumeTopDocs() {
        TopDocsAndMaxScore topDocsAndMaxScore = this.topDocsAndMaxScore;
        if (topDocsAndMaxScore == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        this.topDocsAndMaxScore = null;
        return topDocsAndMaxScore;
    }

    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        setTopDocs(topDocs);
        if (topDocs.topDocs.scoreDocs.length > 0 && topDocs.topDocs.scoreDocs[0] instanceof FieldDoc) {
            int numFields = ((FieldDoc) topDocs.topDocs.scoreDocs[0]).fields.length;
            if (numFields != sortValueFormats.length) {
                throw new IllegalArgumentException(
                    "The number of sort fields does not match: " + numFields + " != " + sortValueFormats.length
                );
            }
        }
        this.sortValueFormats = sortValueFormats;
    }

    public DocValueFormat[] sortValueFormats() {
        return sortValueFormats;
    }

    /**
     * Returns <code>true</code> if this query result has unconsumed aggregations
     */
    public boolean hasAggs() {
        return hasAggs;
    }

    /**
     * Returns and nulls out the aggregation for this search results. This allows to free up memory once the aggregation is consumed.
     * @throws IllegalStateException if the aggregations have already been consumed.
     */
    public DelayableWriteable<InternalAggregations> consumeAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already consumed");
        }
        DelayableWriteable<InternalAggregations> aggs = aggregations;
        aggregations = null;
        return aggs;
    }

    public void aggregations(InternalAggregations aggregations) {
        this.aggregations = aggregations == null ? null : DelayableWriteable.referencing(aggregations);
        hasAggs = aggregations != null;
    }

    public DelayableWriteable<InternalAggregations> aggregations() {
        return aggregations;
    }

    public boolean hasProfileResults() {
        return hasProfileResults;
    }

    public void consumeAll() {
        if (hasConsumedTopDocs() == false) {
            consumeTopDocs();
        }
        if (hasAggs()) {
            consumeAggs();
        }
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(ProfileShardResult shardResults) {
        this.profileShardResults = shardResults;
        hasProfileResults = shardResults != null;
    }

    public ProfileShardResult profileResults() {
        return profileShardResults;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public int from() {
        return from;
    }

    public void from(int from) {
        this.from = from;
    }

    /**
     * Returns the maximum size of this results top docs.
     */
    public int size() {
        return size;
    }

    public void size(int size) {
        this.size = size;
    }

    public long serviceTimeEWMA() {
        return this.serviceTimeEWMA;
    }

    public void serviceTimeEWMA(long serviceTimeEWMA) {
        this.serviceTimeEWMA = serviceTimeEWMA;
    }

    public int nodeQueueSize() {
        return this.nodeQueueSize;
    }

    public void nodeQueueSize(int nodeQueueSize) {
        this.nodeQueueSize = nodeQueueSize;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
        return (suggest != null && suggest.hasScoreDocs());
    }

    public boolean hasSearchContext() {
        return hasScoreDocs || hasSuggestHits();
    }

}
