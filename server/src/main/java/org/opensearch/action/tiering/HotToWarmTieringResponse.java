package org.opensearch.action.tiering;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Response object for an {@link TieringIndexRequest} which is sent to client after the initial verification of the request
 * by the backend service. The format of the response object will be as below:
 *
 *  {
 *     "acknowledged": true/false,
 *     "failed_indices": [
 *              {
 *                "index": "index1",
 *                "error": "Low disk threshold watermark breached"
 *              },
 *              {
 *                "index": "index2",
 *                "error": "Index is not a remote store backed index"
 *              }
 *     ]
 *  }
 */
public class HotToWarmTieringResponse extends AcknowledgedResponse {

    private List<IndexResult> failedIndices;

    public HotToWarmTieringResponse(boolean acknowledged, List<IndexResult> indicesResults) {
        super(acknowledged);
        this.failedIndices = (indicesResults == null) ? new LinkedList<>() : indicesResults;
    }

    public HotToWarmTieringResponse(StreamInput in) throws IOException {
        super(in);
        failedIndices = Collections.unmodifiableList(in.readList(IndexResult::new));
    }

    public HotToWarmTieringResponse failedIndices(List<IndexResult> failedIndices) {
        this.failedIndices = failedIndices;
        return this;
    }

    public List<HotToWarmTieringResponse.IndexResult> getFailedIndices() {
        return this.failedIndices;
    }

    public void addIndexResult(String indexName, String failureReason) {
        failedIndices.add(new IndexResult(indexName, failureReason));
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.failedIndices);
    }

    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startArray("failed_indices");

        for (IndexResult failedIndex : failedIndices) {
            failedIndex.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Inner class to represent the result of a failed index for tiering.
     */
    public static class IndexResult implements Writeable, ToXContentFragment {
        private final String index;
        private final String failureReason;

        public IndexResult(String index, String failureReason) {
            this.index = index;
            this.failureReason = failureReason;
        }

        IndexResult(StreamInput in) throws IOException {
            this.index = in.readString();
            this.failureReason = in.readString();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(failureReason);
        }

        public String getIndex() {
            return index;
        }

        public String getFailureReason() {
            return failureReason;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index", index);
            builder.field("error", failureReason);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexResult that = (IndexResult) o;
            return Objects.equals(index, that.index) && Objects.equals(failureReason, that.failureReason);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(index);
            result = 31 * result + Objects.hashCode(failureReason);
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }
}
