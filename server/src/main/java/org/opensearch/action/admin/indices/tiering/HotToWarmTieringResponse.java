/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
 *
 *  @opensearch.experimental
 */
@ExperimentalApi
public class HotToWarmTieringResponse extends AcknowledgedResponse {

    private final List<IndexResult> failedIndices;

    public HotToWarmTieringResponse(boolean acknowledged) {
        super(acknowledged);
        this.failedIndices = Collections.emptyList();
    }

    public HotToWarmTieringResponse(boolean acknowledged, List<IndexResult> indicesResults) {
        super(acknowledged);
        this.failedIndices = (indicesResults == null)
            ? Collections.emptyList()
            : indicesResults.stream().sorted(Comparator.comparing(IndexResult::getIndex)).collect(Collectors.toList());
    }

    public HotToWarmTieringResponse(StreamInput in) throws IOException {
        super(in);
        failedIndices = Collections.unmodifiableList(in.readList(IndexResult::new));
    }

    public List<IndexResult> getFailedIndices() {
        return this.failedIndices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.failedIndices);
    }

    @Override
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
     * @opensearch.experimental
     */
    @ExperimentalApi
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

        public String getIndex() {
            return index;
        }

        public String getFailureReason() {
            return failureReason;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(failureReason);
        }

        @Override
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
