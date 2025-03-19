/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Transport response for pausing ingestion.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PauseIngestionResponse extends AcknowledgedResponse {
    private static final String INDICES = "indices";
    private static final String INDEX = "index";
    private static final String ERROR = "error";

    private final List<IndexResult> indices;

    PauseIngestionResponse(StreamInput in) throws IOException {
        super(in);
        indices = unmodifiableList(in.readList(IndexResult::new));
    }

    public PauseIngestionResponse(final boolean acknowledged, final List<IndexResult> indexResults) {
        super(acknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indexResults));
    }

    public List<IndexResult> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(indices);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startArray(INDICES);
        for (IndexResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Represents result for an index.
     * todo: error message will be set when verbose mode is supported.
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class IndexResult implements Writeable, ToXContentFragment {
        private final String index;
        private final String errorMessage;

        public IndexResult(String index, String errorMessage) {
            this.index = index;
            this.errorMessage = errorMessage;
        }

        IndexResult(StreamInput in) throws IOException {
            this.index = in.readString();
            this.errorMessage = in.readString();
        }

        public String getIndex() {
            return index;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(errorMessage);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX, index);
            builder.field(ERROR, errorMessage);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PauseIngestionResponse.IndexResult that = (PauseIngestionResponse.IndexResult) o;
            return Objects.equals(index, that.index) && Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(index);
            result = 31 * result + Objects.hashCode(errorMessage);
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }
}
