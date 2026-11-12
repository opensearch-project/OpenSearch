/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.OpenSearchException;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Transport response to remove an index block. It reports, for every index the request resolved to, whether the block is gone.
 * The response is acknowledged when all nodes applied the cluster state in time and the block is gone from every index.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.11.0")
public class RemoveIndexBlockResponse extends AcknowledgedResponse {

    private final List<RemoveBlockResult> indices;

    RemoveIndexBlockResponse(StreamInput in) throws IOException {
        super(in);
        indices = unmodifiableList(in.readList(RemoveBlockResult::new));
    }

    public RemoveIndexBlockResponse(final boolean acknowledged, final List<RemoveBlockResult> indices) {
        super(acknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indices));
    }

    /**
     * Returns the result for every index the request resolved to, in request order
     */
    public List<RemoveBlockResult> getIndices() {
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
        builder.startArray("indices");
        for (RemoveBlockResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Result for removing a block from one index. The block is unblocked unless the index reports an exception. An index that did not
     * have the block counts as unblocked.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.11.0")
    public static class RemoveBlockResult implements Writeable, ToXContentFragment {

        private final Index index;
        private final @Nullable Exception exception;

        /**
         * Creates the result for an index the block is no longer set on
         */
        public RemoveBlockResult(final Index index) {
            this.index = Objects.requireNonNull(index);
            this.exception = null;
        }

        /**
         * Creates the result for an index the block could not be removed from
         */
        public RemoveBlockResult(final Index index, final Exception failure) {
            this.index = Objects.requireNonNull(index);
            this.exception = Objects.requireNonNull(failure);
        }

        RemoveBlockResult(final StreamInput in) throws IOException {
            this.index = new Index(in);
            this.exception = in.readException();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            index.writeTo(out);
            out.writeException(exception);
        }

        public Index getIndex() {
            return index;
        }

        /**
         * Returns why the block could not be removed, or {@code null} if it was
         */
        public Exception getException() {
            return exception;
        }

        /**
         * Returns whether the index no longer has the block
         */
        public boolean isUnblocked() {
            return exception == null;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", index.getName());
                builder.field("unblocked", isUnblocked());
                if (exception != null) {
                    builder.startObject("exception");
                    OpenSearchException.generateFailureXContent(builder, params, exception, true);
                    builder.endObject();
                }
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }
}
