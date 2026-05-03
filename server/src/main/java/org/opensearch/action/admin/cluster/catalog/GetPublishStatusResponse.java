/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.catalog.PublishEntry;
import org.opensearch.catalog.PublishPhase;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response to {@link GetPublishStatusAction}.
 * <p>
 * {@code status} is derived from the matched entry's phase. A successful publish has its
 * entry removed from cluster state, so {@code NOT_FOUND} is how callers observe success.
 *
 * @opensearch.experimental
 */
public class GetPublishStatusResponse extends ActionResponse implements ToXContentObject {

    public enum Status {
        IN_PROGRESS,
        FAILED,
        NOT_FOUND
    }

    @Nullable
    private final PublishEntry entry;

    public GetPublishStatusResponse(@Nullable PublishEntry entry) {
        this.entry = entry;
    }

    public GetPublishStatusResponse(StreamInput in) throws IOException {
        super(in);
        this.entry = in.readBoolean() ? new PublishEntry(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (entry == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            entry.writeTo(out);
        }
    }

    @Nullable
    public PublishEntry entry() {
        return entry;
    }

    public Status status() {
        if (entry == null) return Status.NOT_FOUND;
        return entry.phase() == PublishPhase.FAILED ? Status.FAILED : Status.IN_PROGRESS;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status().name());
        if (entry != null) {
            builder.field("entry");
            entry.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
