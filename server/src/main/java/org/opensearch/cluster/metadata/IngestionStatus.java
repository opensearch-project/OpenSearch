/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Indicates pull-based ingestion status.
 */
@ExperimentalApi
public record IngestionStatus(boolean isPaused) implements Writeable, ToXContent {
    public static final String IS_PAUSED = "is_paused";

    public IngestionStatus(StreamInput in) throws IOException {
        this(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isPaused);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(IS_PAUSED, isPaused);
        builder.endObject();
        return builder;
    }

    public static IngestionStatus fromXContent(XContentParser parser) throws IOException {
        boolean isPaused = false;

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }

        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if (IS_PAUSED.equals(fieldName)) {
                        parser.nextToken();
                        isPaused = parser.booleanValue();
                    }
                }
            }
        }

        return new IngestionStatus(isPaused);
    }

    public static IngestionStatus getDefaultValue() {
        return new IngestionStatus(false);
    }
}
