/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for maximum primary shard docs. Evaluates to <code>true</code>
 * when the largest primary shard of the index has at least {@link #value} docs
 *
 * @opensearch.internal
 */
public class MaxPrimaryShardDocsCondition extends Condition<Long> {
    public static final String NAME = "max_primary_shard_docs";

    public MaxPrimaryShardDocsCondition(Long value) {
        super(NAME);
        this.value = value;
    }

    public MaxPrimaryShardDocsCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = in.readLong();
    }

    @Override
    public Result evaluate(final Stats stats) {
        return new Result(this, this.value <= stats.maxPrimaryShardDocs);
    }

    @Override
    boolean includedInVersion(Version version) {
        return version.onOrAfter(Version.V_3_11_0);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value);
    }

    public static MaxPrimaryShardDocsCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_NUMBER) {
            return new MaxPrimaryShardDocsCondition(parser.longValue());
        } else {
            throw new IllegalArgumentException("invalid token: " + parser.currentToken());
        }
    }
}
