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
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for maximum primary shard size. Evaluates to <code>true</code>
 * when the largest primary shard of the index is at least {@link #value} bytes
 *
 * @opensearch.internal
 */
public class MaxPrimaryShardSizeCondition extends Condition<ByteSizeValue> {
    public static final String NAME = "max_primary_shard_size";

    public MaxPrimaryShardSizeCondition(ByteSizeValue value) {
        super(NAME);
        if (value.getBytes() < 0) {
            throw new IllegalArgumentException("[" + NAME + "] must be non-negative, got [" + value.getStringRep() + "]");
        }
        this.value = value;
    }

    public MaxPrimaryShardSizeCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = new ByteSizeValue(in.readVLong(), ByteSizeUnit.BYTES);
    }

    @Override
    public Result evaluate(Stats stats) {
        return new Result(this, stats.maxPrimaryShardSize.getBytes() >= value.getBytes());
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
        out.writeVLong(value.getBytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MaxPrimaryShardSizeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MaxPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token: " + parser.currentToken());
        }
    }
}
