/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A size-based condition for an index size.
 * Evaluates to <code>true</code> if the index size is at least {@link #value}.
 *
 * @opensearch.internal
 */
public class MaxSizeCondition extends Condition<ByteSizeValue> {
    public static final String NAME = "max_size";

    public MaxSizeCondition(ByteSizeValue value) {
        super(NAME);
        this.value = value;
    }

    public MaxSizeCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = new ByteSizeValue(in.readVLong(), ByteSizeUnit.BYTES);
    }

    @Override
    public Result evaluate(Stats stats) {
        return new Result(this, stats.indexSize.getBytes() >= value.getBytes());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // While we technically could serialize this with value.writeTo(...), that would
        // require doing the song and dance around backwards compatibility for this value. Since
        // in this case the deserialized version is not displayed to a user, it's okay to simply use
        // bytes.
        out.writeVLong(value.getBytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MaxSizeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MaxSizeCondition(ByteSizeValue.parseBytesSizeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token: " + parser.currentToken());
        }
    }
}
