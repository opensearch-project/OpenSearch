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

package org.opensearch.action.support.master;

import org.opensearch.action.ActionResponse;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response that indicates that a request has been acknowledged
 *
 * @opensearch.internal
 */
public class AcknowledgedResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    protected static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    protected boolean acknowledged;

    public AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
    }

    public AcknowledgedResponse(StreamInput in, boolean readAcknowledged) throws IOException {
        super(in);
        if (readAcknowledged) {
            acknowledged = in.readBoolean();
        }
    }

    public AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Returns whether the response is acknowledged or not
     * @return true if the response is acknowledged, false otherwise
     */
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), isAcknowledged());
        addCustomFields(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {

    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
        "acknowledged_flag",
        true,
        args -> (Boolean) args[0]
    );

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    public static AcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return new AcknowledgedResponse(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }
}
