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

package org.opensearch.action.admin.indices.create;

import org.opensearch.action.support.master.ShardsAcknowledgedResponse;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a create index action.
 *
 * @opensearch.internal
 */
public class CreateIndexResponse extends ShardsAcknowledgedResponse {

    private static final ParseField INDEX = new ParseField("index");

    private static final ConstructingObjectParser<CreateIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "create_index",
        true,
        args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2])
    );

    static {
        declareFields(PARSER);
    }

    protected static <T extends CreateIndexResponse> void declareFields(ConstructingObjectParser<T, Void> objectParser) {
        declareAcknowledgedAndShardsAcknowledgedFields(objectParser);
        objectParser.declareField(constructorArg(), (parser, context) -> parser.textOrNull(), INDEX, ObjectParser.ValueType.STRING_OR_NULL);
    }

    private final String index;

    protected CreateIndexResponse(StreamInput in) throws IOException {
        super(in, true);
        index = in.readString();
    }

    public CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
        out.writeString(index);
    }

    public String index() {
        return index;
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(INDEX.getPreferredName(), index());
    }

    public static CreateIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            CreateIndexResponse that = (CreateIndexResponse) o;
            return Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}
