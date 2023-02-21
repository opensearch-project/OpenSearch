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

package org.opensearch.test;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Function;

public abstract class TestCustomMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private final String data;

    protected TestCustomMetadata(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestCustomMetadata that = (TestCustomMetadata) o;

        if (!data.equals(that.data)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    protected static <T extends TestCustomMetadata> T readFrom(Function<String, T> supplier, StreamInput in) throws IOException {
        return supplier.apply(in.readString());
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(String name, StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, name, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getData());
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metadata.Custom> T fromXContent(Function<String, Metadata.Custom> supplier, XContentParser parser)
        throws IOException {
        XContentParser.Token token;
        String data = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if ("data".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new OpenSearchParseException("failed to parse snapshottable metadata, invalid data type");
                    }
                    data = parser.text();
                } else {
                    throw new OpenSearchParseException("failed to parse snapshottable metadata, unknown field [{}]", currentFieldName);
                }
            } else {
                throw new OpenSearchParseException("failed to parse snapshottable metadata");
            }
        }
        if (data == null) {
            throw new OpenSearchParseException("failed to parse snapshottable metadata, data not found");
        }
        return (T) supplier.apply(data);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("data", getData());
        return builder;
    }

    @Override
    public String toString() {
        return "[" + getWriteableName() + "][" + data + "]";
    }
}
