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

package org.opensearch.common.document;

import com.google.protobuf.ByteString;
import org.apache.lucene.util.SuppressForbidden;
import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;
import org.opensearch.search.SearchHit;
import org.opensearch.server.proto.FetchSearchResultProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.parseFieldsValue;

/**
 * A single field name and values part of {@link SearchHit} and {@link GetResult}.
 *
 * @see SearchHit
 * @see GetResult
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocumentField implements Writeable, ToXContentFragment, Iterable<Object> {

    private final String name;
    private final List<Object> values;
    private FetchSearchResultProto.SearchHit.DocumentField documentField;

    public DocumentField(StreamInput in) throws IOException {
        name = in.readString();
        values = in.readList(StreamInput::readGenericValue);
    }

    public DocumentField(String name, List<Object> values) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    @SuppressForbidden(reason = "We need to read from a byte array")
    public DocumentField(byte[] in) throws IOException {
        assert FeatureFlags.isEnabled(FeatureFlags.PROTOBUF) : "protobuf feature flag is not enabled";
        documentField = FetchSearchResultProto.SearchHit.DocumentField.parseFrom(in);
        name = documentField.getName();
        values = new ArrayList<>();
        for (ByteString value : documentField.getValuesList()) {
            InputStream is = new ByteArrayInputStream(value.toByteArray());
            try (ObjectInputStream ois = new ObjectInputStream(is)) {
                values.add(ois.readObject());
            } catch (ClassNotFoundException e) {
                throw new OpenSearchException(e);
            }
        }
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * The first value of the hit.
     */
    public <V> V getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return (V) values.get(0);
    }

    /**
     * The field values.
     */
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(values, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(name);
        for (Object value : values) {
            // This call doesn't really need to support writing any kind of object, since the values
            // here are always serializable to xContent. Each value could be a leaf types like a string,
            // number, or boolean, a list of such values, or a map of such values with string keys.
            builder.value(value);
        }
        builder.endArray();
        return builder;
    }

    public static DocumentField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
        List<Object> values = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            values.add(parseFieldsValue(parser));
        }
        return new DocumentField(fieldName, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DocumentField objects = (DocumentField) o;
        return Objects.equals(name, objects.name) && Objects.equals(values, objects.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values);
    }

    @Override
    public String toString() {
        return "DocumentField{" + "name='" + name + '\'' + ", values=" + values + '}';
    }
}
