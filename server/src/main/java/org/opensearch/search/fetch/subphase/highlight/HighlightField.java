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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.fetch.subphase.highlight;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A field highlighted with its highlighted fragments.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class HighlightField implements ToXContentFragment, Writeable {

    private String name;

    private Text[] fragments;

    public HighlightField(StreamInput in) throws IOException {
        name = in.readString();
        if (in.readBoolean()) {
            int size = in.readVInt();
            if (size == 0) {
                fragments = Text.EMPTY_ARRAY;
            } else {
                fragments = new Text[size];
                for (int i = 0; i < size; i++) {
                    fragments[i] = in.readText();
                }
            }
        }
    }

    public HighlightField(String name, Text[] fragments) {
        this.name = Objects.requireNonNull(name, "missing highlight field name");
        this.fragments = fragments;
    }

    /**
     * The name of the field highlighted.
     */
    public String name() {
        return name;
    }

    /**
     * The name of the field highlighted.
     */
    public String getName() {
        return name();
    }

    /**
     * The highlighted fragments. {@code null} if failed to highlight (for example, the field is not stored).
     */
    public Text[] fragments() {
        return fragments;
    }

    /**
     * The highlighted fragments. {@code null} if failed to highlight (for example, the field is not stored).
     */
    public Text[] getFragments() {
        return fragments();
    }

    @Override
    public String toString() {
        return "[" + name + "], fragments[" + Arrays.toString(fragments) + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (fragments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(fragments.length);
            for (Text fragment : fragments) {
                out.writeText(fragment);
            }
        }
    }

    public static HighlightField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        Text[] fragments = null;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            List<Text> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                values.add(new Text(parser.text()));
            }
            fragments = values.toArray(new Text[0]);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            fragments = null;
        } else {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token type [" + token + "]");
        }
        return new HighlightField(fieldName, fragments);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(name);
        if (fragments == null) {
            builder.nullValue();
        } else {
            builder.startArray();
            for (Text fragment : fragments) {
                builder.value(fragment);
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HighlightField other = (HighlightField) obj;
        return Objects.equals(name, other.name) && Arrays.equals(fragments, other.fragments);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, Arrays.hashCode(fragments));
    }

}
