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

package org.opensearch.search.fetch.subphase;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Wrapper around a field name and the format that should be used to
 * display values of this field.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class FieldAndFormat implements Writeable, ToXContentObject {
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField FORMAT_FIELD = new ParseField("format");

    private static final ConstructingObjectParser<FieldAndFormat, Void> PARSER = new ConstructingObjectParser<>(
        "docvalues_field",
        a -> new FieldAndFormat((String) a[0], (String) a[1])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("field"));
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("format"));
    }

    /**
     * Parse a {@link FieldAndFormat} from some {@link XContent}.
     */
    public static FieldAndFormat fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            return new FieldAndFormat(parser.text(), null);
        } else {
            return PARSER.apply(parser, null);
        }
    }

    /** The name of the field. */
    public final String field;

    /** The format of the field, or {@code null} if defaults should be used. */
    public final String format;

    /** Sole constructor. */
    public FieldAndFormat(String field, @Nullable String format) {
        this.field = Objects.requireNonNull(field);
        this.format = format;
    }

    /** Serialization constructor. */
    public FieldAndFormat(StreamInput in) throws IOException {
        this.field = in.readString();
        format = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(format);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldAndFormat that = (FieldAndFormat) o;
        return Objects.equals(field, that.field) && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        int h = field.hashCode();
        h = 31 * h + Objects.hashCode(format);
        return h;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        builder.endObject();
        return builder;
    }
}
