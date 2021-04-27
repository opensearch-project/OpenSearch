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

package org.opensearch.search.aggregations.bucket.composite;

import org.opensearch.LegacyESVersion;
import org.opensearch.common.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.AbstractObjectParser;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.ToXContent.Params;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class CompositeValuesSourceParserHelper {

    static <VB extends CompositeValuesSourceBuilder<VB>, T> void declareValuesSourceFields(AbstractObjectParser<VB, T> objectParser) {
        objectParser.declareField(VB::field, XContentParser::text,
            new ParseField("field"), ObjectParser.ValueType.STRING);
        objectParser.declareBoolean(VB::missingBucket, new ParseField("missing_bucket"));

        objectParser.declareField(VB::userValuetypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            return valueType;
        }, new ParseField("value_type"), ObjectParser.ValueType.STRING);

        objectParser.declareField(VB::script,
            (parser, context) -> Script.parse(parser), Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);

        objectParser.declareField(VB::order,  XContentParser::text, new ParseField("order"), ObjectParser.ValueType.STRING);
    }

    public static void writeTo(CompositeValuesSourceBuilder<?> builder, StreamOutput out) throws IOException {
        final byte code;
        if (builder.getClass() == TermsValuesSourceBuilder.class) {
            code = 0;
        } else if (builder.getClass() == DateHistogramValuesSourceBuilder.class) {
            code = 1;
        } else if (builder.getClass() == HistogramValuesSourceBuilder.class) {
            code = 2;
        } else if (builder.getClass() == GeoTileGridValuesSourceBuilder.class) {
            if (out.getVersion().before(LegacyESVersion.V_7_5_0)) {
                throw new IOException("Attempting to serialize [" + builder.getClass().getSimpleName()
                    + "] to a node with unsupported version [" + out.getVersion() + "]");
            }
            code = 3;
        } else {
            throw new IOException("invalid builder type: " + builder.getClass().getSimpleName());
        }
        out.writeByte(code);
        builder.writeTo(out);
    }

    public static CompositeValuesSourceBuilder<?> readFrom(StreamInput in) throws IOException {
        int code = in.readByte();
        switch(code) {
            case 0:
                return new TermsValuesSourceBuilder(in);
            case 1:
                return new DateHistogramValuesSourceBuilder(in);
            case 2:
                return new HistogramValuesSourceBuilder(in);
            case 3:
                return new GeoTileGridValuesSourceBuilder(in);
            default:
                throw new IOException("Invalid code " + code);
        }
    }

    public static CompositeValuesSourceBuilder<?> fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String name = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String type = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        final CompositeValuesSourceBuilder<?> builder;
        switch(type) {
            case TermsValuesSourceBuilder.TYPE:
                builder = TermsValuesSourceBuilder.parse(name, parser);
                break;
            case DateHistogramValuesSourceBuilder.TYPE:
                builder = DateHistogramValuesSourceBuilder.PARSER.parse(parser, name);
                break;
            case HistogramValuesSourceBuilder.TYPE:
                builder = HistogramValuesSourceBuilder.parse(name, parser);
                break;
            case GeoTileGridValuesSourceBuilder.TYPE:
                builder = GeoTileGridValuesSourceBuilder.parse(name, parser);
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "invalid source type: " + type);
        }
        parser.nextToken();
        parser.nextToken();
        return builder;
    }

    public static XContentBuilder toXContent(CompositeValuesSourceBuilder<?> source, XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        builder.startObject(source.name());
        source.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
