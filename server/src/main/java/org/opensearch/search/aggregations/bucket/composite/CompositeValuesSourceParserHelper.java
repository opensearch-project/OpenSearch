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

import org.opensearch.core.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.AbstractObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder.AGGREGATION_TYPE_TO_COMPOSITE_VALUE_SOURCE_READER;
import static org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder.BUILDER_CLASS_TO_AGGREGATION_TYPE;
import static org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder.BUILDER_CLASS_TO_BYTE_CODE;
import static org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder.BUILDER_TYPE_TO_PARSER;
import static org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder.BYTE_CODE_TO_COMPOSITE_VALUE_SOURCE_READER;

/**
 * Helper class for obtaining values source parsers for different aggs
 *
 * @opensearch.internal
 */
public class CompositeValuesSourceParserHelper {

    private static final int AGGREGATION_TYPE_REFERENCE = Byte.MAX_VALUE;

    public static <VB extends CompositeValuesSourceBuilder<VB>, T> void declareValuesSourceFields(
        AbstractObjectParser<VB, T> objectParser
    ) {
        objectParser.declareField(VB::field, XContentParser::text, new ParseField("field"), ObjectParser.ValueType.STRING);
        objectParser.declareBoolean(VB::missingBucket, new ParseField("missing_bucket"));
        objectParser.declareString(VB::missingOrder, new ParseField(MissingOrder.NAME));

        objectParser.declareField(VB::userValuetypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            return valueType;
        }, new ParseField("value_type"), ObjectParser.ValueType.STRING);

        objectParser.declareField(
            VB::script,
            (parser, context) -> Script.parse(parser),
            Script.SCRIPT_PARSE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );

        objectParser.declareField(VB::order, XContentParser::text, new ParseField("order"), ObjectParser.ValueType.STRING);
    }

    public static void writeTo(CompositeValuesSourceBuilder<?> builder, StreamOutput out) throws IOException {
        int code = Byte.MIN_VALUE;
        String aggregationType = null;
        if (builder.getClass() == TermsValuesSourceBuilder.class) {
            code = 0;
        } else if (builder.getClass() == DateHistogramValuesSourceBuilder.class) {
            code = 1;
        } else if (builder.getClass() == HistogramValuesSourceBuilder.class) {
            code = 2;
        } else {
            if (!BUILDER_CLASS_TO_BYTE_CODE.containsKey(builder.getClass())
                && !BUILDER_CLASS_TO_AGGREGATION_TYPE.containsKey(builder.getClass())) {
                throw new IOException("invalid builder type: " + builder.getClass().getSimpleName());
            }
            aggregationType = BUILDER_CLASS_TO_AGGREGATION_TYPE.get(builder.getClass());
            if (BUILDER_CLASS_TO_BYTE_CODE.containsKey(builder.getClass())) {
                code = BUILDER_CLASS_TO_BYTE_CODE.get(builder.getClass());
            }
        }

        if (code != Byte.MIN_VALUE) {
            out.writeByte((byte) code);
        } else if (!BUILDER_CLASS_TO_BYTE_CODE.containsKey(builder.getClass())) {
            /*
             * This is added for backward compatibility when 1 data node is using the new code which is using the
             * aggregation type and another is using the only byte code in the serialisation.
             */
            out.writeByte((byte) AGGREGATION_TYPE_REFERENCE);
            assert aggregationType != null;
            out.writeString(aggregationType);
        }
        builder.writeTo(out);
    }

    public static CompositeValuesSourceBuilder<?> readFrom(StreamInput in) throws IOException {
        int code = in.readByte();
        switch (code) {
            case 0:
                return new TermsValuesSourceBuilder(in);
            case 1:
                return new DateHistogramValuesSourceBuilder(in);
            case 2:
                return new HistogramValuesSourceBuilder(in);
            case AGGREGATION_TYPE_REFERENCE:
                final String aggregationType = in.readString();
                if (!AGGREGATION_TYPE_TO_COMPOSITE_VALUE_SOURCE_READER.containsKey(aggregationType)) {
                    throw new IOException("Invalid aggregation type " + aggregationType);
                }
                return (CompositeValuesSourceBuilder<?>) AGGREGATION_TYPE_TO_COMPOSITE_VALUE_SOURCE_READER.get(aggregationType).read(in);
            default:
                if (!BYTE_CODE_TO_COMPOSITE_VALUE_SOURCE_READER.containsKey(code)) {
                    throw new IOException("Invalid code " + code);
                }
                return (CompositeValuesSourceBuilder<?>) BYTE_CODE_TO_COMPOSITE_VALUE_SOURCE_READER.get(code).read(in);
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
        switch (type) {
            case TermsValuesSourceBuilder.TYPE:
                builder = TermsValuesSourceBuilder.parse(name, parser);
                break;
            case DateHistogramValuesSourceBuilder.TYPE:
                builder = DateHistogramValuesSourceBuilder.PARSER.parse(parser, name);
                break;
            case HistogramValuesSourceBuilder.TYPE:
                builder = HistogramValuesSourceBuilder.parse(name, parser);
                break;
            default:
                if (!BUILDER_TYPE_TO_PARSER.containsKey(type)) {
                    throw new ParsingException(parser.getTokenLocation(), "invalid source type: " + type);
                }
                builder = BUILDER_TYPE_TO_PARSER.get(type).parse(name, parser);
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
