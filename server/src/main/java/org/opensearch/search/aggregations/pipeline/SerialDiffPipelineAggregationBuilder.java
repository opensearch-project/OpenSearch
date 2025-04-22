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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.opensearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

/**
 * Aggregation Builder for serial_diff agg
 *
 * @opensearch.internal
 */
public class SerialDiffPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<SerialDiffPipelineAggregationBuilder> {
    public static final String NAME = "serial_diff";

    private static final ParseField GAP_POLICY = new ParseField("gap_policy");
    private static final ParseField LAG = new ParseField("lag");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int lag = 1;

    public SerialDiffPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public SerialDiffPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
        lag = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        out.writeVInt(lag);
    }

    /**
     * Sets the lag to use when calculating the serial difference.
     */
    public SerialDiffPipelineAggregationBuilder lag(int lag) {
        if (lag <= 0) {
            throw new IllegalArgumentException("[lag] must be a positive integer: [" + name + "]");
        }
        this.lag = lag;
        return this;
    }

    /**
     * Gets the lag to use when calculating the serial difference.
     */
    public int lag() {
        return lag;
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public SerialDiffPipelineAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Sets the GapPolicy to use on the output of this aggregation.
     */
    public SerialDiffPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Gets the GapPolicy to use on the output of this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new SerialDiffPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, lag, metadata);
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateParentAggSequentiallyOrdered(NAME, name);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        builder.field(LAG.getPreferredName(), lag);
        return builder;
    }

    public static SerialDiffPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        GapPolicy gapPolicy = null;
        Integer lag = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName, parser.getDeprecationHandler())) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName, parser.getDeprecationHandler())) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (LAG.match(currentFieldName, parser.getDeprecationHandler())) {
                    lag = parser.intValue(true);
                    if (lag <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Lag must be a positive, non-zero integer.  Value supplied was"
                                + lag
                                + " in ["
                                + reducerName
                                + "]: ["
                                + currentFieldName
                                + "]."
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[0]);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " in [" + reducerName + "].",
                    parser.getTokenLocation()
                );
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Missing required field [" + BUCKETS_PATH.getPreferredName() + "] for derivative aggregation [" + reducerName + "]"
            );
        }

        SerialDiffPipelineAggregationBuilder factory = new SerialDiffPipelineAggregationBuilder(reducerName, bucketsPaths[0]);
        if (lag != null) {
            factory.lag(lag);
        }
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        return factory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, gapPolicy, lag);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        SerialDiffPipelineAggregationBuilder other = (SerialDiffPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format) && Objects.equals(gapPolicy, other.gapPolicy) && Objects.equals(lag, other.lag);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
