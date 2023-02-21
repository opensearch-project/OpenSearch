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

package org.opensearch.index.query.functionscore;

import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.lucene.search.function.ScoreFunction;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 *
 * @opensearch.internal
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder<FieldValueFactorFunctionBuilder> {
    public static final String NAME = "field_value_factor";
    public static final FieldValueFactorFunction.Modifier DEFAULT_MODIFIER = FieldValueFactorFunction.Modifier.NONE;
    public static final float DEFAULT_FACTOR = 1;

    private final String field;
    private float factor = DEFAULT_FACTOR;
    private Double missing;
    private FieldValueFactorFunction.Modifier modifier = DEFAULT_MODIFIER;

    public FieldValueFactorFunctionBuilder(String fieldName) {
        this(fieldName, null);
    }

    public FieldValueFactorFunctionBuilder(String fieldName, @Nullable String functionName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field_value_factor: field must not be null");
        }
        this.field = fieldName;
        setFunctionName(functionName);
    }

    /**
     * Read from a stream.
     */
    public FieldValueFactorFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        factor = in.readFloat();
        missing = in.readOptionalDouble();
        modifier = FieldValueFactorFunction.Modifier.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloat(factor);
        out.writeOptionalDouble(missing);
        modifier.writeTo(out);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String fieldName() {
        return this.field;
    }

    public FieldValueFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    public float factor() {
        return this.factor;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(double missing) {
        this.missing = missing;
        return this;
    }

    public Double missing() {
        return this.missing;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        if (modifier == null) {
            throw new IllegalArgumentException("field_value_factor: modifier must not be null");
        }
        this.modifier = modifier;
        return this;
    }

    public FieldValueFactorFunction.Modifier modifier() {
        return this.modifier;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        builder.field("factor", factor);
        if (missing != null) {
            builder.field("missing", missing);
        }
        builder.field("modifier", modifier.name().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    @Override
    protected boolean doEquals(FieldValueFactorFunctionBuilder functionBuilder) {
        return Objects.equals(this.field, functionBuilder.field)
            && Objects.equals(this.factor, functionBuilder.factor)
            && Objects.equals(this.missing, functionBuilder.missing)
            && Objects.equals(this.modifier, functionBuilder.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.factor, this.missing, this.modifier);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        MappedFieldType fieldType = context.getMapperService().fieldType(field);
        IndexNumericFieldData fieldData = null;
        if (fieldType == null) {
            if (missing == null) {
                throw new OpenSearchException("Unable to find a field mapper for field [" + field + "]. No 'missing' value defined.");
            }
        } else {
            fieldData = context.getForField(fieldType);
        }
        return new FieldValueFactorFunction(field, factor, modifier, missing, fieldData, getFunctionName());
    }

    public static FieldValueFactorFunctionBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        String currentFieldName = null;
        String field = null;
        float boostFactor = FieldValueFactorFunctionBuilder.DEFAULT_FACTOR;
        FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.NONE;
        Double missing = null;
        XContentParser.Token token;
        String functionName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("factor".equals(currentFieldName)) {
                    boostFactor = parser.floatValue();
                } else if ("modifier".equals(currentFieldName)) {
                    modifier = FieldValueFactorFunction.Modifier.fromString(parser.text());
                } else if ("missing".equals(currentFieldName)) {
                    missing = parser.doubleValue();
                } else if (FunctionScoreQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    functionName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            } else if ("factor".equals(currentFieldName)
                && (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT)) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] field 'factor' does not support lists or objects"
                    );
                }
        }

        if (field == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] required field 'field' missing");
        }

        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = new FieldValueFactorFunctionBuilder(field, functionName).factor(
            boostFactor
        ).modifier(modifier);
        if (missing != null) {
            fieldValueFactorFunctionBuilder.missing(missing);
        }
        return fieldValueFactorFunctionBuilder;
    }
}
