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

package org.opensearch.search.aggregations.support;

import org.opensearch.core.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.core.xcontent.AbstractObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Helper class to parse multi values sources
 *
 * @opensearch.internal
 */
public final class MultiValuesSourceParseHelper {

    public static <T> void declareCommon(
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean formattable,
        ValueType expectedValueType
    ) {

        objectParser.declareField(MultiValuesSourceAggregationBuilder::userValueTypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            if (expectedValueType != null && valueType.isNotA(expectedValueType)) {
                throw new ParsingException(
                    p.getTokenLocation(),
                    "Aggregation ["
                        + objectParser.getName()
                        + "] was configured with an incompatible value type ["
                        + valueType
                        + "].  It can only work on value off type ["
                        + expectedValueType
                        + "]"
                );
            }
            return valueType;
        }, ValueType.VALUE_TYPE, ObjectParser.ValueType.STRING);

        if (formattable) {
            objectParser.declareField(
                MultiValuesSourceAggregationBuilder::format,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }
    }

    public static <VS extends ValuesSource, T> void declareField(
        String fieldName,
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean scriptable,
        boolean timezoneAware,
        boolean filterable
    ) {

        objectParser.declareField(
            (o, fieldConfig) -> o.field(fieldName, fieldConfig.build()),
            (p, c) -> MultiValuesSourceFieldConfig.PARSER.apply(scriptable, timezoneAware, filterable).parse(p, null),
            new ParseField(fieldName),
            ObjectParser.ValueType.OBJECT
        );
    }
}
