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

package org.opensearch.test.rest.yaml.section;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 * Utility methods used for parsing test sections.
 */
class ParserUtils {
    private ParserUtils() {
        // Do not build.
    }

    public static String parseField(XContentParser parser) throws IOException {
        parser.nextToken();
        assert parser.currentToken().isValue();
        String field = parser.text();
        parser.nextToken();
        return field;
    }

    public static Tuple<String, Object> parseTuple(XContentParser parser) throws IOException {
        parser.nextToken();
        advanceToFieldName(parser);
        Map<String, Object> map = parser.map();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        parser.nextToken();

        if (map.size() != 1) {
            throw new IllegalArgumentException("expected key value pair but found an object with " + map.size() + " fields");
        }

        Map.Entry<String, Object> entry = map.entrySet().iterator().next();
        return Tuple.tuple(entry.getKey(), entry.getValue());
    }

    public static Map<String, Object> parseFields(XContentParser parser) throws IOException {
        parser.nextToken();
        advanceToFieldName(parser);
        Map<String, Object> map = parser.map();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        parser.nextToken();

        if (map.size() == 0) {
            throw new IllegalArgumentException("expected at least one field but found an empty object");
        }

        return map;
    }

    public static void advanceToFieldName(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        // we are in the beginning, haven't called nextToken yet
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException(
                "malformed test section: field name expected but found " + token + " at " + parser.getTokenLocation()
            );
        }
    }
}
