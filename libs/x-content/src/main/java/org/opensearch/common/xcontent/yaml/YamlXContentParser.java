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

package org.opensearch.common.xcontent.yaml;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

import java.io.IOException;

import org.snakeyaml.engine.v2.resolver.ScalarResolver;
import tools.jackson.core.JsonParser;
import tools.jackson.dataformat.yaml.YAMLParser;

public class YamlXContentParser extends JsonXContentParser {

    public YamlXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
    }

    @Override
    public XContentType contentType() {
        return XContentType.YAML;
    }

    /**
     * Normalizes YAML boolean variants (e.g. {@code True}, {@code False}, {@code TRUE}, {@code FALSE})
     * to lowercase {@code "true"} / {@code "false"}.
     *
     * The underlying Jackson YAML parser uses {@code JsonScalarResolver} which only recognizes
     * lowercase {@code true}/{@code false} as boolean tokens. Mixed-case variants allowed by the
     * YAML 1.1/1.2 core schema (e.g. {@code True}, {@code FALSE}) are therefore emitted as
     * {@code VALUE_STRING} tokens with their original casing preserved, this we normalize
     * such values at parse time.
     *
     * The better way to deal with that would be be to provide own dedicated {@link ScalarResolver}
     * implementation but unfortunately {@link YAMLParser} does not provide a way to set or override it
     * currently.
     */
    @Override
    public String text() throws IOException {
        String text = super.text();
        if (currentToken() == Token.VALUE_STRING) {
            if ("true".equalsIgnoreCase(text)) {
                return "true";
            } else if ("false".equalsIgnoreCase(text)) {
                return "false";
            }
        }
        return text;
    }
}
