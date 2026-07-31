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

package org.opensearch.index.mapper;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class DynamicTemplateTests extends OpenSearchTestCase {

    public void testParseUnknownParam() throws Exception {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        templateDef.put("random_param", "random_value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DynamicTemplate.parse("my_template", templateDef));
        assertEquals("Illegal dynamic template parameter: [random_param]", e.getMessage());
    }

    public void testParseUnknownMatchType() {
        Map<String, Object> templateDef2 = new HashMap<>();
        templateDef2.put("match_mapping_type", "text");
        templateDef2.put("mapping", Collections.singletonMap("store", true));
        // With no plugin registry (the public 2-arg parse), an unknown match_mapping_type fails immediately.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DynamicTemplate.parse("my_template", templateDef2));
        assertThat(e.getMessage(), containsString("No field type matched on [text]"));
    }

    public void testParseInvalidRegex() {
        for (String param : new String[] { "path_match", "match", "path_unmatch", "unmatch" }) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo");
            templateDef.put(param, "*a");
            templateDef.put("match_pattern", "regex");
            templateDef.put("mapping", Collections.singletonMap("store", true));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DynamicTemplate.parse("my_template", templateDef)
            );
            assertEquals("Pattern [*a] of type [regex] is invalid. Cannot create dynamic template [my_template].", e.getMessage());
        }
    }

    public void testMatchAllTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "*");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match("a.b", "b", randomFrom(XContentFieldType.values())));
    }

    public void testMatchTypeTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match("a.b", "b", XContentFieldType.STRING));
        assertFalse(template.match("a.b", "b", XContentFieldType.BOOLEAN));
    }

    public void testSerialization() throws Exception {
        // type-based template
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        XContentBuilder builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match_mapping_type\":\"string\",\"mapping\":{\"store\":true}}", builder.toString());

        // name-based template
        templateDef = new HashMap<>();
        templateDef.put("match", "*name");
        templateDef.put("unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match\":\"*name\",\"unmatch\":\"first_name\",\"mapping\":{\"store\":true}}", builder.toString());

        // path-based template
        templateDef = new HashMap<>();
        templateDef.put("path_match", "*name");
        templateDef.put("path_unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"path_match\":\"*name\",\"path_unmatch\":\"first_name\",\"mapping\":{\"store\":true}}", builder.toString());

        // regex matching
        templateDef = new HashMap<>();
        templateDef.put("match", "^a$");
        templateDef.put("match_pattern", "regex");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match\":\"^a$\",\"match_pattern\":\"regex\",\"mapping\":{\"store\":true}}", builder.toString());
    }
}
