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

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DotExpanderProcessorFactoryTests extends OpenSearchTestCase {

    public void testCreate() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field.field");
        config.put("path", "_path");
        DotExpanderProcessor processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
        assertThat(processor.getField(), equalTo("_field.field"));
        assertThat(processor.getPath(), equalTo("_path"));

        config = new HashMap<>();
        config.put("field", "_field.field");
        processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
        assertThat(processor.getField(), equalTo("_field.field"));
        assertThat(processor.getPath(), nullValue());
    }

    public void testValidFields() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        String[] fields = new String[] { "a.b", "a.b.c", "a.b.c.d", "ab.cd" };
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            config.put("path", "_path");
            DotExpanderProcessor processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
            assertThat(processor.getField(), equalTo(field));
            assertThat(processor.getPath(), equalTo("_path"));
        }
    }

    public void testCreate_fieldMissing() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("path", "_path");
        Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreate_invalidFields() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();
        String[] fields = new String[] { "a", "abc" };
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] field does not contain a dot"));
        }

        fields = new String[] { ".a", "a.", "." };
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] Field can't start or end with a dot"));
        }

        fields = new String[] { "a..b", "a...b", "a.b..c", "abc.def..hij" };
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] No space between dots"));
        }
    }

}
