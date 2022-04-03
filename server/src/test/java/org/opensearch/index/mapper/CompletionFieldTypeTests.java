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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompletionFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        NamedAnalyzer defaultAnalyzer = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());

        MappedFieldType fieldType = new CompletionFieldMapper.CompletionFieldType("name", defaultAnalyzer, Collections.emptyMap());

        assertEquals(Collections.singletonList("value"), fetchSourceValue(fieldType, "value"));

        List<String> list = Arrays.asList("first", "second");
        assertEquals(list, fetchSourceValue(fieldType, list));

        Map<String, Object> object = new HashMap<>();
        object.put("input", Arrays.asList("first", "second"));
        object.put("weight", "2.718");
        assertEquals(Collections.singletonList(object), fetchSourceValue(fieldType, object));
    }
}
