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

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class NullValueTests extends MapperServiceTestCase {

    public void testNullNullValue() throws Exception {

        String[] typesToTest = { "integer", "long", "double", "float", "short", "date", "ip", "keyword", "boolean", "byte", "geo_point" };

        for (String type : typesToTest) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).nullField("null_value")));

            mapper.parse(source(b -> b.nullField("field")));

            ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
            XContentBuilder b = JsonXContent.contentBuilder().startObject();
            mapper.mapping().toXContent(b, params);
            b.endObject();
            assertThat(b.toString(), containsString("\"null_value\":null"));
        }
    }
}
