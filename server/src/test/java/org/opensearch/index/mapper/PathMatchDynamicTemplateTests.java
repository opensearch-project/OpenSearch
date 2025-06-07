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

import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class PathMatchDynamicTemplateTests extends OpenSearchSingleNodeTestCase {
    // public void testSimple() throws Exception {
    // String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/dynamictemplate/pathmatch/test-mapping.json");
    // IndexService index = createIndex("test");
    // client().admin().indices().preparePutMapping("test").setSource(mapping, MediaTypeRegistry.JSON).get();
    //
    // MapperService mapperService = index.mapperService();
    //
    // byte[] json = copyToBytesFromClasspath("/org/opensearch/index/mapper/dynamictemplate/pathmatch/test-data.json");
    // ParsedDocument parsedDoc = mapperService.documentMapper()
    // .parse(new SourceToParse("test", "1", new BytesArray(json), MediaTypeRegistry.JSON));
    // client().admin()
    // .indices()
    // .preparePutMapping("test")
    // .setSource(parsedDoc.dynamicMappingsUpdate().toString(), MediaTypeRegistry.JSON)
    // .get();
    // Document doc = parsedDoc.rootDoc();
    //
    // IndexableField f = doc.getField("name");
    // assertThat(f.name(), equalTo("name"));
    // assertThat(f.stringValue(), equalTo("top_level"));
    // assertThat(f.fieldType().stored(), equalTo(false));
    //
    // assertThat(mapperService.fieldType("name").isStored(), equalTo(false));
    //
    // f = doc.getField("obj1.name");
    // assertThat(f.name(), equalTo("obj1.name"));
    // assertThat(f.fieldType().stored(), equalTo(true));
    //
    // assertThat(mapperService.fieldType("obj1.name").isStored(), equalTo(true));
    //
    // f = doc.getField("obj1.obj2.name");
    // assertThat(f.name(), equalTo("obj1.obj2.name"));
    // assertThat(f.fieldType().stored(), equalTo(false));
    //
    // assertThat(mapperService.fieldType("obj1.obj2.name").isStored(), equalTo(false));
    //
    // // verify more complex path_match expressions
    // assertNotNull(mapperService.fieldType("obj3.obj4.prop1").getTextSearchInfo());
    // }
}
