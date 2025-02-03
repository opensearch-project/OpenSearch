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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.math.BigInteger;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class StoredNumericValuesTests extends OpenSearchSingleNodeTestCase {
    public void testBytesAndNumericRepresentation() throws Exception {
        IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("field1")
            .field("type", "byte")
            .field("store", true)
            .endObject()
            .startObject("field2")
            .field("type", "short")
            .field("store", true)
            .endObject()
            .startObject("field3")
            .field("type", "integer")
            .field("store", true)
            .endObject()
            .startObject("field4")
            .field("type", "float")
            .field("store", true)
            .endObject()
            .startObject("field5")
            .field("type", "long")
            .field("store", true)
            .endObject()
            .startObject("field6")
            .field("type", "double")
            .field("store", true)
            .endObject()
            .startObject("field7")
            .field("type", "ip")
            .field("store", true)
            .endObject()
            .startObject("field8")
            .field("type", "ip")
            .field("store", true)
            .endObject()
            .startObject("field9")
            .field("type", "date")
            .field("store", true)
            .endObject()
            .startObject("field10")
            .field("type", "boolean")
            .field("store", true)
            .endObject()
            .startObject("field11")
            .field("type", "unsigned_long")
            .field("store", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", 1)
                        .field("field2", 1)
                        .field("field3", 1)
                        .field("field4", 1.1)
                        .startArray("field5")
                        .value(1)
                        .value(2)
                        .value(3)
                        .endArray()
                        .field("field6", 1.1)
                        .field("field7", "192.168.1.1")
                        .field("field8", "2001:db8::2:1")
                        .field("field9", "2016-04-05")
                        .field("field10", true)
                        .field("field11", "1")
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        writer.addDocument(doc.rootDoc());

        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        Set<String> fieldNames = Sets.newHashSet(
            "field1",
            "field2",
            "field3",
            "field4",
            "field5",
            "field6",
            "field7",
            "field8",
            "field9",
            "field10",
            "field11"
        );
        CustomFieldsVisitor fieldsVisitor = new CustomFieldsVisitor(fieldNames, false);
        searcher.storedFields().document(0, fieldsVisitor);

        fieldsVisitor.postProcess(mapperService::fieldType);
        assertThat(fieldsVisitor.fields().size(), equalTo(11));
        assertThat(fieldsVisitor.fields().get("field1").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field1").get(0), equalTo((byte) 1));

        assertThat(fieldsVisitor.fields().get("field2").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field2").get(0), equalTo((short) 1));

        assertThat(fieldsVisitor.fields().get("field3").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field3").get(0), equalTo(1));

        assertThat(fieldsVisitor.fields().get("field4").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field4").get(0), equalTo(1.1f));

        assertThat(fieldsVisitor.fields().get("field5").size(), equalTo(3));
        assertThat(fieldsVisitor.fields().get("field5").get(0), equalTo(1L));
        assertThat(fieldsVisitor.fields().get("field5").get(1), equalTo(2L));
        assertThat(fieldsVisitor.fields().get("field5").get(2), equalTo(3L));

        assertThat(fieldsVisitor.fields().get("field6").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field6").get(0), equalTo(1.1));

        assertThat(fieldsVisitor.fields().get("field7").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field7").get(0), equalTo("192.168.1.1"));

        assertThat(fieldsVisitor.fields().get("field8").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field8").get(0), equalTo("2001:db8::2:1"));

        assertThat(fieldsVisitor.fields().get("field9").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field9").get(0), equalTo("2016-04-05T00:00:00.000Z"));

        assertThat(fieldsVisitor.fields().get("field10").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field10").get(0), equalTo(true));

        assertThat(fieldsVisitor.fields().get("field11").size(), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field11").get(0), equalTo(BigInteger.valueOf(1)));

        reader.close();
        writer.close();
    }

    public void testFieldsVisitorValidateIncludesExcludes() throws Exception {
        Set<String> fieldNames = Sets.newHashSet(
            "field1",
            "field2",
            "field3",
            "field4",
            "field5",
            "field6",
            "field7",
            "field8",
            "field9",
            "field10",
            "field11"
        );
        String[] includes = { "field1", "field2", "field3" };
        String[] excludes = { "field7", "field8" };

        CustomFieldsVisitor fieldsVisitor = new CustomFieldsVisitor(fieldNames, false, includes, excludes);

        assertArrayEquals(fieldsVisitor.includes(), includes);
        assertArrayEquals(fieldsVisitor.excludes(), excludes);

        FieldsVisitor fieldsVisitor1 = new FieldsVisitor(false, includes, excludes);
        assertArrayEquals(fieldsVisitor1.includes(), includes);
        assertArrayEquals(fieldsVisitor1.excludes(), excludes);

        FieldsVisitor fieldsVisitor2 = new FieldsVisitor(false);
        assertArrayEquals(fieldsVisitor2.includes(), Strings.EMPTY_ARRAY);
        assertArrayEquals(fieldsVisitor2.excludes(), Strings.EMPTY_ARRAY);

    }
}
