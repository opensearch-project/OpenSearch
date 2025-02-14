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
package org.opensearch.index.mapper;

import org.apache.lucene.search.FieldExistsQuery;
import org.opensearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Arrays;

public class DocCountFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        QueryShardException e = expectThrows(QueryShardException.class, () -> ft.termQuery(10L, randomMockShardContext()));
        assertEquals("Field [_doc_count] of type [_doc_count] is not searchable", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null)
        );
        assertEquals("Field [_doc_count] of type [_doc_count] does not support range queries", e.getMessage());
    }

    public void testExistsQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        assertTrue(ft.existsQuery(randomMockShardContext()) instanceof FieldExistsQuery);
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new DocCountFieldMapper.DocCountFieldType();
        assertEquals(Arrays.asList(14L), fetchSourceValue(fieldType, 14));
        assertEquals(Arrays.asList(14L), fetchSourceValue(fieldType, "14"));
        assertEquals(Arrays.asList(1L), fetchSourceValue(fieldType, ""));
        assertEquals(Arrays.asList(1L), fetchSourceValue(fieldType, null));
    }
}
