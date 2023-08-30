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

package org.opensearch.search.lookup;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafFieldsLookupTests extends OpenSearchTestCase {
    private LeafFieldsLookup fieldsLookup;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        // Add 10 when valueForDisplay is called so it is easy to be sure it *was* called
        when(fieldType.valueForDisplay(any())).then(invocation -> (Double) invocation.getArguments()[0] + 10);

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("field")).thenReturn(fieldType);
        when(mapperService.fieldType("alias")).thenReturn(fieldType);

        FieldInfo mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false
        );

        LeafReader leafReader = mock(LeafReader.class);
        doAnswer(invocation -> new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                visitor.doubleField(mockFieldInfo, 2.718);
            }
        }).when(leafReader).storedFields();

        fieldsLookup = new LeafFieldsLookup(mapperService, leafReader);
    }

    public void testBasicLookup() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("field");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }

    public void testLookupWithFieldAlias() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("alias");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }
}
