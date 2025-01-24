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

package org.opensearch.search.fetch;

import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchPhaseTests extends OpenSearchTestCase {
    public void testSequentialDocs() {
        FetchPhase.DocIdToIndex[] docs = new FetchPhase.DocIdToIndex[10];
        int start = randomIntBetween(0, Short.MAX_VALUE);
        for (int i = 0; i < 10; i++) {
            docs[i] = new FetchPhase.DocIdToIndex(start, i);
            ++start;
        }
        assertTrue(FetchPhase.hasSequentialDocs(docs));

        int from = randomIntBetween(0, 9);
        start = docs[from].docId;
        for (int i = from; i < 10; i++) {
            start += randomIntBetween(2, 10);
            docs[i] = new FetchPhase.DocIdToIndex(start, i);
        }
        assertFalse(FetchPhase.hasSequentialDocs(docs));
    }

    public void testFieldsVisitorsInFetchPhase() {

        FetchPhase fetchPhase = new FetchPhase(new ArrayList<>());
        SearchContext mockSearchContext = mock(SearchContext.class);
        when(mockSearchContext.docIdsToLoadSize()).thenReturn(1);
        when(mockSearchContext.docIdsToLoad()).thenReturn(new int[] { 1 });
        String[] includes = new String[] { "field1", "field2" };
        String[] excludes = new String[] { "field7", "field8" };

        FetchSourceContext mockFetchSourceContext = new FetchSourceContext(true, includes, excludes);
        when(mockSearchContext.hasFetchSourceContext()).thenReturn(true);
        when(mockSearchContext.fetchSourceContext()).thenReturn(mockFetchSourceContext);

        // Case 1
        // if storedFieldsContext is null
        FieldsVisitor fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, null);
        assertArrayEquals(fieldsVisitor.excludes(), excludes);
        assertArrayEquals(fieldsVisitor.includes(), includes);

        // Case 2
        // if storedFieldsContext is not null
        StoredFieldsContext storedFieldsContext = mock(StoredFieldsContext.class);
        when(mockSearchContext.storedFieldsContext()).thenReturn(storedFieldsContext);

        fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, null);
        assertNull(fieldsVisitor);

        // Case 3
        // if storedFieldsContext is true but fieldNames are empty
        when(storedFieldsContext.fetchFields()).thenReturn(true);
        when(storedFieldsContext.fieldNames()).thenReturn(List.of());
        fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, Collections.emptyMap());
        assertArrayEquals(fieldsVisitor.excludes(), excludes);
        assertArrayEquals(fieldsVisitor.includes(), includes);

        // Case 4
        // if storedToRequested Fields is not empty
        // creates an instance of CustomFieldsVisitor
        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        storedToRequestedFields.put("test_field_key", Set.of("test_field_value"));

        fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, storedToRequestedFields);

        assertTrue(fieldsVisitor instanceof CustomFieldsVisitor);
        assertArrayEquals(fieldsVisitor.excludes(), excludes);
        assertArrayEquals(fieldsVisitor.includes(), includes);

    }

}
