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

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
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
        when(mockSearchContext.innerHits()).thenReturn(new InnerHitsContext());

        // Case 1
        // if storedFieldsContext is null
        FieldsVisitor fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, null);
        assertArrayEquals(fieldsVisitor.excludes(), excludes);
        assertArrayEquals(fieldsVisitor.includes(), includes);
        assertArrayEquals(fieldsVisitor.codecExcludes(), excludes);

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
        assertArrayEquals(fieldsVisitor.codecExcludes(), excludes);

        // Case 4
        // if storedToRequested Fields is not empty
        // creates an instance of CustomFieldsVisitor
        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        storedToRequestedFields.put("test_field_key", Set.of("test_field_value"));

        fieldsVisitor = fetchPhase.createStoredFieldsVisitor(mockSearchContext, storedToRequestedFields);

        assertTrue(fieldsVisitor instanceof CustomFieldsVisitor);
        assertArrayEquals(fieldsVisitor.excludes(), excludes);
        assertArrayEquals(fieldsVisitor.includes(), includes);
        assertArrayEquals(fieldsVisitor.codecExcludes(), excludes);
    }

    public void testCodecSourceExcludes() {
        // A user source exclude is dropped from the codec excludes when an inner hit requests an
        // overlapping field (so it stays available at the codec level), while the returned _source
        // includes/excludes are never mutated. Inner-hit fields may be requested via _source
        // includes (viaSource=true) or via fetch fields with source disabled (viaSource=false), and
        // both excludes and includes support simple-match wildcards.
        //
        // rootIncludes | excludes | innerField | viaSource | -> codecExcludes
        // ["field3"] | field1,field2 | field1 | source | -> [field2] literal, source include
        // null | field1,field2 | field2 | fetch | -> [field1] literal, fetch field
        // null | obj.*,secret* | obj.field | source | -> [secret*] wildcard exclude vs literal
        // null | foo*,bar* | bar* | fetch | -> [foo*] identical wildcard patterns
        // null | foo*,x.y | *bar | source | -> [x.y] "foo*" overlaps "*bar" via "foobar"
        // null | secret* | public.field | source | -> [secret*] no overlap, exclude kept
        // null | field1,field2 | null | source | -> [] full-source fetch drops all codec excludes
        assertCodecExcludes(new String[] { "field3" }, new String[] { "field1", "field2" }, "field1", true, new String[] { "field2" });
        assertCodecExcludes(null, new String[] { "field1", "field2" }, "field2", false, new String[] { "field1" });
        assertCodecExcludes(null, new String[] { "obj.*", "secret*" }, "obj.field", true, new String[] { "secret*" });
        assertCodecExcludes(null, new String[] { "foo*", "bar*" }, "bar*", false, new String[] { "foo*" });
        assertCodecExcludes(null, new String[] { "foo*", "x.y" }, "*bar", true, new String[] { "x.y" });
        assertCodecExcludes(null, new String[] { "secret*" }, "public.field", true, new String[] { "secret*" });
        assertCodecExcludes(null, new String[] { "field1", "field2" }, null, true, Strings.EMPTY_ARRAY);
    }

    /**
     * Builds a search context whose root source has {@code rootIncludes}/{@code excludes} and a single
     * inner hit requesting {@code innerHitField}, then asserts the resulting codec excludes. When
     * {@code viaSource} is true the field is an inner-hit source include; otherwise it is requested via
     * fetch fields with source fetching disabled. A null {@code innerHitField} with {@code viaSource}
     * true models a default full-source fetch (fetchSource=true, no explicit includes). The returned
     * _source includes/excludes are asserted to be the unmodified root request values.
     */
    private void assertCodecExcludes(
        String[] rootIncludes,
        String[] excludes,
        String innerHitField,
        boolean viaSource,
        String[] expectedCodecExcludes
    ) {
        FetchPhase fetchPhase = new FetchPhase(new ArrayList<>());
        SearchContext context = mock(SearchContext.class);
        when(context.hasScriptFields()).thenReturn(false);
        when(context.storedFieldsContext()).thenReturn(null);

        FetchSourceContext rootSource = new FetchSourceContext(true, rootIncludes, excludes);
        when(context.hasFetchSourceContext()).thenReturn(true);
        when(context.fetchSourceContext()).thenReturn(rootSource);
        when(context.sourceRequested()).thenReturn(true);
        when(context.fetchFieldsContext()).thenReturn(null);

        InnerHitsContext.InnerHitSubContext innerHit = mock(InnerHitsContext.InnerHitSubContext.class);
        if (viaSource) {
            String[] innerIncludes = innerHitField != null ? new String[] { innerHitField } : null;
            when(innerHit.fetchSourceContext()).thenReturn(new FetchSourceContext(true, innerIncludes, null));
            when(innerHit.fetchFieldsContext()).thenReturn(null);
        } else {
            when(innerHit.fetchSourceContext()).thenReturn(new FetchSourceContext(false));
            when(innerHit.fetchFieldsContext()).thenReturn(new FetchFieldsContext(List.of(new FieldAndFormat(innerHitField, null))));
        }

        InnerHitsContext innerHitsContext = new InnerHitsContext();
        innerHitsContext.getInnerHits().put("inner", innerHit);
        when(context.innerHits()).thenReturn(innerHitsContext);

        FieldsVisitor fieldsVisitor = fetchPhase.createStoredFieldsVisitor(context, null);

        // Requested includes/excludes must not be mutated.
        assertArrayEquals(excludes, fieldsVisitor.excludes());
        assertArrayEquals(rootIncludes != null ? rootIncludes : Strings.EMPTY_ARRAY, fieldsVisitor.includes());
        assertArrayEquals(expectedCodecExcludes, fieldsVisitor.codecExcludes());
    }

    public void testCodecSourceExcludesRemovesNestedInnerHitFields() {
        // A nested (grandchild) inner hit reads from the same root _source, so a field it requests
        // must also be dropped from the codec excludes.
        FetchPhase fetchPhase = new FetchPhase(new ArrayList<>());
        SearchContext context = mock(SearchContext.class);
        when(context.hasScriptFields()).thenReturn(false);
        when(context.storedFieldsContext()).thenReturn(null);

        String[] excludes = new String[] { "field1", "field2" };
        FetchSourceContext rootSource = new FetchSourceContext(true, null, excludes);
        when(context.hasFetchSourceContext()).thenReturn(true);
        when(context.fetchSourceContext()).thenReturn(rootSource);
        when(context.sourceRequested()).thenReturn(true);
        when(context.fetchFieldsContext()).thenReturn(null);

        // Child inner hit requests nothing overlapping, but its own nested inner hit requests "field1".
        InnerHitsContext.InnerHitSubContext nestedInnerHit = mock(InnerHitsContext.InnerHitSubContext.class);
        when(nestedInnerHit.fetchSourceContext()).thenReturn(new FetchSourceContext(true, new String[] { "field1" }, null));
        when(nestedInnerHit.fetchFieldsContext()).thenReturn(null);
        when(nestedInnerHit.innerHits()).thenReturn(null);

        InnerHitsContext nestedInnerHitsContext = new InnerHitsContext();
        nestedInnerHitsContext.getInnerHits().put("nested", nestedInnerHit);

        InnerHitsContext.InnerHitSubContext innerHit = mock(InnerHitsContext.InnerHitSubContext.class);
        when(innerHit.fetchSourceContext()).thenReturn(new FetchSourceContext(true, new String[] { "field3" }, null));
        when(innerHit.fetchFieldsContext()).thenReturn(null);
        when(innerHit.innerHits()).thenReturn(nestedInnerHitsContext);

        InnerHitsContext innerHitsContext = new InnerHitsContext();
        innerHitsContext.getInnerHits().put("inner", innerHit);
        when(context.innerHits()).thenReturn(innerHitsContext);

        FieldsVisitor fieldsVisitor = fetchPhase.createStoredFieldsVisitor(context, null);

        // Requested includes/excludes must not be mutated.
        assertArrayEquals(excludes, fieldsVisitor.excludes());
        assertArrayEquals(Strings.EMPTY_ARRAY, fieldsVisitor.includes());
        // "field1" is requested by the nested inner hit so it is dropped; "field2" is not requested anywhere.
        assertArrayEquals(new String[] { "field2" }, fieldsVisitor.codecExcludes());
    }

    public void testTaskCancellationDuringFetch() {
        FetchPhase fetchPhase = new FetchPhase(Collections.emptyList());

        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
        SearchContext context = mock(SearchContext.class);
        when(context.isCancelled()).thenReturn(false, true);
        when(context.getTask()).thenReturn(task);

        when(context.docIdsToLoadSize()).thenReturn(1);
        when(context.docIdsToLoad()).thenReturn(new int[] { 0 });
        when(context.docIdsToLoadFrom()).thenReturn(0);
        when(context.hasScriptFields()).thenReturn(false);
        when(context.hasFetchSourceContext()).thenReturn(false);
        when(context.storedFieldsContext()).thenReturn(null);
        when(context.fetchSourceContext(any(FetchSourceContext.class))).thenReturn(null);
        when(context.fetchSourceContext()).thenReturn(FetchSourceContext.FETCH_SOURCE);

        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        SearchLookup lookup = new SearchLookup(mock(MapperService.class), (ft, sl) -> null);
        when(queryShardContext.newFetchLookup()).thenReturn(lookup);
        when(context.getQueryShardContext()).thenReturn(queryShardContext);

        SearchShardTarget target = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        when(context.shardTarget()).thenReturn(target);

        task.cancel("test");

        TaskCancelledException ex = expectThrows(TaskCancelledException.class, () -> fetchPhase.execute(context));
        assertEquals("cancelled task with reason: test", ex.getMessage());
    }

    public void testExecuteCancelledTaskThrows() {
        FetchPhase fetchPhase = new FetchPhase(new ArrayList<>());
        TestSearchContext context = new TestSearchContext(null, null, null);

        SearchShardTask task = mock(SearchShardTask.class);
        when(task.isCancelled()).thenReturn(true);
        when(task.getReasonCancelled()).thenReturn("test reason");
        context.setTask(task);

        TaskCancelledException ex = expectThrows(TaskCancelledException.class, () -> fetchPhase.execute(context));
        assertEquals("cancelled task with reason: test reason", ex.getMessage());
    }
}
