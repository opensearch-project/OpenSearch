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

package org.opensearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType.Relation;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

// The purpose of this test case is to test RangeQueryBuilder.getRelation()
// Whether it should return INTERSECT/DISJOINT/WITHIN is already tested in
// RangeQueryBuilderTests
public class RangeQueryRewriteTests extends OpenSearchSingleNodeTestCase {

    public void testRewriteMissingField() throws Exception {
        IndexService indexService = createIndex("test");
        IndexReader reader = new MultiReader();
        QueryRewriteContext context = new QueryShardContext(
            0,
            indexService.getIndexSettings(),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            indexService.mapperService(),
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            null,
            null,
            null,
            () -> true,
            null
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }

    public void testRewriteMissingReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("foo")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        QueryRewriteContext context = new QueryShardContext(
            0,
            indexService.getIndexSettings(),
            null,
            null,
            null,
            indexService.mapperService(),
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            null,
            null,
            null,
            () -> true,
            null
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // can't make assumptions on a missing reader, so it must return INTERSECT
        assertEquals(Relation.INTERSECTS, range.getRelation(context));
    }

    public void testRewriteEmptyReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("foo")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        IndexReader reader = new MultiReader();
        QueryRewriteContext context = new QueryShardContext(
            0,
            indexService.getIndexSettings(),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            indexService.mapperService(),
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            null,
            null,
            null,
            () -> true,
            null
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // no values -> DISJOINT
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }
}
