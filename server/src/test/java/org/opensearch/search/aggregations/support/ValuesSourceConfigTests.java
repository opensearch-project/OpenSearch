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

package org.opensearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

// TODO: This whole set of tests needs to be rethought.
public class ValuesSourceConfigTests extends OpenSearchSingleNodeTestCase {

    public void testKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "bytes", "type=keyword");
        client().prepareIndex("index").setId("1").setSource("bytes", "abc").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "bytes",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testEmptyKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "bytes", "type=keyword");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "bytes",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(context, null, "bytes", null, "abc", null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testUnmappedKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);
            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                ValueType.STRING,
                "bytes",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());

            config = ValuesSourceConfig.resolve(context, ValueType.STRING, "bytes", null, "abc", null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "long", "type=long");
        client().prepareIndex("index").setId("1").setSource("long", 42).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "long",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testEmptyLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "long", "type=long");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "long",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(context, null, "long", null, 42, null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testUnmappedLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                ValueType.NUMBER,
                "long",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());

            config = ValuesSourceConfig.resolve(context, ValueType.NUMBER, "long", null, 42, null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "bool", "type=boolean");
        client().prepareIndex("index").setId("1").setSource("bool", true).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "bool",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testEmptyBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "bool", "type=boolean");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                "bool",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(context, null, "bool", null, true, null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testUnmappedBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1").setSource().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                ValueType.BOOLEAN,
                "bool",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());

            config = ValuesSourceConfig.resolve(context, ValueType.BOOLEAN, "bool", null, true, null, null, CoreValuesSourceType.BYTES);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testFieldAlias() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "field", "type=keyword", "alias", "type=alias,path=field");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);
            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                ValueType.STRING,
                "alias",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();

            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("value"), values.nextValue());
        }
    }

    public void testDerivedField() throws Exception {
        String script = "derived_field_script";
        String derived_field = "derived_keyword";

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("derived")
            .startObject(derived_field)
            .field("type", "keyword")
            .startObject("script")
            .field("source", script)
            .field("lang", "mockscript")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexService indexService = createIndex("index", Settings.EMPTY, MapperService.SINGLE_MAPPING_NAME, mapping);
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);
            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context,
                null,
                derived_field,
                null,
                null,
                null,
                null,
                CoreValuesSourceType.BYTES
            );
            assertNotNull(script);
            assertEquals(ValuesSource.Bytes.Script.class, config.getValuesSource().getClass());
        }
    }
}
