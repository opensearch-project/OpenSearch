/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.stream.StreamSupport;

public class DerivedSourceTestCase extends OpenSearchSingleNodeTestCase {
    protected static final String INDEX_NAME = "test";
    protected static final String FIELD_NAME = "field";

    private XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

    private XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject(FIELD_NAME);
            buildField.accept(b);
            b.endObject();
        });
    }

    private void createIndexAndWaitForGreen(XContentBuilder mapping) {
        createIndex(
            "test",
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setMapping(mapping)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        ensureGreen(INDEX_NAME);
    }

    private void indexOneFieldDoc(Object value) throws IOException {
        final XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
        if (value.getClass().isArray() && value.getClass().getComponentType() != Byte.TYPE) {
            doc.startArray(FIELD_NAME);
            for (Object o : (Object[]) value) {
                doc.value(o);
            }
            doc.endArray();
        } else {
            doc.field(FIELD_NAME, value);
        }
        doc.endObject();
        client().prepareIndex(INDEX_NAME).setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
    }

    private void assertDerivedSource(CheckedConsumer<Map<String, Object>, IOException> asserter) throws IOException {
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexShard shard = indicesService.indexService(resolveIndex("test")).getShard(0);
        final Mapper fieldMapper = StreamSupport.stream(shard.mapperService().documentMapper().root().spliterator(), false)
            .filter(mapper -> mapper.name().equals(FIELD_NAME))
            .findFirst()
            .get();
        fieldMapper.validateDerivedSource();
        try (Engine.Searcher searcher = shard.acquireSearcher(INDEX_NAME)) {
            final LeafReaderContext leafReaderCtx = searcher.getDirectoryReader().leaves().get(0);
            assertTrue(leafReaderCtx.reader().numDocs() > 0);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                fieldMapper.fillSource(leafReaderCtx.reader(), 0, builder);
                builder.endObject();
                // convertToMap will convert float to double
                Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
                asserter.accept(map);
            }
        }
    }

    protected void testOneField(
        CheckedConsumer<XContentBuilder, IOException> fieldMappingBuilder,
        Object fieldValue,
        CheckedConsumer<Map<String, Object>, IOException> asserter
    ) throws IOException {
        XContentBuilder mapping = fieldMapping(fieldMappingBuilder);
        logger.info("--> create index and wait for green");
        createIndexAndWaitForGreen(mapping);
        logger.info("--> index one doc");
        indexOneFieldDoc(fieldValue);
        logger.info("--> assert derived source");
        assertDerivedSource(asserter);
        logger.info("--> delete index");
        client().admin().indices().prepareDelete("test").get();
    }
}
