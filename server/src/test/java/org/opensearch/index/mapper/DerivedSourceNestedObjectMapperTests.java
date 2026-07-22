/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class DerivedSourceNestedObjectMapperTests extends MapperServiceTestCase {

    @SuppressWarnings("unchecked")
    public void testDerivedSourceWithSingleLevelNested() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.startObject("score").field("type", "integer").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.field("title", "doc");
            b.startArray("comments");
            b.startObject().field("tag", "b").field("score", 2).endObject();
            b.startObject().field("tag", "a").field("score", 1).endObject();
            b.endArray();
        }));

        assertEquals("doc", source.get("title"));
        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(2, comments.size());
        assertEquals("b", comments.get(0).get("tag"));
        assertEquals(2, ((Number) comments.get(0).get("score")).intValue());
        assertEquals("a", comments.get(1).get("tag"));
        assertEquals(1, ((Number) comments.get(1).get("score")).intValue());
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceWithMultiLevelNested() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("author").field("type", "keyword").endObject();
            b.startObject("replies").field("type", "nested").startObject("properties");
            b.startObject("message").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject();
            b.field("author", "kim");
            b.startArray("replies");
            b.startObject().field("message", "r1").endObject();
            b.startObject().field("message", "r2").endObject();
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("author", "lee");
            b.startArray("replies");
            b.startObject().field("message", "r3").endObject();
            b.endArray();
            b.endObject();
            b.endArray();
        }));

        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(2, comments.size());
        assertEquals("kim", comments.get(0).get("author"));
        assertEquals("lee", comments.get(1).get("author"));
        List<Map<String, Object>> firstReplies = (List<Map<String, Object>>) comments.get(0).get("replies");
        List<Map<String, Object>> secondReplies = (List<Map<String, Object>>) comments.get(1).get("replies");
        assertEquals("r1", firstReplies.get(0).get("message"));
        assertEquals("r2", firstReplies.get(1).get("message"));
        assertEquals("r3", secondReplies.get(0).get("message"));
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceWithNestedUsesBitSetProducer() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("author").field("type", "keyword").endObject();
            b.startObject("replies").field("type", "nested").startObject("properties");
            b.startObject("message").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject().endObject();
        }));

        AtomicInteger bitSetProducerCalls = new AtomicInteger();
        AtomicInteger prevSetBitCalls = new AtomicInteger();
        List<LeafReaderContext> bitSetContexts = new ArrayList<>();
        Function<Query, BitSetProducer> bitSetProducer = query -> {
            bitSetProducerCalls.incrementAndGet();
            QueryBitSetProducer delegate = new QueryBitSetProducer(query);
            return context -> {
                bitSetContexts.add(context);
                BitSet bitSet = delegate.getBitSet(context);
                return bitSet == null ? null : new CountingPrevSetBitSet(bitSet, prevSetBitCalls);
            };
        };
        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject();
            b.field("author", "kim");
            b.startArray("replies");
            b.startObject().field("message", "r1").endObject();
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("author", "lee");
            b.startArray("replies");
            b.startObject().field("message", "r2").endObject();
            b.endArray();
            b.endObject();
            b.endArray();
        }), bitSetProducer);

        assertThat(bitSetProducerCalls.get(), greaterThan(0));
        assertEquals(1, prevSetBitCalls.get());
        assertFalse(bitSetContexts.isEmpty());
        for (LeafReaderContext context : bitSetContexts) {
            assertNotNull(context.parent);
            assertFalse(context.isTopLevel);
        }
        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(2, comments.size());
        assertEquals("kim", comments.get(0).get("author"));
        assertEquals("lee", comments.get(1).get("author"));
        List<Map<String, Object>> firstReplies = (List<Map<String, Object>>) comments.get(0).get("replies");
        List<Map<String, Object>> secondReplies = (List<Map<String, Object>>) comments.get(1).get("replies");
        assertEquals("r1", firstReplies.get(0).get("message"));
        assertEquals("r2", secondReplies.get(0).get("message"));
    }

    public void testDerivedSourceDelegatesBitSetProducerForEachRootDoc() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        ParsedDocument firstDoc = docMapper.parse(source(b -> {
            b.startArray("comments");
            b.startObject().field("tag", "a").endObject();
            b.endArray();
        }));
        ParsedDocument secondDoc = docMapper.parse(source(b -> {
            b.startArray("comments");
            b.startObject().field("tag", "b").endObject();
            b.endArray();
        }));

        AtomicInteger bitSetProducerCalls = new AtomicInteger();
        Function<Query, BitSetProducer> bitSetProducer = query -> {
            bitSetProducerCalls.incrementAndGet();
            QueryBitSetProducer delegate = new QueryBitSetProducer(query);
            return delegate::getBitSet;
        };

        try (
            Directory directory = newDirectory();
            RandomIndexWriter writer = new RandomIndexWriter(random(), directory, new IndexWriterConfig())
        ) {
            writer.addDocuments(firstDoc.docs());
            writer.addDocuments(secondDoc.docs());
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                CheckedFunction<Integer, BytesReference, IOException> provider = docMapper.root()
                    .derivedSourceProvider(leafReaderContext, bitSetProducer, docMapper.hasNestedObjects());
                provider.apply(firstDoc.docs().size() - 1);
                provider.apply(firstDoc.docs().size() + secondDoc.docs().size() - 1);
            }
        }

        assertEquals(4, bitSetProducerCalls.get());
    }

    public void testNestedFieldWithUnsupportedDerivedSourceChildIsRejected() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
                b.startObject("comments").field("type", "nested").startObject("properties");
                b.startObject("tag").field("type", "keyword").field("ignore_above", 10).endObject();
                b.endObject().endObject();
            }))
        );
        assertThat(e.getMessage(), containsString("Unable to derive source for [comments.tag]"));
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceIgnoresNestedIncludeFlags() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments")
                .field("type", "nested")
                .field("include_in_parent", true)
                .field("include_in_root", true)
                .startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject().field("tag", "a").endObject();
            b.endArray();
        }));

        assertFalse(source.containsKey("comments.tag"));
        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(1, comments.size());
        assertEquals("a", comments.get(0).get("tag"));
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceIgnoresMultiLevelNestedIncludeFlags() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments")
                .field("type", "nested")
                .field("include_in_parent", true)
                .field("include_in_root", true)
                .startObject("properties");
            b.startObject("author").field("type", "keyword").endObject();
            b.startObject("replies")
                .field("type", "nested")
                .field("include_in_parent", true)
                .field("include_in_root", true)
                .startObject("properties");
            b.startObject("message").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject();
            b.field("author", "kim");
            b.startArray("replies");
            b.startObject().field("message", "r1").endObject();
            b.startObject().field("message", "r2").endObject();
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("author", "lee");
            b.startArray("replies");
            b.startObject().field("message", "r3").endObject();
            b.endArray();
            b.endObject();
            b.endArray();
        }));

        assertEquals(List.of("comments"), new ArrayList<>(source.keySet()));
        assertFalse(source.containsKey("comments.author"));
        assertFalse(source.containsKey("comments.replies.message"));

        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(2, comments.size());
        assertEquals("kim", comments.get(0).get("author"));
        assertEquals("lee", comments.get(1).get("author"));
        assertFalse(comments.get(0).containsKey("comments.replies.message"));
        assertFalse(comments.get(0).containsKey("replies.message"));
        assertFalse(comments.get(1).containsKey("comments.replies.message"));
        assertFalse(comments.get(1).containsKey("replies.message"));

        List<Map<String, Object>> firstReplies = (List<Map<String, Object>>) comments.get(0).get("replies");
        List<Map<String, Object>> secondReplies = (List<Map<String, Object>>) comments.get(1).get("replies");
        assertEquals(2, firstReplies.size());
        assertEquals(1, secondReplies.size());
        assertEquals("r1", firstReplies.get(0).get("message"));
        assertEquals("r2", firstReplies.get(1).get("message"));
        assertEquals("r3", secondReplies.get(0).get("message"));
    }

    public void testDerivedSourceOmitsMissingNestedObjects() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> b.field("title", "doc")));

        assertEquals("doc", source.get("title"));
        assertFalse(source.containsKey("comments"));
    }

    public void testDerivedSourceIncludesExistingEmptyNestedObjects() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> emptyArraySource = deriveSource(docMapper, source(b -> {
            b.field("title", "empty-array");
            b.startArray("comments").endArray();
        }));
        assertEquals("empty-array", emptyArraySource.get("title"));
        assertFalse(emptyArraySource.containsKey("comments"));

        Map<String, Object> nullSource = deriveSource(docMapper, source(b -> {
            b.field("title", "null");
            b.nullField("comments");
        }));
        assertEquals("null", nullSource.get("title"));
        assertFalse(nullSource.containsKey("comments"));

        Map<String, Object> emptyObjectSource = deriveSource(docMapper, source(b -> {
            b.field("title", "empty-object");
            b.startArray("comments");
            b.startObject().endObject();
            b.endArray();
        }));
        assertEquals("empty-object", emptyObjectSource.get("title"));
        assertSingleEmptyNestedObject(emptyObjectSource, "comments");
    }

    public void testNestedEmptyContainersDoNotCreateNestedLuceneDocs() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        assertParsedDocCount(docMapper, source(b -> b.field("title", "missing")), 1);
        assertParsedDocCount(docMapper, source(b -> {
            b.field("title", "null");
            b.nullField("comments");
        }), 1);
        assertParsedDocCount(docMapper, source(b -> {
            b.field("title", "empty-array");
            b.startArray("comments").endArray();
        }), 1);
    }

    public void testNestedObjectsWithoutDerivedValuesCreateEmptyDerivedNestedObjects() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        SourceToParse emptyObjectSource = source(b -> {
            b.field("title", "empty-object");
            b.startArray("comments");
            b.startObject().endObject();
            b.endArray();
        });
        assertParsedDocCount(docMapper, emptyObjectSource, 2);
        assertSingleEmptyNestedObject(deriveSource(docMapper, emptyObjectSource), "comments");

        SourceToParse nullValueSource = source(b -> {
            b.field("title", "null-value");
            b.startArray("comments");
            b.startObject().nullField("tag").endObject();
            b.endArray();
        });
        assertParsedDocCount(docMapper, nullValueSource, 2);
        assertSingleEmptyNestedObject(deriveSource(docMapper, nullValueSource), "comments");

        SourceToParse emptyValueArraySource = source(b -> {
            b.field("title", "empty-value-array");
            b.startArray("comments");
            b.startObject().startArray("tag").endArray().endObject();
            b.endArray();
        });
        assertParsedDocCount(docMapper, emptyValueArraySource, 2);
        assertSingleEmptyNestedObject(deriveSource(docMapper, emptyValueArraySource), "comments");
    }

    public void testDynamicFalseUnknownNestedObjectCreatesEmptyDerivedNestedObject() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").field("dynamic", "false").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        SourceToParse source = source(b -> {
            b.field("title", "unknown-field");
            b.startArray("comments");
            b.startObject().field("unknown", "ignored").endObject();
            b.endArray();
        });
        assertParsedDocCount(docMapper, source, 2);

        Map<String, Object> derivedSource = deriveSource(docMapper, source);
        assertEquals("unknown-field", derivedSource.get("title"));
        assertSingleEmptyNestedObject(derivedSource, "comments");
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceKeepsSiblingNestedPathsSeparate() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("labels").field("type", "nested").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject().field("tag", "comment-a").endObject();
            b.startObject().field("tag", "comment-b").endObject();
            b.endArray();
            b.startArray("labels");
            b.startObject().field("name", "label-a").endObject();
            b.endArray();
        }));

        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        List<Map<String, Object>> labels = (List<Map<String, Object>>) source.get("labels");
        assertEquals(2, comments.size());
        assertEquals("comment-a", comments.get(0).get("tag"));
        assertEquals("comment-b", comments.get(1).get("tag"));
        assertEquals(1, labels.size());
        assertEquals("label-a", labels.get(0).get("name"));
        assertFalse(comments.get(0).containsKey("name"));
        assertFalse(labels.get(0).containsKey("tag"));
    }

    @SuppressWarnings("unchecked")
    public void testDerivedSourceSupportsFieldSemanticsInsideNestedDocs() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(Settings.builder().put("index.derived_source.enabled", true).build(), mapping(b -> {
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("tags").field("type", "keyword").endObject();
            b.startObject("score").field("type", "integer").endObject();
            b.startObject("published").field("type", "date").field("format", "strict_date_time_no_millis").endObject();
            b.startObject("approved").field("type", "boolean").endObject();
            b.startObject("message").field("type", "text").endObject();
            b.endObject().endObject();
        }));

        Map<String, Object> source = deriveSource(docMapper, source(b -> {
            b.startArray("comments");
            b.startObject();
            b.startArray("tags").value("z").value("a").value("a").endArray();
            b.field("score", 3);
            b.field("published", "2025-02-18T06:00:00Z");
            b.field("approved", true);
            b.field("message", "stored text");
            b.endObject();
            b.endArray();
        }));

        List<Map<String, Object>> comments = (List<Map<String, Object>>) source.get("comments");
        assertEquals(1, comments.size());
        Map<String, Object> comment = comments.get(0);
        assertEquals(List.of("a", "z"), comment.get("tags"));
        assertEquals(3, ((Number) comment.get("score")).intValue());
        assertEquals("2025-02-18T06:00:00Z", comment.get("published"));
        assertEquals(true, comment.get("approved"));
        assertEquals("stored text", comment.get("message"));
    }

    private Map<String, Object> deriveSource(DocumentMapper docMapper, SourceToParse source) throws IOException {
        return deriveSource(docMapper, source, QueryBitSetProducer::new);
    }

    private Map<String, Object> deriveSource(DocumentMapper docMapper, SourceToParse source, Function<Query, BitSetProducer> bitSetProducer)
        throws IOException {
        ParsedDocument parsedDocument = docMapper.parse(source);
        try (
            Directory directory = newDirectory();
            RandomIndexWriter writer = new RandomIndexWriter(random(), directory, new IndexWriterConfig())
        ) {
            writer.addDocuments(parsedDocument.docs());
            try (IndexReader reader = writer.getReader()) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int rootDocId = parsedDocument.docs().size() - 1;
                return XContentHelper.convertToMap(
                    docMapper.root()
                        .derivedSourceProvider(leafReaderContext, bitSetProducer, docMapper.hasNestedObjects())
                        .apply(rootDocId),
                    false,
                    MediaTypeRegistry.JSON
                ).v2();
            }
        }
    }

    private void assertParsedDocCount(DocumentMapper docMapper, SourceToParse source, int expectedDocCount) throws IOException {
        assertEquals(expectedDocCount, docMapper.parse(source).docs().size());
    }

    @SuppressWarnings("unchecked")
    private void assertSingleEmptyNestedObject(Map<String, Object> source, String field) {
        List<Map<String, Object>> nestedObjects = (List<Map<String, Object>>) source.get(field);
        assertNotNull(nestedObjects);
        assertEquals(1, nestedObjects.size());
        assertTrue(nestedObjects.get(0).isEmpty());
    }

    private static class CountingPrevSetBitSet extends BitSet {
        private final BitSet delegate;
        private final AtomicInteger prevSetBitCalls;

        private CountingPrevSetBitSet(BitSet delegate, AtomicInteger prevSetBitCalls) {
            this.delegate = delegate;
            this.prevSetBitCalls = prevSetBitCalls;
        }

        @Override
        public void set(int i) {
            delegate.set(i);
        }

        @Override
        public boolean getAndSet(int i) {
            return delegate.getAndSet(i);
        }

        @Override
        public void clear(int i) {
            delegate.clear(i);
        }

        @Override
        public void clear(int startIndex, int endIndex) {
            delegate.clear(startIndex, endIndex);
        }

        @Override
        public int cardinality() {
            return delegate.cardinality();
        }

        @Override
        public int approximateCardinality() {
            return delegate.approximateCardinality();
        }

        @Override
        public int prevSetBit(int index) {
            prevSetBitCalls.incrementAndGet();
            return delegate.prevSetBit(index);
        }

        @Override
        public int nextSetBit(int index) {
            return delegate.nextSetBit(index);
        }

        @Override
        public int nextSetBit(int startIndex, int endIndex) {
            return delegate.nextSetBit(startIndex, endIndex);
        }

        @Override
        public boolean get(int index) {
            return delegate.get(index);
        }

        @Override
        public int length() {
            return delegate.length();
        }

        @Override
        public long ramBytesUsed() {
            return delegate.ramBytesUsed();
        }
    }
}
