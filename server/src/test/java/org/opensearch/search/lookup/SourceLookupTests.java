/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.lookup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SourceLookupTests extends OpenSearchTestCase {

    public void testLazyFieldReaderWithRegularReader() throws IOException {
        try (Directory dir = newDirectory()) {
            indexSourceDoc(dir, "{\"field\":\"value\"}");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                assertFalse(ctx.reader() instanceof SequentialStoredFieldsLeafReader);

                SourceLookup lookup = new SourceLookup();
                lookup.setSegmentAndDocument(ctx, 0);

                Map<String, Object> source = lookup.loadSourceIfNeeded();
                assertEquals("value", source.get("field"));

                // Same reader+doc — source should remain cached
                lookup.setSegmentAndDocument(ctx, 0);
                assertSame(source, lookup.loadSourceIfNeeded());
            }
        }
    }

    public void testLazyFieldReaderWithSequentialReader() throws IOException {
        try (Directory dir = newDirectory()) {
            indexSourceDoc(dir, "{\"field\":\"value2\"}");
            try (DirectoryReader rawReader = DirectoryReader.open(dir)) {
                DirectoryReader wrappedReader = OpenSearchDirectoryReader.wrap(rawReader, new ShardId(new Index("test", "_na_"), 0));
                LeafReaderContext ctx = wrappedReader.leaves().get(0);
                assertTrue(ctx.reader() instanceof SequentialStoredFieldsLeafReader);

                SourceLookup lookup = new SourceLookup();
                lookup.setSegmentAndDocument(ctx, 0);

                Map<String, Object> source = lookup.loadSourceIfNeeded();
                assertEquals("value2", source.get("field"));
            }
        }
    }

    public void testSetSegmentAndDocumentWithNewReaderDefersFieldReader() throws IOException {
        try (Directory dir1 = newDirectory(); Directory dir2 = newDirectory()) {
            indexSourceDoc(dir1, "{\"a\":\"1\"}");
            indexSourceDoc(dir2, "{\"b\":\"2\"}");
            try (DirectoryReader reader1 = DirectoryReader.open(dir1); DirectoryReader reader2 = DirectoryReader.open(dir2)) {
                LeafReaderContext ctx1 = reader1.leaves().get(0);
                LeafReaderContext ctx2 = reader2.leaves().get(0);

                SourceLookup lookup = new SourceLookup();
                lookup.setSegmentAndDocument(ctx1, 0);
                assertEquals("1", lookup.loadSourceIfNeeded().get("a"));

                // Switch to a different reader — should reset fieldReader and source
                lookup.setSegmentAndDocument(ctx2, 0);
                assertEquals("2", lookup.loadSourceIfNeeded().get("b"));
            }
        }
    }

    public void testLoadSourceWithNoSourceReturnsEmptyMap() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StoredField("some_other_field", "data"));
                writer.addDocument(doc);
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext ctx = reader.leaves().get(0);

                SourceLookup lookup = new SourceLookup();
                lookup.setSegmentAndDocument(ctx, 0);

                Map<String, Object> source = lookup.loadSourceIfNeeded();
                assertTrue(source.isEmpty());
            }
        }
    }

    private static void indexSourceDoc(Directory dir, String jsonSource) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StoredField("_source", jsonSource.getBytes(StandardCharsets.UTF_8)));
            writer.addDocument(doc);
            writer.commit();
        }
    }
}
