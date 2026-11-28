/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.VirtualShardRoutingHelper;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VirtualShardFilteredMergePolicyTests extends OpenSearchTestCase {

    public void testIsolateVirtualShard() throws Exception {
        try (Directory source = newDirectory()) {
            IndexMetadata metadata = metadata("test", 5, 20);
            List<String> ids = new ArrayList<>();

            try (IndexWriter writer = new IndexWriter(source, new IndexWriterConfig(null))) {
                for (int i = 0; i < 100; i++) {
                    String id = Integer.toString(i);
                    ids.add(id);
                    writer.addDocument(doc(id, null));
                }
                writer.commit();
            }

            int targetVShard = 0;
            int expected = 0;
            for (String id : ids) {
                if (VirtualShardRoutingHelper.computeVirtualShardId(metadata, id, null) == targetVShard) {
                    expected++;
                }
            }

            int actual = filteredDocCount(source, metadata, targetVShard);
            assertEquals(expected, actual);
        }
    }

    public void testEmptyExtraction() throws Exception {
        try (Directory source = newDirectory(); Directory output = newDirectory()) {
            IndexMetadata metadata = metadata("test", 5, 20);
            String id = "1";
            int occupiedVShard = VirtualShardRoutingHelper.computeVirtualShardId(metadata, id, null);
            int targetVShard = (occupiedVShard + 1) % metadata.getNumberOfVirtualShards();

            try (IndexWriter writer = new IndexWriter(source, new IndexWriterConfig(null))) {
                writer.addDocument(doc(id, null));
                writer.commit();
            }

            try (
                DirectoryReader reader = DirectoryReader.open(source);
                IndexWriter writer = new IndexWriter(output, new IndexWriterConfig(null))
            ) {
                List<CodecReader> filteredReaders = buildFilteredReaders(reader, metadata, targetVShard);
                writer.addIndexes(filteredReaders.toArray(new CodecReader[0]));
                writer.forceMerge(1);
                writer.commit();
            }

            try (DirectoryReader outReader = DirectoryReader.open(output)) {
                assertEquals(0, outReader.numDocs());
            }
        }
    }

    public void testAllDocsInSingleShard() throws Exception {
        try (Directory source = newDirectory()) {
            IndexMetadata metadata = metadata("test", 1, 1);

            try (IndexWriter writer = new IndexWriter(source, new IndexWriterConfig(null))) {
                for (int i = 0; i < 50; i++) {
                    writer.addDocument(doc(Integer.toString(i), null));
                }
                writer.commit();
            }

            assertEquals(50, filteredDocCount(source, metadata, 0));
        }
    }

    public void testFilterReaderNumDocsConsistency() throws Exception {
        try (Directory source = newDirectory(); DirectoryReader reader = DirectoryReader.open(writeTwoSegments(source))) {
            IndexMetadata metadata = metadata("test", 5, 20);
            int targetVShard = 3;
            List<CodecReader> filteredReaders = buildFilteredReaders(reader, metadata, targetVShard);

            for (CodecReader filtered : filteredReaders) {
                Bits liveDocs = filtered.getLiveDocs();
                int expected = 0;
                for (int i = 0; i < filtered.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        expected++;
                    }
                }
                assertEquals(expected, filtered.numDocs());
            }
        }
    }

    private Directory writeTwoSegments(Directory source) throws IOException {
        try (IndexWriter writer = new IndexWriter(source, new IndexWriterConfig(null))) {
            for (int i = 0; i < 10; i++) {
                writer.addDocument(doc(Integer.toString(i), null));
            }
            writer.commit();
            for (int i = 10; i < 20; i++) {
                writer.addDocument(doc(Integer.toString(i), null));
            }
            writer.commit();
        }
        return source;
    }

    private int filteredDocCount(Directory source, IndexMetadata metadata, int targetVShard) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(source)) {
            int count = 0;
            for (CodecReader filtered : buildFilteredReaders(reader, metadata, targetVShard)) {
                count += filtered.numDocs();
            }
            return count;
        }
    }

    private List<CodecReader> buildFilteredReaders(DirectoryReader reader, IndexMetadata metadata, int targetVShard) throws IOException {
        List<CodecReader> filteredReaders = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            CodecReader codecReader = unwrapToCodecReader(leaf);
            assertNotNull(codecReader);
            filteredReaders.add(
                new VirtualShardFilteredMergePolicy.VirtualShardFilterReader(codecReader, leaf.getLiveDocs(), metadata, targetVShard)
            );
        }
        return filteredReaders;
    }

    private CodecReader unwrapToCodecReader(LeafReader reader) {
        LeafReader current = reader;
        for (int i = 0; i < 100; i++) {
            if (current instanceof CodecReader) {
                return (CodecReader) current;
            }
            if (current instanceof FilterLeafReader) {
                current = ((FilterLeafReader) current).getDelegate();
            } else {
                break;
            }
        }
        return current instanceof CodecReader ? (CodecReader) current : null;
    }

    private IndexMetadata metadata(String index, int numShards, int numVirtualShards) {
        return IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, numVirtualShards)
            )
            .numberOfShards(numShards)
            .numberOfReplicas(1)
            .build();
    }

    private Document doc(String id, String routing) {
        Document doc = new Document();
        doc.add(new StoredField(IdFieldMapper.NAME, Uid.encodeId(id)));
        if (routing != null) {
            doc.add(new StoredField(RoutingFieldMapper.NAME, routing));
        }
        return doc;
    }
}
