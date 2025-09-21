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

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.codec.CriteriaBasedCodec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Simple tests for this filterreader
 */
public class OpenSearchDirectoryReaderTests extends OpenSearchTestCase {

    /**
     * Test that core cache key (needed for NRT) is working
     */
    public void testCoreCacheKey() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add two docs, id:0 and id:1
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue("0");
        iw.addDocument(doc);
        idField.setStringValue("1");
        iw.addDocument(doc);

        // open reader
        ShardId shardId = new ShardId("fake", "_na_", 1);
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(iw), shardId);
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);

        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheHelper().getKey(), ir2.leaves().get(0).reader().getCoreCacheHelper().getKey());
        IOUtils.close(ir, ir2, iw, dir);
    }

    public void testCriteriaBasedReaders() {
        try (Directory dir1 = newDirectory(); Directory dir2 = newDirectory(); Directory dir3 = newDirectory()) {

            // Given
            IndexWriterConfig iwc1 = indexWriterConfig("criteria1");
            IndexWriterConfig iwc2 = indexWriterConfig("criteria2");

            IndexWriter iw1 = new IndexWriter(dir1, iwc1);
            IndexWriter iw2 = new IndexWriter(dir2, iwc2);

            iw1.addDocument(document("0"));
            iw1.addDocument(document("1"));
            iw1.close();
            iw2.addDocument(document("2"));
            iw2.addDocument(document("3"));
            iw2.close();

            IndexWriter iw3 = new IndexWriter(dir3, indexWriterConfig(null));
            iw3.addIndexes(dir1, dir2);
            iw3.flush();
            iw3.commit();

            ShardId shardId = new ShardId("fake", "_na_", 1);
            OpenSearchDirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(iw3), shardId);

            // When
            OpenSearchDirectoryReader reader1 = ir.getCriteriaBasedReader(Set.of("criteria1"));
            OpenSearchDirectoryReader reader2 = ir.getCriteriaBasedReader(Set.of("criteria1", "criteria2"));
            OpenSearchDirectoryReader reader3 = ir.getCriteriaBasedReader(Set.of("criteria1", "criteria2"));

            // Verify reader1
            assertEquals(2, reader1.numDocs());
            assertEquals(1, reader1.leaves().size());
            assertTrue(reader1.getDelegate() instanceof OpenSearchDirectoryReader.ChildDirectoryReader);
            OpenSearchDirectoryReader.ChildDirectoryReader cbr1 = (OpenSearchDirectoryReader.ChildDirectoryReader) reader1.getDelegate();

            Optional<LeafReaderContext> segmentReader = cbr1.leaves().stream().findFirst();
            assertTrue(segmentReader.isPresent());

            final String bucket = Lucene.segmentReader(segmentReader.get().reader()).getSegmentInfo().info.getAttribute("bucket");

            assertEquals("criteria1", bucket);

            // Verify reader2
            assertEquals(4, reader2.numDocs());
            assertEquals(2, reader2.leaves().size());
            assertTrue(reader2.getDelegate() instanceof OpenSearchDirectoryReader.ChildDirectoryReader);
            OpenSearchDirectoryReader.ChildDirectoryReader cbr2 = (OpenSearchDirectoryReader.ChildDirectoryReader) reader2.getDelegate();

            Set<String> buckets = new HashSet<>();
            for (LeafReaderContext leaf : cbr2.leaves()) {
                SegmentReader sr = Lucene.segmentReader(leaf.reader());
                String attribute = sr.getSegmentInfo().info.getAttribute("bucket");
                buckets.add(attribute);
            }

            assertEquals(Set.of("criteria1", "criteria2").toString(), buckets.toString());

            // Check if caching works
            assertEquals(reader3.getReaderCacheHelper().getKey(), reader2.getReaderCacheHelper().getKey());

            // Non-existent reader
            try (OpenSearchDirectoryReader nonExistentReader = ir.getCriteriaBasedReader(Set.of("non_existent"))) {
                assertEquals(0, nonExistentReader.numDocs());
                assertEquals(0, nonExistentReader.leaves().size());
            }

            IOUtils.close(reader1, reader2, reader3, iw3, ir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IndexWriterConfig indexWriterConfig(String bucket) {
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        if (bucket != null) {
            iwc.setCodec(new CriteriaBasedCodec(Codec.getDefault(), bucket));
        }
        return iwc;
    }

    private Document document(String value) {
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue(value);
        return doc;
    }

}
