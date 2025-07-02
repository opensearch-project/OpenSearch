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

package org.opensearch.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.loadDocIdAndVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class VersionsTests extends OpenSearchTestCase {

    public static DirectoryReader reopen(DirectoryReader reader) throws IOException {
        return reopen(reader, true);
    }

    public static DirectoryReader reopen(DirectoryReader reader, boolean newReaderExpected) throws IOException {
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        if (newReader != null) {
            reader.close();
        } else {
            assertFalse(newReaderExpected);
        }
        return newReader;
    }

    public void testVersions() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        DirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());

        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(1L));

        doc = new Document();
        Field uid = new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE);
        Field version = new NumericDocValuesField(VersionFieldMapper.NAME, 2);
        doc.add(uid);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(2L));

        // test reuse of uid field
        doc = new Document();
        version.setLongValue(3);
        doc.add(uid);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);

        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(3L));

        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());
        directoryReader.close();
        writer.close();
        dir.close();
    }

    public void testNestedDocuments() throws IOException {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 4; ++i) {
            // Nested
            Document doc = new Document();
            doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            docs.add(doc);
        }
        // Root
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE));
        NumericDocValuesField version = new NumericDocValuesField(VersionFieldMapper.NAME, 5L);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        docs.add(doc);

        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        DirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(5L));

        version.setLongValue(6L);
        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        version.setLongValue(7L);
        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(7L));

        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());
        directoryReader.close();
        writer.close();
        dir.close();
    }

    /** Test that version map cache works, is evicted on close, etc */
    public void testCache() throws Exception {
        int size = VersionsAndSeqNoResolver.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        // should increase cache size by 1
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size + 1, VersionsAndSeqNoResolver.lookupStates.size());
        // should be cache hit
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size + 1, VersionsAndSeqNoResolver.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, VersionsAndSeqNoResolver.lookupStates.size());
        dir.close();
    }

    /** Test that version map cache behaves properly with a filtered reader */
    public void testCacheFilterReader() throws Exception {
        int size = VersionsAndSeqNoResolver.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size + 1, VersionsAndSeqNoResolver.lookupStates.size());
        // now wrap the reader
        DirectoryReader wrapped = OpenSearchDirectoryReader.wrap(reader, new ShardId("bogus", "_na_", 5));
        assertEquals(87, loadDocIdAndVersion(wrapped, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        // same size map: core cache key is shared
        assertEquals(size + 1, VersionsAndSeqNoResolver.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, VersionsAndSeqNoResolver.lookupStates.size());
        dir.close();
    }

    public void testLuceneVersionOnUnknownVersions() {
        // between two known versions, should use the lucene version of the previous version
        Version version = Version.fromString("2.1.50");
        assertEquals(VersionUtils.getPreviousVersion(Version.fromString("2.1.3")).luceneVersion, version.luceneVersion);

        // too old version, major should be the oldest supported lucene version minus 1
        version = Version.fromString("5.2.1");
        assertEquals(VersionUtils.getFirstVersion().luceneVersion.major - 1, version.luceneVersion.major);

        // future version, should be the same version as today
        version = Version.fromString(Version.CURRENT.major + ".77.1");
        assertEquals(Version.CURRENT.luceneVersion, version.luceneVersion);
    }
}
