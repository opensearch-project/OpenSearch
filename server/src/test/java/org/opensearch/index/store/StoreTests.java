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

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.hamcrest.Matchers;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.ShardLock;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;
import static org.opensearch.test.VersionUtils.randomVersion;

public class StoreTests extends OpenSearchTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );

    IndexSettings SEGMENT_REPLICATION_INDEX_SETTINGS = new IndexSettings(
        INDEX_SETTINGS.getIndexMetadata(),
        Settings.builder().put(INDEX_SETTINGS.getSettings()).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build()
    );

    private static final Version MIN_SUPPORTED_LUCENE_VERSION = org.opensearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;

    public void testRefCount() {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexSettings indexSettings = INDEX_SETTINGS;
        Store store = new Store(shardId, indexSettings, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        int incs = randomIntBetween(1, 100);
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertTrue(store.tryIncRef());
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.incRef();
        store.close();
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertTrue(store.tryIncRef());
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.decRef();
        assertThat(store.refCount(), Matchers.equalTo(0));
        assertFalse(store.tryIncRef());
        expectThrows(IllegalStateException.class, store::incRef);
        expectThrows(IllegalStateException.class, store::ensureOpen);
    }

    public void testVerifyingIndexOutput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        indexInput.seek(0);
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long length = indexInput.length();
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(
            new StoreFileMetadata("foo1.bar", length, checksum, MIN_SUPPORTED_LUCENE_VERSION),
            dir.createOutput("foo1.bar", IOContext.DEFAULT)
        );
        while (length > 0) {
            if (random().nextInt(10) == 0) {
                verifyingOutput.writeByte(indexInput.readByte());
                length--;
            } else {
                int min = (int) Math.min(length, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                verifyingOutput.writeBytes(ref.bytes, ref.offset, min);
                length -= min;
            }
        }
        Store.verify(verifyingOutput);
        try {
            appendRandomData(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        try {
            Store.verify(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }

        IOUtils.close(indexInput, verifyingOutput, dir);
    }

    public void testVerifyingIndexOutputOnEmptyFile() throws IOException {
        Directory dir = newDirectory();
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(
            new StoreFileMetadata("foo.bar", 0, Store.digestToString(0), MIN_SUPPORTED_LUCENE_VERSION),
            dir.createOutput("foo1.bar", IOContext.DEFAULT)
        );
        try {
            Store.verify(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingOutput, dir);
    }

    public void testChecksumCorrupted() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeBEInt(output, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(output, 0);
        String checksum = Store.digestToString(output.getChecksum());
        CodecUtil.writeBELong(output, output.getChecksum() + 1); // write a wrong checksum to the file
        output.close();

        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        indexInput.seek(0);
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long length = indexInput.length();
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(
            new StoreFileMetadata("foo1.bar", length, checksum, MIN_SUPPORTED_LUCENE_VERSION),
            dir.createOutput("foo1.bar", IOContext.DEFAULT)
        );
        length -= 8; // we write the checksum in the try / catch block below
        while (length > 0) {
            if (random().nextInt(10) == 0) {
                verifyingOutput.writeByte(indexInput.readByte());
                length--;
            } else {
                int min = (int) Math.min(length, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                verifyingOutput.writeBytes(ref.bytes, ref.offset, min);
                length -= min;
            }
        }

        try {
            BytesRef checksumBytes = new BytesRef(8);
            checksumBytes.length = 8;
            indexInput.readBytes(checksumBytes.bytes, checksumBytes.offset, checksumBytes.length);
            if (randomBoolean()) {
                verifyingOutput.writeBytes(checksumBytes.bytes, checksumBytes.offset, checksumBytes.length);
            } else {
                for (int i = 0; i < checksumBytes.length; i++) {
                    verifyingOutput.writeByte(checksumBytes.bytes[i]);
                }
            }
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(indexInput, verifyingOutput, dir);
    }

    private void appendRandomData(IndexOutput output) throws IOException {
        int numBytes = randomIntBetween(1, 1024);
        final BytesRef ref = new BytesRef(scaledRandomIntBetween(1, numBytes));
        ref.length = ref.bytes.length;
        while (numBytes > 0) {
            if (random().nextInt(10) == 0) {
                output.writeByte(randomByte());
                numBytes--;
            } else {
                for (int i = 0; i < ref.length; i++) {
                    ref.bytes[i] = randomByte();
                }
                final int min = Math.min(numBytes, ref.bytes.length);
                output.writeBytes(ref.bytes, ref.offset, min);
                numBytes -= min;
            }
        }
    }

    public void testVerifyingIndexOutputWithBogusInput() throws IOException {
        Directory dir = newDirectory();
        int length = scaledRandomIntBetween(10, 1024);
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(
            new StoreFileMetadata("foo1.bar", length, "", MIN_SUPPORTED_LUCENE_VERSION),
            dir.createOutput("foo1.bar", IOContext.DEFAULT)
        );
        try {
            while (length > 0) {
                verifyingOutput.writeByte((byte) random().nextInt());
                length--;
            }
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingOutput, dir);
    }

    public void testNewChecksums() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        // set default codec - all segments need checksums
        IndexWriter writer = new IndexWriter(
            store.directory(),
            newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec())
        );
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(
                        new TextField(
                            "body",
                            TestUtil.randomRealisticUnicodeString(random()),
                            random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                        )
                    );
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer).close(); // flush
        }
        Store.MetadataSnapshot metadata;
        // check before we committed
        try {
            store.getMetadata();
            fail("no index present - expected exception");
        } catch (IndexNotFoundException ex) {
            // expected
        }
        writer.commit();
        writer.close();
        metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(false));
        for (StoreFileMetadata meta : metadata) {
            try (IndexInput input = store.directory().openInput(meta.name(), IOContext.DEFAULT)) {
                String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                assertThat("File: " + meta.name() + " has a different checksum", meta.checksum(), equalTo(checksum));
                assertThat(meta.writtenBy(), equalTo(Version.LATEST));
                if (meta.name().endsWith(".si") || meta.name().startsWith("segments_")) {
                    assertThat(meta.hash().length, greaterThan(0));
                }
            }
        }
        assertConsistent(store, metadata);

        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testCheckIntegrity() throws IOException {
        Directory dir = newDirectory();
        long luceneFileLength = 0;

        try (IndexOutput output = dir.createOutput("lucene_checksum.bin", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                luceneFileLength += bytesRef.length;
            }
            CodecUtil.writeFooter(output);
            luceneFileLength += CodecUtil.footerLength();

        }

        try (IndexInput indexInput = dir.openInput("lucene_checksum.bin", IOContext.DEFAULT)) {
            assertEquals(luceneFileLength, indexInput.length());
        }

        dir.close();

    }

    public void testVerifyingIndexInput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();

        // Check file
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        long checksum = CodecUtil.retrieveChecksum(indexInput);
        indexInput.seek(0);
        IndexInput verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        Store.verify(verifyingIndexInput);
        assertThat(checksum, equalTo(((ChecksumIndexInput) verifyingIndexInput).getChecksum()));
        IOUtils.close(indexInput, verifyingIndexInput);

        // Corrupt file and check again
        corruptFile(dir, "foo.bar", "foo1.bar");
        verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo1.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        try {
            Store.verify(verifyingIndexInput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingIndexInput);
        IOUtils.close(dir);
    }

    private void readIndexInputFullyWithRandomSeeks(IndexInput indexInput) throws IOException {
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long pos = 0;
        while (pos < indexInput.length()) {
            assertEquals(pos, indexInput.getFilePointer());
            int op = random().nextInt(5);
            if (op == 0) {
                int shift = 100 - randomIntBetween(0, 200);
                pos = Math.min(indexInput.length() - 1, Math.max(0, pos + shift));
                indexInput.seek(pos);
            } else if (op == 1) {
                indexInput.readByte();
                pos++;
            } else {
                int min = (int) Math.min(indexInput.length() - pos, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                pos += min;
            }
        }
    }

    private void corruptFile(Directory dir, String fileIn, String fileOut) throws IOException {
        IndexInput input = dir.openInput(fileIn, IOContext.READONCE);
        IndexOutput output = dir.createOutput(fileOut, IOContext.DEFAULT);
        long len = input.length();
        byte[] b = new byte[1024];
        long broken = randomInt((int) len - 1);
        long pos = 0;
        while (pos < len) {
            int min = (int) Math.min(input.length() - pos, b.length);
            input.readBytes(b, 0, min);
            if (broken >= pos && broken < pos + min) {
                // Flip one byte
                int flipPos = (int) (broken - pos);
                b[flipPos] = (byte) (b[flipPos] ^ 42);
            }
            output.writeBytes(b, min);
            pos += min;
        }
        IOUtils.close(input, output);

    }

    public void assertDeleteContent(Store store, Directory dir) throws IOException {
        deleteContent(store.directory());
        assertThat(Arrays.toString(store.directory().listAll()), store.directory().listAll().length, equalTo(0));
        assertThat(store.stats(0L).sizeInBytes(), equalTo(0L));
        assertThat(dir.listAll().length, equalTo(0));
    }

    public static void assertConsistent(Store store, Store.MetadataSnapshot metadata) throws IOException {
        for (String file : store.directory().listAll()) {
            if (IndexWriter.WRITE_LOCK_NAME.equals(file) == false && file.startsWith("extra") == false) {
                assertTrue(
                    file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.asMap().containsKey(file)
                );
            } else {
                assertFalse(
                    file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.asMap().containsKey(file)
                );
            }
        }
    }

    public void testRecoveryDiff() throws IOException, InterruptedException {
        int numDocs = 2 + random().nextInt(100);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        long seed = random().nextLong();
        Store.MetadataSnapshot first;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean()) {
                    writer.commit();
                } else if (rarely(random)) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            first = store.getMetadata();
            assertDeleteContent(store, store.directory());
            store.close();
        }
        long time = new Date().getTime();
        while (time == new Date().getTime()) {
            Thread.sleep(10); // bump the time
        }
        Store.MetadataSnapshot second;
        Store store;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean()) {
                    writer.commit();
                } else if (rarely(random)) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            second = store.getMetadata();
        }
        Store.RecoveryDiff diff = first.recoveryDiff(second);
        assertThat(first.size(), equalTo(second.size()));
        for (StoreFileMetadata md : first) {
            assertThat(second.get(md.name()), notNullValue());
            // si files are different - containing timestamps etc
            assertThat(second.get(md.name()).isSame(md), equalTo(false));
        }
        assertThat(diff.different.size(), equalTo(first.size()));
        assertThat(diff.identical.size(), equalTo(0)); // in lucene 5 nothing is identical - we use random ids in file headers
        assertThat(diff.missing, empty());

        // check the self diff
        Store.RecoveryDiff selfDiff = first.recoveryDiff(first);
        assertThat(selfDiff.identical.size(), equalTo(first.size()));
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());

        // lets add some deletes
        Random random = new Random(seed);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setUseCompoundFile(random.nextBoolean());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        IndexWriter writer = new IndexWriter(store.directory(), iwc);
        writer.deleteDocuments(new Term("id", Integer.toString(random().nextInt(numDocs))));
        writer.commit();
        writer.close();
        Store.MetadataSnapshot metadata = store.getMetadata();
        StoreFileMetadata delFile = null;
        for (StoreFileMetadata md : metadata) {
            if (md.name().endsWith(".liv")) {
                delFile = md;
                break;
            }
        }
        Store.RecoveryDiff afterDeleteDiff = metadata.recoveryDiff(second);
        if (delFile != null) {
            assertThat(afterDeleteDiff.identical.size(), equalTo(metadata.size() - 2)); // segments_N + del file
            assertThat(afterDeleteDiff.different.size(), equalTo(0));
            assertThat(afterDeleteDiff.missing.size(), equalTo(2));
        } else {
            // an entire segment must be missing (single doc segment got dropped)
            assertThat(afterDeleteDiff.identical.size(), greaterThan(0));
            assertThat(afterDeleteDiff.different.size(), equalTo(0));
            assertThat(afterDeleteDiff.missing.size(), equalTo(1)); // the commit file is different
        }

        // check the self diff
        selfDiff = metadata.recoveryDiff(metadata);
        assertThat(selfDiff.identical.size(), equalTo(metadata.size()));
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());

        // add a new commit
        iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setUseCompoundFile(true); // force CFS - easier to test here since we know it will add 3 files
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        writer = new IndexWriter(store.directory(), iwc);
        writer.addDocument(docs.get(0));
        writer.close();

        Store.MetadataSnapshot newCommitMetadata = store.getMetadata();
        Store.RecoveryDiff newCommitDiff = newCommitMetadata.recoveryDiff(metadata);
        if (delFile != null) {
            assertThat(newCommitDiff.identical.size(), equalTo(newCommitMetadata.size() - 5)); // segments_N, del file, cfs, cfe, si for the
                                                                                               // new segment
            assertThat(newCommitDiff.different.size(), equalTo(1)); // the del file must be different
            assertThat(newCommitDiff.different.get(0).name(), endsWith(".liv"));
            assertThat(newCommitDiff.missing.size(), equalTo(4)); // segments_N,cfs, cfe, si for the new segment
        } else {
            assertThat(newCommitDiff.identical.size(), equalTo(newCommitMetadata.size() - 4)); // segments_N, cfs, cfe, si for the new
                                                                                               // segment
            assertThat(newCommitDiff.different.size(), equalTo(0));
            assertThat(newCommitDiff.missing.size(), equalTo(4)); // an entire segment must be missing (single doc segment got dropped) plus
                                                                  // the commit is different
        }

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testCleanupFromSnapshot() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        // this time random codec....
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(
            TestUtil.getDefaultCodec()
        );
        // we keep all commits and that allows us clean based on multiple snapshots
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(store.directory(), indexWriterConfig);
        int docs = 1 + random().nextInt(100);
        int numCommits = 0;
        for (int i = 0; i < docs; i++) {
            if (i > 0 && randomIntBetween(0, 10) == 0) {
                writer.commit();
                numCommits++;
            }
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);

        }
        if (numCommits < 1) {
            writer.commit();
            Document doc = new Document();
            doc.add(new TextField("id", "" + docs++, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }

        Store.MetadataSnapshot firstMeta = store.getMetadata();

        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(
                        new TextField(
                            "body",
                            TestUtil.randomRealisticUnicodeString(random()),
                            random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                        )
                    );
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot secondMeta = store.getMetadata();

        if (randomBoolean()) {
            store.cleanupAndVerify("test", firstMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertTrue(firstMeta.contains(file) || file.equals("write.lock"));
                if (secondMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertTrue("at least one file must not be in here since we have two commits?", numNotFound > 0);
        } else {
            store.cleanupAndVerify("test", secondMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertTrue(file, secondMeta.contains(file) || file.equals("write.lock"));
                if (firstMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertTrue("at least one file must not be in here since we have two commits?", numNotFound > 0);
        }

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testOnCloseCallback() throws IOException {
        final ShardId shardId = new ShardId(
            new Index(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), "_na_"),
            randomIntBetween(0, 100)
        );
        final AtomicInteger count = new AtomicInteger(0);
        final ShardLock lock = new DummyShardLock(shardId);

        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), lock, theLock -> {
            assertEquals(shardId, theLock.getShardId());
            assertEquals(lock, theLock);
            count.incrementAndGet();
        });
        assertEquals(count.get(), 0);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            store.close();
        }

        assertEquals(count.get(), 1);
    }

    public void testStoreStats() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
            .build();
        Store store = new Store(
            shardId,
            IndexSettingsModule.newIndexSettings("index", settings),
            StoreTests.newDirectory(random()),
            new DummyShardLock(shardId)
        );
        long initialStoreSize = 0;
        for (String extraFiles : store.directory().listAll()) {
            assertTrue("expected extraFS file but got: " + extraFiles, extraFiles.startsWith("extra"));
            initialStoreSize += store.directory().fileLength(extraFiles);
        }
        final long reservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        StoreStats stats = store.stats(reservedBytes);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        stats.add(null);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        final long otherStatsBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsReservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        stats.add(new StoreStats(otherStatsBytes, otherStatsReservedBytes));
        assertEquals(initialStoreSize + otherStatsBytes, stats.getSize().getBytes());
        assertEquals(Math.max(reservedBytes, 0L) + Math.max(otherStatsReservedBytes, 0L), stats.getReservedSize().getBytes());

        Directory dir = store.directory();
        final long length;
        try (IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            length = output.getFilePointer();
        }

        assertTrue(numNonExtraFiles(store) > 0);
        stats = store.stats(0L);
        assertEquals(stats.getSizeInBytes(), length + initialStoreSize);

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public int numNonExtraFiles(Store store) throws IOException {
        int numNonExtra = 0;
        for (String file : store.directory().listAll()) {
            if (file.startsWith("extra") == false) {
                numNonExtra++;
            }
        }
        return numNonExtra;
    }

    public void testMetadataSnapshotStreaming() throws Exception {
        Store.MetadataSnapshot outMetadataSnapshot = createMetadataSnapshot();
        org.opensearch.Version targetNodeVersion = randomVersion(random());

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(targetNodeVersion);
        outMetadataSnapshot.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        Store.MetadataSnapshot inMetadataSnapshot = new Store.MetadataSnapshot(in);
        Map<String, StoreFileMetadata> origEntries = new HashMap<>();
        origEntries.putAll(outMetadataSnapshot.asMap());
        for (Map.Entry<String, StoreFileMetadata> entry : inMetadataSnapshot.asMap().entrySet()) {
            assertThat(entry.getValue().name(), equalTo(origEntries.remove(entry.getKey()).name()));
        }
        assertThat(origEntries.size(), equalTo(0));
        assertThat(inMetadataSnapshot.getCommitUserData(), equalTo(outMetadataSnapshot.getCommitUserData()));
    }

    protected Store.MetadataSnapshot createMetadataSnapshot() {
        StoreFileMetadata storeFileMetadata1 = new StoreFileMetadata("segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION);
        StoreFileMetadata storeFileMetadata2 = new StoreFileMetadata("no_segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION);
        Map<String, StoreFileMetadata> storeFileMetadataMap = new HashMap<>();
        storeFileMetadataMap.put(storeFileMetadata1.name(), storeFileMetadata1);
        storeFileMetadataMap.put(storeFileMetadata2.name(), storeFileMetadata2);
        Map<String, String> commitUserData = new HashMap<>();
        commitUserData.put("userdata_1", "test");
        commitUserData.put("userdata_2", "test");
        return new Store.MetadataSnapshot(unmodifiableMap(storeFileMetadataMap), unmodifiableMap(commitUserData), 0);
    }

    public void testUserDataRead() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriterConfig config = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec());
        SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        config.setIndexDeletionPolicy(deletionPolicy);
        IndexWriter writer = new IndexWriter(store.directory(), config);
        Document doc = new Document();
        doc.add(new TextField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        Map<String, String> commitData = new HashMap<>(2);
        String syncId = "a sync id";
        commitData.put(Engine.SYNC_COMMIT_ID, syncId);
        writer.setLiveCommitData(commitData.entrySet());
        writer.commit();
        writer.close();
        Store.MetadataSnapshot metadata;
        metadata = store.getMetadata(randomBoolean() ? null : deletionPolicy.snapshot());
        assertFalse(metadata.asMap().isEmpty());
        // do not check for correct files, we have enough tests for that above
        assertThat(metadata.getCommitUserData().get(Engine.SYNC_COMMIT_ID), equalTo(syncId));
        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testStreamStoreFilesMetadata() throws Exception {
        Store.MetadataSnapshot metadataSnapshot = createMetadataSnapshot();
        int numOfLeases = randomIntBetween(0, 10);
        List<RetentionLease> peerRecoveryRetentionLeases = new ArrayList<>();
        for (int i = 0; i < numOfLeases; i++) {
            peerRecoveryRetentionLeases.add(
                new RetentionLease(
                    ReplicationTracker.getPeerRecoveryRetentionLeaseId(UUIDs.randomBase64UUID()),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
                )
            );
        }
        TransportNodesListShardStoreMetadata.StoreFilesMetadata outStoreFileMetadata =
            new TransportNodesListShardStoreMetadata.StoreFilesMetadata(
                new ShardId("test", "_na_", 0),
                metadataSnapshot,
                peerRecoveryRetentionLeases
            );
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        org.opensearch.Version targetNodeVersion = randomVersion(random());
        out.setVersion(targetNodeVersion);
        outStoreFileMetadata.writeTo(out);
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        TransportNodesListShardStoreMetadata.StoreFilesMetadata inStoreFileMetadata =
            new TransportNodesListShardStoreMetadata.StoreFilesMetadata(in);
        Iterator<StoreFileMetadata> outFiles = outStoreFileMetadata.iterator();
        for (StoreFileMetadata inFile : inStoreFileMetadata) {
            assertThat(inFile.name(), equalTo(outFiles.next().name()));
        }
        assertThat(outStoreFileMetadata.syncId(), equalTo(inStoreFileMetadata.syncId()));
        assertThat(outStoreFileMetadata.peerRecoveryRetentionLeases(), equalTo(peerRecoveryRetentionLeases));
    }

    public void testMarkCorruptedOnTruncatedSegmentsFile() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriter writer = new IndexWriter(store.directory(), iwc);

        int numDocs = 1 + random().nextInt(10);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        for (Document d : docs) {
            writer.addDocument(d);
        }
        writer.commit();
        writer.close();
        SegmentInfos segmentCommitInfos = store.readLastCommittedSegmentsInfo();
        store.directory().deleteFile(segmentCommitInfos.getSegmentsFileName());
        try (IndexOutput out = store.directory().createOutput(segmentCommitInfos.getSegmentsFileName(), IOContext.DEFAULT)) {
            // empty file
        }

        try {
            if (randomBoolean()) {
                store.getMetadata();
            } else {
                store.readLastCommittedSegmentsInfo();
            }
            fail("corrupted segments_N file");
        } catch (CorruptIndexException ex) {
            // expected
        }
        assertTrue(store.isMarkedCorrupted());
        // we have to remove the index since it's corrupted and might fail the MocKDirWrapper checkindex call
        Lucene.cleanLuceneIndex(store.directory());
        store.close();
    }

    public void testCanOpenIndex() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexWriterConfig iwc = newIndexWriterConfig();
        Path tempDir = createTempDir();
        final BaseDirectoryWrapper dir = newFSDirectory(tempDir);
        assertFalse(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        assertTrue(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        store.markStoreCorrupted(new CorruptIndexException("foo", "bar"));
        assertFalse(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        store.close();
    }

    public void testDeserializeCorruptionException() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory(); // I use ram dir to prevent that virusscanner being a PITA
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        CorruptIndexException ex = new CorruptIndexException("foo", "bar");
        store.markStoreCorrupted(ex);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertEquals(ex.getMessage(), e.getMessage());
            assertEquals(ex.toString(), e.toString());
            assertArrayEquals(ex.getStackTrace(), e.getStackTrace());
        }

        store.removeCorruptionMarker();
        assertFalse(store.isMarkedCorrupted());
        FileNotFoundException ioe = new FileNotFoundException("foobar");
        store.markStoreCorrupted(ioe);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertEquals("foobar (resource=preexisting_corruption)", e.getMessage());
            assertArrayEquals(ioe.getStackTrace(), e.getCause().getStackTrace());
        }
        store.close();
    }

    public void testCorruptionMarkerVersionCheck() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory(); // I use ram dir to prevent that virusscanner being a PITA

        try (Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId))) {
            final String corruptionMarkerName = Store.CORRUPTED_MARKER_NAME_PREFIX + UUIDs.randomBase64UUID();
            try (IndexOutput output = dir.createOutput(corruptionMarkerName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, Store.CODEC, Store.CORRUPTED_MARKER_CODEC_VERSION + randomFrom(1, 2, -1, -2, -3));
                // we only need the header to trigger the exception
            }
            final IOException ioException = expectThrows(IOException.class, store::failIfCorrupted);
            assertThat(ioException, anyOf(instanceOf(IndexFormatTooOldException.class), instanceOf(IndexFormatTooNewException.class)));
            assertThat(ioException.getMessage(), containsString(corruptionMarkerName));
        }
    }

    public void testEnsureIndexHasHistoryUUID() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        try (Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId))) {

            store.createEmpty(Version.LATEST);

            // remove the history uuid
            IndexWriterConfig iwc = new IndexWriterConfig(null).setCommitOnClose(false)
                // we don't want merges to happen here - we call maybe merge on the engine
                // later once we stared it up otherwise we would need to wait for it here
                // we also don't specify a codec here and merges should use the engines for this index
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
                Map<String, String> newCommitData = new HashMap<>();
                for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
                    if (entry.getKey().equals(Engine.HISTORY_UUID_KEY) == false) {
                        newCommitData.put(entry.getKey(), entry.getValue());
                    }
                }
                writer.setLiveCommitData(newCommitData.entrySet());
                writer.commit();
            }

            store.ensureIndexHasHistoryUUID();

            SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
        }
    }

    public void testHistoryUUIDCanBeForced() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        try (Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId))) {

            store.createEmpty(Version.LATEST);

            SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            final String oldHistoryUUID = segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY);

            store.bootstrapNewHistory();

            segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            assertThat(segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY), not(equalTo(oldHistoryUUID)));
        }
    }

    public void testGetPendingFiles() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final String testfile = "testfile";
        try (Store store = new Store(shardId, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(shardId))) {
            store.directory().createOutput(testfile, IOContext.DEFAULT).close();
            try (IndexInput input = store.directory().openInput(testfile, IOContext.DEFAULT)) {
                store.directory().deleteFile(testfile);
                assertEquals(FilterDirectory.unwrap(store.directory()).getPendingDeletions(), store.directory().getPendingDeletions());
            }
        }
    }

    public void testGetMetadataWithSegmentInfos() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST);
        SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
        Store.MetadataSnapshot metadataSnapshot = store.getMetadata(segmentInfos);
        // loose check for equality
        assertEquals(segmentInfos.getSegmentsFileName(), metadataSnapshot.getSegmentsFile().name());
        store.close();
    }

    public void testCleanupAndPreserveLatestCommitPoint() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(
            shardId,
            SEGMENT_REPLICATION_INDEX_SETTINGS,
            StoreTests.newDirectory(random()),
            new DummyShardLock(shardId)
        );
        commitRandomDocs(store);

        Store.MetadataSnapshot commitMetadata = store.getMetadata();

        // index more docs but only IW.flush, this will create additional files we'll clean up.
        final IndexWriter writer = indexRandomDocs(store);
        writer.flush();
        writer.close();

        final List<String> additionalSegments = new ArrayList<>();
        for (String file : store.directory().listAll()) {
            if (commitMetadata.contains(file) == false) {
                additionalSegments.add(file);
            }
        }
        assertFalse(additionalSegments.isEmpty());

        Collection<String> filesToConsiderForCleanUp = Stream.of(store.readLastCommittedSegmentsInfo().files(true), additionalSegments)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        // clean up everything not in the latest commit point.
        store.cleanupAndPreserveLatestCommitPoint(filesToConsiderForCleanUp, "test");

        // we want to ensure commitMetadata files are preserved after calling cleanup
        for (String existingFile : store.directory().listAll()) {
            if (!IndexWriter.WRITE_LOCK_NAME.equals(existingFile)) {
                assertTrue(commitMetadata.contains(existingFile));
                assertFalse(additionalSegments.contains(existingFile));
            }
        }
        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testGetSegmentMetadataMap() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(
            shardId,
            SEGMENT_REPLICATION_INDEX_SETTINGS,
            new NIOFSDirectory(createTempDir()),
            new DummyShardLock(shardId)
        );
        store.createEmpty(Version.LATEST);
        final Map<String, StoreFileMetadata> metadataSnapshot = store.getSegmentMetadataMap(store.readLastCommittedSegmentsInfo());
        // no docs indexed only _N file exists.
        assertTrue(metadataSnapshot.isEmpty());

        // commit some docs to create a commit point.
        commitRandomDocs(store);

        final Map<String, StoreFileMetadata> snapshotAfterCommit = store.getSegmentMetadataMap(store.readLastCommittedSegmentsInfo());
        assertFalse(snapshotAfterCommit.isEmpty());
        assertFalse(snapshotAfterCommit.keySet().stream().anyMatch((name) -> name.startsWith(IndexFileNames.SEGMENTS)));
        store.close();
    }

    public void testSegmentReplicationDiff() {
        final String segmentName = "_0.si";
        final StoreFileMetadata SEGMENT_FILE = new StoreFileMetadata(segmentName, 1L, "0", Version.LATEST);
        // source has file target is missing.
        Store.RecoveryDiff diff = Store.segmentReplicationDiff(Map.of(segmentName, SEGMENT_FILE), Collections.emptyMap());
        assertEquals(List.of(SEGMENT_FILE), diff.missing);
        assertTrue(diff.different.isEmpty());
        assertTrue(diff.identical.isEmpty());

        // target has file not on source.
        diff = Store.segmentReplicationDiff(Collections.emptyMap(), Map.of(segmentName, SEGMENT_FILE));
        assertTrue(diff.missing.isEmpty());
        assertTrue(diff.different.isEmpty());
        assertTrue(diff.identical.isEmpty());

        // source and target have identical file.
        diff = Store.segmentReplicationDiff(Map.of(segmentName, SEGMENT_FILE), Map.of(segmentName, SEGMENT_FILE));
        assertTrue(diff.missing.isEmpty());
        assertTrue(diff.different.isEmpty());
        assertEquals(List.of(SEGMENT_FILE), diff.identical);

        // source has diff copy of same file as target.
        StoreFileMetadata SOURCE_DIFF_FILE = new StoreFileMetadata(segmentName, 1L, "abc", Version.LATEST);
        diff = Store.segmentReplicationDiff(Map.of(segmentName, SOURCE_DIFF_FILE), Map.of(segmentName, SEGMENT_FILE));
        assertTrue(diff.missing.isEmpty());
        assertEquals(List.of(SOURCE_DIFF_FILE), diff.different);
        assertTrue(diff.identical.isEmpty());

        // ignore _N files if included in source map.
        final String segmentsFile = IndexFileNames.SEGMENTS.concat("_2");
        StoreFileMetadata SEGMENTS_FILE = new StoreFileMetadata(segmentsFile, 1L, "abc", Version.LATEST);
        diff = Store.segmentReplicationDiff(Map.of(segmentsFile, SEGMENTS_FILE), Collections.emptyMap());
        assertTrue(diff.missing.isEmpty());
        assertTrue(diff.different.isEmpty());
        assertTrue(diff.identical.isEmpty());
    }

    @SuppressForbidden(reason = "sets the SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY feature flag")
    public void testReadSegmentsFromOldIndices() throws Exception {
        int expectedIndexCreatedVersionMajor = SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION.luceneVersion.major;
        final String pathToTestIndex = "/indices/bwc/es-6.3.0/testIndex-es-6.3.0.zip";
        Path tmp = createTempDir();
        TestUtil.unzip(getClass().getResourceAsStream(pathToTestIndex), tmp);
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = null;

        try {
            FeatureFlagSetter.set(FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY);
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                    .build()
            );
            store = new Store(shardId, indexSettings, StoreTests.newMockFSDirectory(tmp), new DummyShardLock(shardId));
            assertEquals(expectedIndexCreatedVersionMajor, store.readLastCommittedSegmentsInfo().getIndexCreatedVersionMajor());
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }

    public void testReadSegmentsFromOldIndicesFailure() throws IOException {
        final String pathToTestIndex = "/indices/bwc/es-6.3.0/testIndex-es-6.3.0.zip";
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Path tmp = createTempDir();
        TestUtil.unzip(getClass().getResourceAsStream(pathToTestIndex), tmp);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
                .build()
        );
        Store store = new Store(shardId, indexSettings, StoreTests.newMockFSDirectory(tmp), new DummyShardLock(shardId));
        assertThrows(IndexFormatTooOldException.class, store::readLastCommittedSegmentsInfo);
        store.close();
    }

    public void testCommitSegmentInfos() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(
            shardId,
            SEGMENT_REPLICATION_INDEX_SETTINGS,
            StoreTests.newDirectory(random()),
            new DummyShardLock(shardId)
        );
        commitRandomDocs(store);
        final SegmentInfos lastCommittedInfos = store.readLastCommittedSegmentsInfo();
        final long expectedLocalCheckpoint = 1;
        final long expectedMaxSeqNo = 2;
        store.commitSegmentInfos(lastCommittedInfos, expectedMaxSeqNo, expectedLocalCheckpoint);

        final SegmentInfos updatedInfos = store.readLastCommittedSegmentsInfo();
        assertEquals(lastCommittedInfos.getVersion(), updatedInfos.getVersion());
        final Map<String, String> userData = updatedInfos.getUserData();
        assertEquals(expectedLocalCheckpoint, Long.parseLong(userData.get(LOCAL_CHECKPOINT_KEY)));
        assertEquals(expectedMaxSeqNo, Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO)));
        deleteContent(store.directory());
        IOUtils.close(store);
    }

    private void commitRandomDocs(Store store) throws IOException {
        IndexWriter writer = indexRandomDocs(store);
        writer.commit();
        writer.close();
    }

    private IndexWriter indexRandomDocs(Store store) throws IOException {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(
            TestUtil.getDefaultCodec()
        );
        indexWriterConfig.setCommitOnClose(false);
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(store.directory(), indexWriterConfig);
        int docs = 1 + random().nextInt(100);
        writer.commit();
        Document doc = new Document();
        doc.add(new TextField("id", "" + docs++, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(
            new TextField(
                "body",
                TestUtil.randomRealisticUnicodeString(random()),
                random().nextBoolean() ? Field.Store.YES : Field.Store.NO
            )
        );
        doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
        writer.addDocument(doc);
        return writer;
    }
}
