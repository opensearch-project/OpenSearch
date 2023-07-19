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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.hamcrest.Matchers;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.apache.lucene.store.FSLockFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.opensearch.test.VersionUtils.randomVersion;

import javax.crypto.SecretKey;
import org.opensearch.common.Randomness;
import java.security.cert.CertificateException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import org.junit.BeforeClass;

public class CryptoDirectoryTests extends OpenSearchTestCase {

    private static IndexSettings INDEX_SETTINGS;
    private static String KEYSTORE_PATH;

    IndexSettings SEGMENT_REPLICATION_INDEX_SETTINGS = new IndexSettings(
        INDEX_SETTINGS.getIndexMetadata(),
        Settings.builder().put(INDEX_SETTINGS.getSettings()).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build()
    );

    private static final Version MIN_SUPPORTED_LUCENE_VERSION = org.opensearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;

    private static void setupIndexSettings() {
        KEYSTORE_PATH = createTempDir().toString() + "/keystore.ks";
        INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(FsDirectoryFactory.INDEX_KMS_ALIAS_SETTING.getKey(), "cryptotest")
                .put(FsDirectoryFactory.INDEX_KMS_PASSWORD_SETTING.getKey(), "cryptopass")
                .put(FsDirectoryFactory.INDEX_KMS_PATH_SETTING.getKey(), KEYSTORE_PATH)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "cryptofs")
                .build()
        );
    }

    @BeforeClass
    private static void setupLocalKeyStore() throws IOException {
        setupIndexSettings();
        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            byte[] keyMaterial = new byte[32];
            java.util.Random rnd = Randomness.get();
            rnd.nextBytes(keyMaterial);
            SecretKey master = new javax.crypto.spec.SecretKeySpec(keyMaterial, "AES");
            String alias = "cryptotest";
            String keyPass = "cryptopass";
            keystore.load(null, keyPass.toCharArray());
            keystore.setEntry(alias, new KeyStore.SecretKeyEntry(master), new KeyStore.PasswordProtection(keyPass.toCharArray()));
            String keyStorePath = INDEX_SETTINGS.getValue(FsDirectoryFactory.INDEX_KMS_PATH_SETTING);
            try (OutputStream os = Files.newOutputStream(Path.of(keyStorePath), StandardOpenOption.CREATE)) {
                keystore.store(os, keyPass.toCharArray());
            }

        } catch (java.security.AccessControlException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage());
        }
    }

    private Directory newCryptoDirectory() {
        return newCryptoDirectory(createTempDir());
    }

    private Directory newCryptoDirectory(Path dir) {
        try {
            return new FsDirectoryFactory().newFSDirectory(dir, FSLockFactory.getDefault(), INDEX_SETTINGS);
        } catch (IOException e) {
            return null;
        }
    }

    public void testRefCount() {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexSettings indexSettings = INDEX_SETTINGS;
        Store store = new Store(shardId, indexSettings, newCryptoDirectory(), new DummyShardLock(shardId));
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
        Directory dir = newCryptoDirectory();
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
        Directory dir = newCryptoDirectory();
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
        Directory dir = newCryptoDirectory();
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
        Directory dir = newCryptoDirectory();
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

    public void testCheckIntegrity() throws IOException {
        Directory dir = newCryptoDirectory();
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
        Directory dir = newCryptoDirectory();
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

    public void testConfidentiality() throws IOException {
        Path tempDir = createTempDir();
        Directory dir = newCryptoDirectory(tempDir);
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

        // use non crypto directory and try again
        final BaseDirectoryWrapper newDir = newFSDirectory(tempDir);
        verifyingIndexInput = new Store.VerifyingIndexInput(newDir.openInput("foo.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        try {
            Store.verify(verifyingIndexInput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingIndexInput);
        IOUtils.close(dir, newDir);
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
        final String KEY_FILE_NAME = "keyfile";
        for (String file : store.directory().listAll()) {
            if (IndexWriter.WRITE_LOCK_NAME.equals(file) == false && file.startsWith("extra") == false) {
                // Ignore keyfile as it is not present in metadata
                if (file.equals(KEY_FILE_NAME)) continue;
                assertTrue(
                    file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.asMap().containsKey(file)
                );
            } else {
                // Ignore keyfile as it is not present in metadata
                if (file.equals(KEY_FILE_NAME)) continue;
                assertFalse(
                    file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.asMap().containsKey(file)
                );
            }
        }
    }

    public void testOnCloseCallback() throws IOException {
        final ShardId shardId = new ShardId(
            new Index(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), "_na_"),
            randomIntBetween(0, 100)
        );
        final AtomicInteger count = new AtomicInteger(0);
        final ShardLock lock = new DummyShardLock(shardId);

        Store store = new Store(shardId, INDEX_SETTINGS, newCryptoDirectory(), lock, theLock -> {
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

    public void testDeserializeCorruptionException() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = newCryptoDirectory();
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
        final Directory dir = newCryptoDirectory();

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
        try (Store store = new Store(shardId, INDEX_SETTINGS, newCryptoDirectory(), new DummyShardLock(shardId))) {

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
        try (Store store = new Store(shardId, INDEX_SETTINGS, newCryptoDirectory(), new DummyShardLock(shardId))) {

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
        try (Store store = new Store(shardId, INDEX_SETTINGS, newCryptoDirectory(), new DummyShardLock(shardId))) {
            store.directory().createOutput(testfile, IOContext.DEFAULT).close();
            try (IndexInput input = store.directory().openInput(testfile, IOContext.DEFAULT)) {
                store.directory().deleteFile(testfile);
                assertEquals(FilterDirectory.unwrap(store.directory()).getPendingDeletions(), store.directory().getPendingDeletions());
            }
        }
    }

    public void testGetMetadataWithSegmentInfos() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, newCryptoDirectory(), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST);
        SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
        Store.MetadataSnapshot metadataSnapshot = store.getMetadata(segmentInfos);
        // loose check for equality
        assertEquals(segmentInfos.getSegmentsFileName(), metadataSnapshot.getSegmentsFile().name());
        store.close();
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
