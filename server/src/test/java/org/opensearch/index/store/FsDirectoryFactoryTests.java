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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SleepingLockWrapper;
import org.apache.lucene.util.Constants;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

import javax.crypto.SecretKey;
import org.opensearch.common.Randomness;
import java.security.cert.CertificateException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import static org.opensearch.test.store.MockFSDirectoryFactory.FILE_SYSTEM_BASED_STORE_TYPES;

public class FsDirectoryFactoryTests extends OpenSearchTestCase {

    public void testPreload() throws IOException {
        doTestPreload();
        doTestPreload("nvd", "dvd", "tim");
        doTestPreload("*");
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "dvd", "bar")
            .build();
        try (Directory directory = newDirectory(build)) {
            assertTrue(FsDirectoryFactory.isHybridFs(directory));
            FsDirectoryFactory.HybridDirectory hybridDirectory = (FsDirectoryFactory.HybridDirectory) directory;
            // test default hybrid mmap extensions
            assertTrue(hybridDirectory.useDelegate("foo.nvd"));
            assertTrue(hybridDirectory.useDelegate("foo.dvd"));
            assertTrue(hybridDirectory.useDelegate("foo.tim"));
            assertTrue(hybridDirectory.useDelegate("foo.tip"));
            assertTrue(hybridDirectory.useDelegate("foo.dim"));
            assertTrue(hybridDirectory.useDelegate("foo.kdd"));
            assertTrue(hybridDirectory.useDelegate("foo.kdi"));
            assertTrue(hybridDirectory.useDelegate("foo.cfs"));
            assertTrue(hybridDirectory.useDelegate("foo.doc"));
            assertFalse(hybridDirectory.useDelegate("foo.pos"));
            assertFalse(hybridDirectory.useDelegate("foo.pay"));
            MMapDirectory delegate = hybridDirectory.getDelegate();
            assertThat(delegate, Matchers.instanceOf(FsDirectoryFactory.PreLoadMMapDirectory.class));
            FsDirectoryFactory.PreLoadMMapDirectory preLoadMMapDirectory = (FsDirectoryFactory.PreLoadMMapDirectory) delegate;
            assertTrue(preLoadMMapDirectory.useDelegate("foo.dvd"));
            assertTrue(preLoadMMapDirectory.useDelegate("foo.bar"));
            assertFalse(preLoadMMapDirectory.useDelegate("foo.cfs"));
        }
        build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "nvd", "dvd", "cfs")
            .putList(IndexModule.INDEX_STORE_HYBRID_MMAP_EXTENSIONS.getKey(), "nvd", "dvd", "tim", "pos", "pay")
            .build();
        try (Directory directory = newDirectory(build)) {
            assertTrue(FsDirectoryFactory.isHybridFs(directory));
            FsDirectoryFactory.HybridDirectory hybridDirectory = (FsDirectoryFactory.HybridDirectory) directory;
            // test custom hybrid mmap extensions
            assertTrue(hybridDirectory.useDelegate("foo.nvd"));
            assertTrue(hybridDirectory.useDelegate("foo.dvd"));
            assertTrue(hybridDirectory.useDelegate("foo.tim"));
            assertFalse(hybridDirectory.useDelegate("foo.tip"));
            assertFalse(hybridDirectory.useDelegate("foo.dim"));
            assertFalse(hybridDirectory.useDelegate("foo.kdd"));
            assertFalse(hybridDirectory.useDelegate("foo.kdi"));
            assertFalse(hybridDirectory.useDelegate("foo.cfs"));
            assertFalse(hybridDirectory.useDelegate("foo.doc"));
            assertTrue(hybridDirectory.useDelegate("foo.pos"));
            assertTrue(hybridDirectory.useDelegate("foo.pay"));
            MMapDirectory delegate = hybridDirectory.getDelegate();
            assertThat(delegate, Matchers.instanceOf(FsDirectoryFactory.PreLoadMMapDirectory.class));
            FsDirectoryFactory.PreLoadMMapDirectory preLoadMMapDirectory = (FsDirectoryFactory.PreLoadMMapDirectory) delegate;
            assertTrue(preLoadMMapDirectory.useDelegate("foo.dvd"));
            assertFalse(preLoadMMapDirectory.useDelegate("foo.bar"));
            assertTrue(preLoadMMapDirectory.useDelegate("foo.cfs"));
            assertTrue(preLoadMMapDirectory.useDelegate("foo.nvd"));
        }
    }

    private Directory newDirectory(Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(idxSettings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(idxSettings.getIndex(), 0));
        return new FsDirectoryFactory().newDirectory(idxSettings, path);
    }

    private void doTestPreload(String... preload) throws IOException {
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "mmapfs")
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), preload)
            .build();
        Directory directory = newDirectory(build);
        try (Directory dir = directory) {
            assertSame(dir, directory); // prevent warnings
            assertFalse(directory instanceof SleepingLockWrapper);
            if (preload.length == 0) {
                assertTrue(directory.toString(), directory instanceof MMapDirectory);
                assertFalse(((MMapDirectory) directory).getPreload());
            } else if (Arrays.asList(preload).contains("*")) {
                assertTrue(directory.toString(), directory instanceof MMapDirectory);
                assertTrue(((MMapDirectory) directory).getPreload());
            } else {
                assertTrue(directory.toString(), directory instanceof FsDirectoryFactory.PreLoadMMapDirectory);
                FsDirectoryFactory.PreLoadMMapDirectory preLoadMMapDirectory = (FsDirectoryFactory.PreLoadMMapDirectory) directory;
                for (String ext : preload) {
                    assertTrue("ext: " + ext, preLoadMMapDirectory.useDelegate("foo." + ext));
                    assertTrue("ext: " + ext, preLoadMMapDirectory.getDelegate().getPreload());
                }
                assertFalse(preLoadMMapDirectory.useDelegate("XXX"));
                assertFalse(preLoadMMapDirectory.getPreload());
                preLoadMMapDirectory.close();
                expectThrows(
                    AlreadyClosedException.class,
                    () -> preLoadMMapDirectory.getDelegate().openInput("foo.bar", IOContext.DEFAULT)
                );
            }
        }
        expectThrows(
            AlreadyClosedException.class,
            () -> directory.openInput(randomBoolean() && preload.length != 0 ? "foo." + preload[0] : "foo.bar", IOContext.DEFAULT)
        );
    }

    public void testStoreDirectory() throws IOException {
        Index index = new Index("foo", "fooUUID");
        final Path tempDir = createTempDir().resolve(index.getUUID()).resolve("0");
        // default
        doTestStoreDirectory(tempDir, null, IndexModule.Type.FS);
        // explicit directory impls
        for (IndexModule.Type type : FILE_SYSTEM_BASED_STORE_TYPES) {
            doTestStoreDirectory(tempDir, type.name().toLowerCase(Locale.ROOT), type);
        }
    }

    private void doTestStoreDirectory(Path tempDir, String typeSettingValue, IndexModule.Type type) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        if (typeSettingValue != null) {
            settingsBuilder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), typeSettingValue);
        }
        if (type.toString().equals("CRYPTOFS")) {
            final String KEYSTORE_PATH = createTempDir().toString() + "/keystore.ks";
            settingsBuilder.put(FsDirectoryFactory.INDEX_KMS_ALIAS_SETTING.getKey(), "cryptotest")
                .put(FsDirectoryFactory.INDEX_KMS_PASSWORD_SETTING.getKey(), "cryptopass")
                .put(FsDirectoryFactory.INDEX_KMS_PATH_SETTING.getKey(), KEYSTORE_PATH);

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
                try (OutputStream os = Files.newOutputStream(Path.of(KEYSTORE_PATH), StandardOpenOption.CREATE)) {
                    keystore.store(os, keyPass.toCharArray());
                }

            } catch (java.security.AccessControlException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
                throw new IOException(e.getMessage());
            }
        }

        Settings settings = settingsBuilder.build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        FsDirectoryFactory service = new FsDirectoryFactory();
        try (Directory directory = service.newFSDirectory(tempDir, NoLockFactory.INSTANCE, indexSettings)) {
            switch (type) {
                case HYBRIDFS:
                    assertTrue(FsDirectoryFactory.isHybridFs(directory));
                    break;
                // simplefs was removed in Lucene 9; support for enum is maintained for bwc
                case SIMPLEFS:
                case NIOFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof NIOFSDirectory);
                    break;
                case MMAPFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof MMapDirectory);
                    break;
                case FS:
                    if (Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
                        assertTrue(FsDirectoryFactory.isHybridFs(directory));
                    } else {
                        assertTrue(directory.toString(), directory instanceof NIOFSDirectory);
                    }
                    break;
                case CRYPTOFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof CryptoDirectory);
                    break;
                default:
                    fail();
            }
        }
    }
}
