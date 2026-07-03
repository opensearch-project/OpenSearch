/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PmeKeyfileManagerTests extends OpenSearchTestCase {

    private static final int KEY_LEN = PmeKeyDerivation.DATA_KEY_BYTES;

    // ---- helpers ----

    private static byte[] makeRawKey(byte fill) {
        byte[] k = new byte[KEY_LEN];
        Arrays.fill(k, fill);
        return k;
    }

    private static MasterKeyProvider mockProvider(byte[] rawKey, byte[] encryptedKey) throws IOException {
        MasterKeyProvider provider = mock(MasterKeyProvider.class);
        when(provider.generateDataPair()).thenReturn(new DataKeyPair(rawKey.clone(), encryptedKey.clone()));
        when(provider.decryptKey(any())).thenReturn(rawKey.clone());
        return provider;
    }

    // ---- initOrLoad: keyfile absent ----

    public void testCreatesKeyfileWhenAbsent() throws IOException {
        Path dir = createTempDir();
        byte[] rawKey = makeRawKey((byte) 0xAB);
        byte[] encKey = new byte[] { 1, 2, 3 };

        byte[] result = PmeKeyfileManager.initOrLoad(dir, mockProvider(rawKey, encKey));
        try {
            assertArrayEquals(rawKey, result);
        } finally {
            Arrays.fill(result, (byte) 0);
        }

        Path keyfile = dir.resolve(PmeKeyfileManager.KEYFILE_NAME);
        assertTrue("keyfile must exist after init", Files.exists(keyfile));
        assertArrayEquals(encKey, Files.readAllBytes(keyfile));
    }

    // ---- initOrLoad: keyfile present ----

    public void testLoadsExistingKeyfile() throws IOException {
        Path dir = createTempDir();
        byte[] rawKey = makeRawKey((byte) 0xCD);
        byte[] encKey = new byte[] { 9, 8, 7 };

        // Pre-write the keyfile.
        Files.write(dir.resolve(PmeKeyfileManager.KEYFILE_NAME), encKey);

        byte[] result = PmeKeyfileManager.initOrLoad(dir, mockProvider(rawKey, encKey));
        try {
            assertArrayEquals(rawKey, result);
        } finally {
            Arrays.fill(result, (byte) 0);
        }
    }

    // ---- initOrLoad: race condition ----

    public void testConcurrentInitProducesConsistentKey() throws Exception {
        Path dir = createTempDir();
        byte[] rawKey = makeRawKey((byte) 0x77);
        byte[] encKey = new byte[] { 5, 6, 7, 8 };

        int threads = 4;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger(0);
        byte[][] results = new byte[threads][];
        Thread[] workers = new Thread[threads];

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            workers[idx] = new Thread(() -> {
                try {
                    MasterKeyProvider p = mockProvider(rawKey, encKey);
                    start.await();
                    results[idx] = PmeKeyfileManager.initOrLoad(dir, p);
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
            workers[idx].start();
        }

        start.countDown();
        for (Thread w : workers) {
            w.join(10_000);
        }

        assertEquals("no thread should have failed", 0, errors.get());
        for (byte[] r : results) {
            assertNotNull(r);
            assertArrayEquals("all threads must get the same key", rawKey, r);
            Arrays.fill(r, (byte) 0);
        }

        assertTrue(Files.exists(dir.resolve(PmeKeyfileManager.KEYFILE_NAME)));
        // No stray tmp files should remain (each uses a unique UUID name).
        long tmpCount;
        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            tmpCount = stream.filter(p -> p.getFileName().toString().startsWith("keyfile.tmp.")).count();
        }
        assertEquals("stray tmp keyfiles should be cleaned up", 0, tmpCount);
    }

    // ---- validation ----

    public void testThrowsIfDecryptedKeyHasWrongLength() throws IOException {
        Path dir = createTempDir();
        byte[] shortKey = new byte[16]; // wrong length

        MasterKeyProvider provider = mock(MasterKeyProvider.class);
        when(provider.generateDataPair()).thenReturn(new DataKeyPair(shortKey, new byte[] { 1 }));
        when(provider.decryptKey(any())).thenReturn(shortKey);

        expectThrows(IllegalArgumentException.class, () -> PmeKeyfileManager.initOrLoad(dir, provider));
    }
}
