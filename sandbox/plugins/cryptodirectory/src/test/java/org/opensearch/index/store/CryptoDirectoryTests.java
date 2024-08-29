/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * SMB Tests using NIO FileSystem as index store type.
 */
// @RunWith(RandomizedRunner.class)
public class CryptoDirectoryTests extends OpenSearchBaseDirectoryTestCase {

    static final String KEY_FILE_NAME = "keyfile";

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        MasterKeyProvider keyProvider = mock(MasterKeyProvider.class);
        byte[] rawKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        java.util.Random rnd = Randomness.get();
        rnd.nextBytes(rawKey);
        rnd.nextBytes(encryptedKey);
        DataKeyPair dataKeyPair = new DataKeyPair(rawKey, encryptedKey);
        when(keyProvider.generateDataPair()).thenReturn(dataKeyPair);
        return new CryptoDirectory(FSLockFactory.getDefault(), file, Security.getProvider("SunJCE"), keyProvider);
    }

    @Override
    public void testCreateTempOutput() throws Throwable {
        try (Directory dir = getDirectory(createTempDir())) {
            List<String> names = new ArrayList<>();
            int iters = atLeast(50);
            for (int iter = 0; iter < iters; iter++) {
                IndexOutput out = dir.createTempOutput("foo", "bar", newIOContext(random()));
                names.add(out.getName());
                out.writeVInt(iter);
                out.close();
            }
            for (int iter = 0; iter < iters; iter++) {
                IndexInput in = dir.openInput(names.get(iter), newIOContext(random()));
                assertEquals(iter, in.readVInt());
                in.close();
            }

            Set<String> files = Arrays.stream(dir.listAll())
                .filter(file -> !ExtrasFS.isExtra(file)) // remove any ExtrasFS stuff.
                .filter(file -> !file.equals(KEY_FILE_NAME)) // remove keyfile.
                .collect(Collectors.toSet());

            assertEquals(new HashSet<String>(names), files);
        }
    }

    @Override
    public void testThreadSafetyInListAll() throws Exception {
        /*
        try (Directory dir = getDirectory(createTempDir("testThreadSafety"))) {
            if (dir instanceof BaseDirectoryWrapper) {
                // we are not making a real index, just writing, reading files.
                ((BaseDirectoryWrapper) dir).setCheckIndexOnClose(false);
            }
            if (dir instanceof MockDirectoryWrapper) {
                // makes this test really slow
                ((MockDirectoryWrapper) dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
            }

            AtomicBoolean stop = new AtomicBoolean();
            Thread writer = new Thread(() -> {
                try {
                    for (int i = 0, max = RandomizedTest.randomIntBetween(500, 1000); i < max; i++) {
                        String fileName = "file-" + i;
                        try (IndexOutput output = dir.createOutput(fileName, newIOContext(random()))) {
                            assert output != null;
                            // Add some lags so that the other thread can read the content of the
                            // directory.
                            Thread.yield();
                        }
                        assertTrue(slowFileExists(dir, fileName));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    stop.set(true);
                }
            });

            Thread reader = new Thread(() -> {
                try {
                    Random rnd = new Random(RandomizedTest.randomLong());
                    while (!stop.get()) {
                        String[] files = Arrays.stream(dir.listAll())
                            .filter(name -> !ExtrasFS.isExtra(name)) // Ignore anything from ExtraFS.
                            .filter(name -> !name.equals(KEY_FILE_NAME)) // remove keyfile.
                            .toArray(String[]::new);

                        if (files.length > 0) {
                            do {
                                String file = RandomPicks.randomFrom(rnd, files);
                                try (IndexInput input = dir.openInput(file, newIOContext(random()))) {
                                    // Just open, nothing else.
                                    assert input != null;
                                } catch (@SuppressWarnings("unused") AccessDeniedException e) {
                                    // Access denied is allowed for files for which the output is still open
                                    // (MockDirectoryWriter enforces
                                    // this, for example). Since we don't synchronize with the writer thread,
                                    // just ignore it.
                                } catch (IOException e) {
                                    throw new UncheckedIOException("Something went wrong when opening: " + file, e);
                                }
                            } while (rnd.nextInt(3) != 0); // Sometimes break and list files again.
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            reader.start();
            writer.start();

            writer.join();
            reader.join();
        } */
    }
}
