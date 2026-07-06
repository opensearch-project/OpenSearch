/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.crypto.CryptoRegistryException;
import org.opensearch.plugins.CryptoKeyProviderPlugin;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

/**
 * Manages the index-level PME keyfile.
 *
 * <p>The keyfile is stored at:
 * <pre>
 *   &lt;dataPath&gt;/indices/&lt;indexUUID&gt;/keyfile
 * </pre>
 * where {@code <dataPath>} is the node data directory and {@code <indexUUID>} is the index
 * UUID. This is two levels above the per-shard data path
 * ({@code .../indices/<indexUUID>/<shardId>/index}), so a single keyfile is shared across
 * all shards of the same index on this node. The same convention is used by
 * {@code opensearch-storage-encryption} for Lucene segment encryption.
 *
 * <p>The keyfile stores the {@link MasterKeyProvider}-wrapped (encrypted) form of the
 * 32-byte AES-GCM data key used to derive per-file footer keys.
 *
 * <h2>Write path (first shard to open on this node for a given index)</h2>
 * <ol>
 *   <li>Call {@link MasterKeyProvider#generateDataPair()} to obtain the raw + encrypted key.</li>
 *   <li>Write the encrypted key to {@code keyfile.tmp} on the plain filesystem, then
 *       atomically rename it to {@code keyfile}.  Concurrent shard engines that lose the
 *       rename race delete the tmp file and read the winner's keyfile instead.</li>
 *   <li>Return the raw key; caller must zero after use.</li>
 * </ol>
 * <p>Note: Lucene storage encryption uses a {@code Lucene Directory} for keyfile writes and
 * relies on the directory's own concurrency guarantees rather than an explicit atomic rename.
 * We use tmp+rename here because we write directly to the filesystem without a directory
 * abstraction.
 *
 * <h2>Read path (keyfile already exists)</h2>
 * <ol>
 *   <li>Read encrypted bytes from {@code keyfile}.</li>
 *   <li>Decrypt with {@link MasterKeyProvider#decryptKey(byte[])}.</li>
 *   <li>Return the raw key; caller must zero after use.</li>
 * </ol>
 */
final class PmeKeyfileManager {

    private static final Logger logger = LogManager.getLogger(PmeKeyfileManager.class);

    static final String KEYFILE_NAME = "keyfile";

    private PmeKeyfileManager() {}

    /**
     * Returns the raw 32-byte data key for the given index, creating the keyfile if absent.
     *
     * <p>This method is safe to call concurrently from multiple shard engines on the same index:
     * the atomic rename ensures exactly one key is persisted, and the loser re-reads that key.
     *
     * @param indexDataPath index-level data directory (parent of all shard directories)
     * @param cryptoMetadata encryption configuration for the index
     * @return raw 32-byte data key; <strong>caller must zero after use</strong>
     * @throws IOException              if keyfile I/O fails
     * @throws IllegalArgumentException if the decrypted key is not {@value PmeKeyDerivation#DATA_KEY_BYTES} bytes
     */
    static byte[] initOrLoad(Path indexDataPath, CryptoMetadata cryptoMetadata) throws IOException {
        Path keyfilePath = indexDataPath.resolve(KEYFILE_NAME);
        logger.trace("PME keyfile initOrLoad: path=[{}] exists=[{}]", keyfilePath, Files.exists(keyfilePath));
        try (MasterKeyProvider provider = createKeyProvider(cryptoMetadata)) {
            if (Files.exists(keyfilePath)) {
                return loadKey(keyfilePath, provider);
            }
            return createKey(indexDataPath, keyfilePath, provider);
        }
    }

    /**
     * Package-private overload that accepts a pre-built {@link MasterKeyProvider} directly.
     * Intended for unit tests only — production code must use
     * {@link #initOrLoad(Path, CryptoMetadata)}.
     */
    static byte[] initOrLoad(Path indexDataPath, MasterKeyProvider provider) throws IOException {
        Path keyfilePath = indexDataPath.resolve(KEYFILE_NAME);
        logger.trace("PME keyfile initOrLoad (test overload): path=[{}] exists=[{}]", keyfilePath, Files.exists(keyfilePath));
        if (Files.exists(keyfilePath)) {
            return loadKey(keyfilePath, provider);
        }
        return createKey(indexDataPath, keyfilePath, provider);
    }

    // ---- private helpers ----

    private static byte[] loadKey(Path keyfilePath, MasterKeyProvider provider) throws IOException {
        logger.trace("PME keyfile: loading existing keyfile from [{}]", keyfilePath);
        byte[] encryptedKey = Files.readAllBytes(keyfilePath);
        byte[] rawKey = provider.decryptKey(encryptedKey);
        validateKeyLength(rawKey, keyfilePath);
        logger.trace("PME keyfile: key loaded and decrypted from [{}]", keyfilePath);
        return rawKey;
    }

    private static byte[] createKey(Path indexDataPath, Path keyfilePath, MasterKeyProvider provider) throws IOException {
        logger.trace("PME keyfile: no keyfile found, generating new data key at [{}]", keyfilePath);
        DataKeyPair pair = provider.generateDataPair();
        byte[] rawKey = pair.getRawKey();
        validateKeyLength(rawKey, keyfilePath);

        // Use a unique tmp name per attempt so concurrent callers don't overwrite each other's
        // tmp file before the atomic rename. Each loser sees FileAlreadyExistsException or
        // NoSuchFileException (if another thread already won and moved the same tmp file) and
        // falls through to load the winner's keyfile.
        // TODO: Research whether we can find a more coordinated way to coordinate this.
        // TODO: I am wondering whether we should just use a JVM synchronized lock
        Path tmp = indexDataPath.resolve("keyfile.tmp." + UUID.randomUUID());
        Files.write(tmp, pair.getEncryptedKey(), StandardOpenOption.CREATE_NEW);
        try {
            Files.move(tmp, keyfilePath, StandardCopyOption.ATOMIC_MOVE);
            logger.trace("PME keyfile: successfully created keyfile at [{}]", keyfilePath);
        } catch (FileAlreadyExistsException | NoSuchFileException e) {
            // Another shard won the race — use their keyfile.
            logger.trace("PME keyfile: lost rename race at [{}], loading winner's keyfile", keyfilePath);
            Files.deleteIfExists(tmp);
            Arrays.fill(rawKey, (byte) 0);
            rawKey = loadKey(keyfilePath, provider);
        }
        return rawKey;
    }

    private static void validateKeyLength(byte[] rawKey, Path keyfilePath) {
        int expected = PmeKeyDerivation.DATA_KEY_BYTES;
        int actual = rawKey == null ? 0 : rawKey.length;
        if (actual != expected) {
            throw new IllegalArgumentException(
                "PME keyfile decrypted to " + actual + " bytes, expected " + expected + ": " + keyfilePath
            );
        }
    }

    private static MasterKeyProvider createKeyProvider(CryptoMetadata cm) {
        CryptoHandlerRegistry registry = CryptoHandlerRegistry.getInstance();
        if (registry == null) {
            throw new IllegalStateException("CryptoHandlerRegistry not initialized");
        }
        CryptoKeyProviderPlugin plugin = registry.getCryptoKeyProviderPlugin(cm.keyProviderType());
        if (plugin == null) {
            throw new CryptoRegistryException(
                cm.keyProviderName(),
                cm.keyProviderType(),
                "Crypto key provider plugin not found for PME write path"
            );
        }
        return plugin.createKeyProvider(cm);
    }
}



