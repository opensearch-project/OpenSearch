/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.crypto.CryptoRegistryException;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Manages {@link DatafusionReader} instances per shard.
 * <p>
 * On refresh, a new reader is created from the updated catalog snapshot.
 * File lifecycle events (add/delete) are delegated to the node-level
 * {@link DataFusionService} for cache management.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private final Map<Long, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;
    private final NativeStoreHandle dataformatAwareStoreHandle;
    /**
     * Sourced from {@code index.sort.field} once at construction; passed to every
     * {@link DatafusionReader} created on refresh so the native side can declare
     * file sort order to DataFusion's query optimizer.
     */
    private final List<String> sortFields;
    /** Parallel to {@link #sortFields}; values are {@code "asc"} or {@code "desc"}. */
    private final List<String> sortOrders;
    private final Optional<CryptoMetadata> cryptoMetadata;
    /** Path to the PME keyfile shared across all shards of this index on this node. */
    private final Path keyfilePath;


    /**
     * Creates a reader manager with sort order and optional crypto metadata.
     */
    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders,
        Optional<CryptoMetadata> cryptoMetadata
    ) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
        this.dataformatAwareStoreHandle = dataformatAwareStoreHandle;
        this.sortFields = sortFields == null ? List.of() : List.copyOf(sortFields);
        this.sortOrders = sortOrders == null ? List.of() : List.copyOf(sortOrders);
        this.cryptoMetadata = cryptoMetadata;
        // keyfile is at <nodeDataPath>/indices/keyfile — two levels up from shard data path.
        // shardPath.getDataPath() = .../indices/<shardId>
        // .getParent().getParent() = .../indices (index-shared keyfile location)
        this.keyfilePath = shardPath.getDataPath().getParent().getParent().resolve("keyfile");
    }

    /**
     * Creates a reader manager without crypto metadata (unencrypted indices).
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     * @param dataformatAwareStoreHandle per-format native store handle for reads (null if not available).
     *                                   Pointer is extracted at reader creation time via {@code getPointer()}.
     *                                   0 means use default local file system.
     * @param sortFields {@code index.sort.field} values, or empty if no index sort. Threaded to the native
     *                   reader so the indexed scan path can decide whether to iterate segments in reverse
     *                   catalog-snapshot order to feed a {@code TopK} above us.
     * @param sortOrders {@code index.sort.order} values ("asc"/"desc"), parallel to {@code sortFields}.
     */
    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders
    ) {
        this(dataFormat, shardPath, dataFusionService, dataformatAwareStoreHandle, sortFields, sortOrders, Optional.empty());
    }

    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        NativeStoreHandle dataformatAwareStoreHandle,
        Optional<CryptoMetadata> cryptoMetadata
    ) {
        this(dataFormat, shardPath, dataFusionService, dataformatAwareStoreHandle, List.of(), List.of(), cryptoMetadata);
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (catalogSnapshot == null) {
            throw new IllegalArgumentException("catalogSnapshot must not be null");
        }
        DatafusionReader reader = readers.get(catalogSnapshot.getId());
        if (reader == null) {
            throw new IOException("No DataFusion reader available for catalog snapshot [version=" + catalogSnapshot.getId() + "]");
        }
        return reader;
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot.getId());
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        Collection<String> absolutePaths = toAbsolutePaths(files);
        long storePtr = storePointerOrDefault(dataformatAwareStoreHandle);
        if (storePtr > 0) {
            dataFusionService.onFilesAddedWithStore(absolutePaths, storePtr);
        } else {
            dataFusionService.onFilesAdded(absolutePaths);
        }
    }

    /**
     * Resolves the native store pointer for cache warming. Returns {@code 0} when there is no
     * live handle (no per-shard remote store, e.g. hot tier) so the caller falls back to the
     * legacy local-FS warming path.
     */
    private static long storePointerOrDefault(NativeStoreHandle handle) {
        if (handle == null) {
            return 0L;
        }
        try {
            return handle.getPointer();
        } catch (IllegalStateException closed) {
            // Handle closed between check and extraction — fall back to local.
            return 0L;
        }
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot.getId())) return;
        logger.trace("DataFusion PME: afterRefresh — building reader encrypted=[{}]", cryptoMetadata.isPresent());
        Collection<WriterFileSet> fileSets = catalogSnapshot.getSearchableFiles(dataFormat.name());
        Map<String, byte[]> fileFooterKeys = new HashMap<>();
        Map<String, byte[]> fileAadPrefixes = new HashMap<>();
        rehydrateEncryptionMaterial(fileSets, fileFooterKeys, fileAadPrefixes);
        logger.trace("DataFusion PME: rehydrated keys for {} files", fileFooterKeys.size());
        DatafusionReader reader = new DatafusionReader(
            directoryPath,
            fileSets,
            dataformatAwareStoreHandle,
            sortFields,
            sortOrders,
            fileFooterKeys,
            fileAadPrefixes
        );
        readers.put(catalogSnapshot.getId(), reader);
    }

    private void rehydrateEncryptionMaterial(
        Collection<WriterFileSet> fileSets,
        Map<String, byte[]> fileFooterKeys,
        Map<String, byte[]> fileAadPrefixes
    ) throws IOException {
        if (fileSets == null || fileSets.isEmpty()) {
            return;
        }

        logger.trace("DataFusion PME: rehydrateEncryptionMaterial — {} file sets", fileSets.size());

        // Resolve store pointer once for all files in this refresh.
        // 0 = default local filesystem; >0 = tiered/object-store via NativeStoreHandle.
        long storePtr = 0L;
        if (dataformatAwareStoreHandle != null) {
            try {
                storePtr = dataformatAwareStoreHandle.getPointer();
            } catch (IllegalStateException e) {
                // Handle closed between check and extraction — fall back to local.
                storePtr = 0L;
            }
        }

        byte[] dataKey = null;
        try {
            for (WriterFileSet fileSet : fileSets) {
                for (String file : fileSet.files()) {
                    logger.trace("DataFusion PME: reading footer key_metadata for file=[{}]", file);
                    byte[] keyMetadataBytes = NativeBridge.readFooterKeyMetadata(directoryPath, file, storePtr);
                    if (keyMetadataBytes == null) {
                        // Plain-text file or PARE without key_metadata — no decryption needed.
                        logger.trace("DataFusion PME: file=[{}] not encrypted", file);
                        continue;
                    }

                    // Encrypted file — require cryptoMetadata to be configured.
                    if (cryptoMetadata.isPresent() == false) {
                        throw new IOException(
                            "PME-encrypted file found but no crypto metadata configured for this index: " + file
                        );
                    }

                    // Load the data key on first encounter (shared across all files in this shard).
                    if (dataKey == null) {
                        dataKey = loadDataKey();
                    }

                    String keyMetadataJson = new String(keyMetadataBytes, StandardCharsets.UTF_8);
                    byte[] messageId = parseMessageIdFromKeyMetadataJson(file, keyMetadataJson);
                    byte[] footerKey = deriveFooterKey(dataKey, messageId);
                    logger.trace("DataFusion PME: footer key derived for file=[{}] keyLen=[{}]", file, footerKey.length);
                    fileFooterKeys.put(file, footerKey);
                    fileAadPrefixes.put(file, buildAadPrefix(messageId));
                    logger.trace("DataFusion PME: AAD prefix built for file=[{}]", file);
                }
            }
        } finally {
            if (dataKey != null) {
                Arrays.fill(dataKey, (byte) 0);
            }
        }

        logger.trace("DataFusion PME: rehydration complete — {} footer keys, {} AAD prefixes",
            fileFooterKeys.size(), fileAadPrefixes.size());
    }

    /**
     * Loads the raw 32-byte index data key from the keyfile using the configured
     * {@link MasterKeyProvider}. Mirrors {@code PmeKeyfileManager.loadKey()} — inlined here
     * to avoid a compile-scope dependency on {@code parquet-data-format}.
     */
    private byte[] loadDataKey() throws IOException {
        if (cryptoMetadata.isPresent() == false) {
            throw new IOException("PME-encrypted files present but no crypto metadata configured for this index");
        }
        if (Files.exists(keyfilePath) == false) {
            throw new IOException("PME keyfile not found at [" + keyfilePath + "]; cannot derive footer keys for encrypted files");
        }
        byte[] encryptedKey = Files.readAllBytes(keyfilePath);
        try (MasterKeyProvider keyProvider = createKeyProviderForRead()) {
            if (keyProvider == null) {
                throw new IOException("PME metadata present but index crypto settings are not configured");
            }
            byte[] rawKey = keyProvider.decryptKey(encryptedKey);
            if (rawKey == null || rawKey.length != 32) {
                throw new IOException("PME keyfile decrypted to unexpected length: " + (rawKey == null ? 0 : rawKey.length));
            }
            logger.trace("DataFusion PME: data key loaded from keyfile [{}]", keyfilePath);
            return rawKey;
        }
    }

    // TODO: The helpers below (deriveFooterKey, parseMessageIdFromKeyMetadataJson, buildAadPrefix)
    //  duplicate logic that lives canonically in PmeKeyDerivation and PmeFileKeyMetadata inside the
    //  parquet-data-format plugin. They are inlined here because parquet-data-format is only on
    //  the testImplementation classpath of this module — adding it as a compile/implementation
    //  dependency would create a plugin-to-plugin compile dependency that causes jar-hell at
    //  runtime (both plugins load in separate classloaders under the same OpenSearch node).
    //  The right long-term fix is to extract the shared PME crypto primitives into a dedicated
    //  library module (e.g. sandbox:libs:pme-crypto) that both plugins can depend on without
    //  classloader conflicts. Until then, keep these helpers in sync with the originals manually.

    /**
     * Derives the PME footer key from the 32-byte data key and 16-byte message_id.
     * Inlined from {@code PmeKeyDerivation.deriveFooterKey()} — see TODO above.
     *
     * <pre>
     * PRK          = HMAC-SHA384(key=dataKey,  data=messageId)
     * T1           = HMAC-SHA384(key=PRK,       data=context || 0x01)
     * footerKey    = first 16 bytes of T1
     * context      = "opensearch/parquet-pme/footer-key/v1"
     * </pre>
     */
    private static byte[] deriveFooterKey(byte[] dataKey, byte[] messageId) throws IOException {
        try {
            byte[] prk = hmacSha384(dataKey, messageId);
            String context = "opensearch/parquet-pme/footer-key/v1";
            byte[] contextBytes = context.getBytes(StandardCharsets.UTF_8);
            byte[] expandInput = new byte[contextBytes.length + 1];
            System.arraycopy(contextBytes, 0, expandInput, 0, contextBytes.length);
            expandInput[contextBytes.length] = 0x01;
            byte[] t1 = hmacSha384(prk, expandInput);
            Arrays.fill(prk, (byte) 0);
            // TODO: 16 bytes due to current parquet-rs limitation (mirrors PmeKeyDerivation.FOOTER_KEY_BYTES)
            // This might be solved with parquet-rs v59, see https://github.com/apache/arrow-rs/pull/9203
            return Arrays.copyOf(t1, 16);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException("PME footer key derivation failed", e);
        }
    }

    private static byte[] hmacSha384(byte[] key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance("HmacSHA384");
        mac.init(new SecretKeySpec(key, "HmacSHA384"));
        return mac.doFinal(data);
    }

    /**
     * Parses the {@code message_id} field from a v1 PME key-metadata JSON string.
     * Uses Jackson with {@code FAIL_ON_UNKNOWN_PROPERTIES=true} so any unrecognised
     * field causes a hard failure (fail-closed). Validates:
     * <ul>
     *   <li>{@code version} == 1</li>
     *   <li>{@code data_key_id} == {@code "default"}</li>
     *   <li>{@code message_id} decodes from base64url to exactly 16 bytes</li>
     * </ul>
     */
    private static byte[] parseMessageIdFromKeyMetadataJson(String file, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        PmeKeyMetadata meta;
        try {
            meta = mapper.readValue(json, PmeKeyMetadata.class);
        } catch (Exception e) {
            throw new IOException("PME key metadata JSON parse error for file: " + file, e);
        }
        if (meta.version != 1) {
            throw new IOException(
                "PME key metadata version must be 1, got " + meta.version + " for file: " + file
            );
        }
        if ("default".equals(meta.data_key_id) == false) {
            throw new IOException(
                "PME key metadata data_key_id must be 'default', got '" + meta.data_key_id
                    + "' for file: " + file
            );
        }
        if (meta.message_id == null || meta.message_id.isEmpty()) {
            throw new IOException("PME key metadata missing message_id for file: " + file);
        }
        byte[] messageId;
        try {
            messageId = Base64.getUrlDecoder().decode(meta.message_id);
        } catch (IllegalArgumentException e) {
            throw new IOException("PME key metadata invalid base64url message_id for file: " + file, e);
        }
        if (messageId.length != 16) {
            throw new IOException(
                "PME key metadata message_id must be 16 bytes, got " + messageId.length
                    + " for file: " + file
            );
        }
        return messageId;
    }

    /** Jackson DTO for PME key-metadata JSON. All fields are required; unknown fields are rejected. */
    private static class PmeKeyMetadata {
        public int version;
        public String data_key_id;
        public String message_id;
    }

    /**
     * Builds the v1 binary AAD prefix for the given 16-byte message_id.
     * Inlined from {@code PmeKeyDerivation.buildAadPrefix()} — see TODO above.
     *
     * <pre>
     * u16_be(domain.len) || domain || u8(1) || u16_be(dataKeyId.len) || dataKeyId || messageId[16]
     * </pre>
     */
    private static byte[] buildAadPrefix(byte[] messageId) {
        byte[] domain = "opensearch/parquet-pme/file/v1".getBytes(StandardCharsets.UTF_8);
        byte[] dataKeyId = "default".getBytes(StandardCharsets.UTF_8);
        byte[] aad = new byte[2 + domain.length + 1 + 2 + dataKeyId.length + 16];
        int pos = 0;
        aad[pos++] = (byte) (domain.length >>> 8);
        aad[pos++] = (byte) domain.length;
        System.arraycopy(domain, 0, aad, pos, domain.length);
        pos += domain.length;
        aad[pos++] = 0x01; // version
        aad[pos++] = (byte) (dataKeyId.length >>> 8);
        aad[pos++] = (byte) dataKeyId.length;
        System.arraycopy(dataKeyId, 0, aad, pos, dataKeyId.length);
        pos += dataKeyId.length;
        System.arraycopy(messageId, 0, aad, pos, 16);
        return aad;
    }

    private MasterKeyProvider createKeyProviderForRead() {
        if (cryptoMetadata.isPresent() == false) {
            return null;
        }

        CryptoMetadata metadata = cryptoMetadata.get();
        CryptoHandlerRegistry cryptoHandlerRegistry = CryptoHandlerRegistry.getInstance();
        if (cryptoHandlerRegistry == null) {
            throw new IllegalStateException("CryptoHandlerRegistry is not initialized");
        }

        // PME only needs the MasterKeyProvider to decrypt the keyfile — it does NOT require a
        // full CryptoPlugin (which handles data-at-rest encryption via CryptoHandler).
        // Calling fetchCryptoHandler() would try to create a CryptoHandler, which requires a
        // CryptoPlugin to be registered. The mock-pme-kms (and any PME-only plugin) only
        // implements CryptoKeyProviderPlugin, so we go directly to getCryptoKeyProviderPlugin.
        CryptoKeyProviderPlugin keyProviderPlugin = cryptoHandlerRegistry.getCryptoKeyProviderPlugin(metadata.keyProviderType());
        if (keyProviderPlugin == null) {
            throw new CryptoRegistryException(
                metadata.keyProviderName(),
                metadata.keyProviderType(),
                "Crypto key provider plugin not found for DataFusion PME read"
            );
        }
        return keyProviderPlugin.createKeyProvider(metadata);
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        for (DatafusionReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
