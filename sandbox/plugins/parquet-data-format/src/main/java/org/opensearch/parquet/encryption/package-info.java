/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * PME (Parquet Modular Encryption) key management for the parquet-data-format plugin.
 *
 * <p>Implements v1 key management following the spec in {@code pme-key-management.md}:
 * <ul>
 *   <li>Index-level {@code keyfile} stores the provider-wrapped data key (same convention as
 *       Lucene storage encryption).</li>
 *   <li>Node-level {@link org.opensearch.parquet.encryption.PmeDataKeyCache} caches decrypted
 *       data keys, keyed by index UUID.</li>
 *   <li>Per-file key derivation uses two-step HMAC-SHA384 with context
 *       {@code "opensearch/parquet-pme/footer-key/v1"}.</li>
 *   <li>Per-file key metadata ({@code version}, {@code data_key_id}, {@code message_id}) is
 *       stored as compact UTF-8 JSON in {@code FileCryptoMetaData.key_metadata}.</li>
 *   <li>Binary AAD prefix encodes domain, version, data key id, and message_id.</li>
 * </ul>
 *
 * <p>Entry point for callers: {@link org.opensearch.parquet.encryption.PmeContext}.
 */
package org.opensearch.parquet.encryption;

