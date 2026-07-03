/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Dummy KMS plugin for Parquet Modular Encryption (PME) development and integration testing.
 *
 * <p>Provides {@link org.opensearch.crypto.mock.MockPmeKmsPlugin}, which registers the
 * {@code "mock-pme"} key provider type. The provider uses random 32-byte keys with
 * identity decryption — no external KMS service is required.
 *
 * <p><strong>NOT for production use.</strong>
 */
package org.opensearch.crypto.mock;

