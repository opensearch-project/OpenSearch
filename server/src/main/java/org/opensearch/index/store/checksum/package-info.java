/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Checksum calculation handlers for format-aware file integrity verification.
 * Provides a registry-based approach to delegate checksum computation to format-specific
 * handlers (e.g., Lucene CodecUtil for Lucene files, CRC32 for other formats).
 */
package org.opensearch.index.store.checksum;
