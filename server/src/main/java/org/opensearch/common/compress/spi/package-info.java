/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Service Provider Interface for registering the{@link org.opensearch.common.compress.DeflateCompressor} with the
 * {@link org.opensearch.core.compress.CompressorRegistry}.
 *
 * Note: this will be refactored to the {@code :libs:opensearch-compress} library after other dependency classes are
 * refactored.
 */
package org.opensearch.common.compress.spi;
