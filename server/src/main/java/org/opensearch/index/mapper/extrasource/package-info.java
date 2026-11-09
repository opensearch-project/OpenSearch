/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Support for indexing extra field values outside of {@code _source}.
 *
 * <p>This package provides abstractions to pass pre-processed field values directly
 * to the indexing layer, enabling more efficient ingestion by avoiding {@code _source}
 * parsing for supported data types.</p>
 */
package org.opensearch.index.mapper.extrasource;
