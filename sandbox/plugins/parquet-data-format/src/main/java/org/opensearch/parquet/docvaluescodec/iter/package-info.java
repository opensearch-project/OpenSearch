/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene DocValues iterators backed by a Parquet column. Each reads from the column reader's resident
 * decoded batch on the hot path and crosses the native boundary only when a requested document falls
 * outside that batch. Numeric only for now.
 */
package org.opensearch.parquet.docvaluescodec.iter;
