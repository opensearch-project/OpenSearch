/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Transport integration contracts for native Arrow request/response flows. A concrete
 * Arrow-aware transport (e.g., Arrow Flight over gRPC) implements these to carry
 * {@link ArrowBatchResponse} batches without serialization.
 */
package org.opensearch.arrow.transport;
