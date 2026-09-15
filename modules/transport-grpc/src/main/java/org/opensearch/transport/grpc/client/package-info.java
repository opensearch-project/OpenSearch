/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Client wrappers for gRPC transport functionality.
 *
 * This package contains {@link org.opensearch.transport.client.Client} implementations that adapt
 * OpenSearch actions to gRPC call semantics, such as cancelling the underlying task when a gRPC call
 * is abandoned by the client.
 *
 * @opensearch.internal
 */
package org.opensearch.transport.grpc.client;
