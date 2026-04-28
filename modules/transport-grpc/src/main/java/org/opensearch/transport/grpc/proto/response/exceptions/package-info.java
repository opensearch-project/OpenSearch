/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * This package contains utility classes for converting various OpenSearch exceptions to Protocol Buffer representations.
 * Each utility class is specialized for a specific exception type and handles the conversion of that exception's
 * metadata to Protocol Buffers, preserving the relevant information about the exception.
 * <p>
 * These utilities are used by the gRPC transport plugin to convert OpenSearch exceptions to a format that can be
 * transmitted over gRPC and properly interpreted by clients.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;
