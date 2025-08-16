/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import io.grpc.ServerInterceptor;

public interface OrderedGrpcInterceptor {
    int getOrder(); // Lower values = higher priority
    ServerInterceptor getInterceptor();
}
