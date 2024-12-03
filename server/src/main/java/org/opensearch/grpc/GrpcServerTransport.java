/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.service.ReportingService;

/**
 * gRPC Transport server
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface GrpcServerTransport extends LifecycleComponent, ReportingService<GrpcInfo> {
    BoundTransportAddress boundAddress();
    GrpcInfo info();
    GrpcStats stats();
}
