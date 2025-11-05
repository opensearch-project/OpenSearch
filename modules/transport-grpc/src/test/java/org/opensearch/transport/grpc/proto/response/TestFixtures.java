/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response;

import org.opensearch.protobufs.GlobalParams;

public final class TestFixtures {

    public static final GlobalParams FULL_STACK_TRACE_REQUESTED = GlobalParams.newBuilder().setErrorTrace(true).build();

    public static final GlobalParams ERROR_SUMMARY_REQUESTED = GlobalParams.newBuilder().setErrorTrace(false).build();

}
