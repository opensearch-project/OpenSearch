/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.settings.Settings;
import org.opensearch.protobufs.GlobalParams;

public final class TestFixtures {

    public static final GlobalParams FULL_STACK_TRACE_REQUESTED = GlobalParams.newBuilder().setErrorTrace(true).build();

    public static final GlobalParams ERROR_SUMMARY_REQUESTED = GlobalParams.newBuilder().setErrorTrace(false).build();

    public static Settings settingsWithGivenStackTraceConfig(boolean stackTracesEnabled) {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_DETAILED_ERRORS_ENABLED.getKey(), stackTracesEnabled).build();
    }

}
