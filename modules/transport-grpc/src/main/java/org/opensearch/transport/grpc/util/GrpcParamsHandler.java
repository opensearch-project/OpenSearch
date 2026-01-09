/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_DETAILED_ERRORS_ENABLED;

/**
 * Central utility class to handle how global gRPC request parameters are handled.
 * Handling of the server side {@code detailedErrorsEnabled} and the fail-fast behaviour
 * in case of faulty configuration is aligned with the
 * {@link RestController#tryAllHandlers(RestRequest, RestChannel, ThreadContext)} and the
 * {@link AbstractRestChannel}.
 */
public class GrpcParamsHandler {

    /**
     * Indicates whether detailed error traces are enabled on the gRPC server.
     */
    private static final AtomicBoolean detailedErrorsEnabled = new AtomicBoolean(true);

    private GrpcParamsHandler() {}

    /**
     * Initializes the handler with the given settings.
     *
     * @param settings the node settings
     */
    public static void initialize(Settings settings) {
        detailedErrorsEnabled.set(SETTING_GRPC_DETAILED_ERRORS_ENABLED.get(settings));
    }

    /**
     * Checks whether detailed stack trace was requested in the gRPC request parameters.
     * This method can be further extended to encapsulate more complex behaviour like
     * skipping stack traces as per server configuration, etc.
     *
     * @param globalParams the global gRPC request parameters
     * @return if, in case of an exception, a detailed stack trace should be included in the response
     */
    public static boolean isDetailedStackTraceRequested(GlobalParams globalParams) {
        return globalParams.getErrorTrace();
    }

    /**
     * Checks if detailed errors are enabled on the gRPC server.
     *
     * @return true if detailed errors are enabled, false otherwise
     */
    public static boolean isDetailedErrorsEnabled() {
        return detailedErrorsEnabled.get();
    }

    /**
     * Validates if error details are allowed to be shared in the response
     * based on the grpc server configuration and request parameters.
     *
     * @param globalRequestParams The global parameters from the gRPC request
     * @throws IllegalArgumentException if error tracing is requested but disabled by the server side
     */
    public static void validateStackTraceDetailsConfiguration(GlobalParams globalRequestParams) {
        if (detailedErrorsEnabled.get() == false && globalRequestParams.getErrorTrace()) {
            throw new IllegalArgumentException("error traces in responses are disabled.");
        }
    }

}
