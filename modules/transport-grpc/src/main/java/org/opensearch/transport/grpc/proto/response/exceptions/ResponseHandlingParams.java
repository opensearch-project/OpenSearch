/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.protobufs.GlobalParams;

public class ResponseHandlingParams {

    private final TracingLevel errorTracingLevel;

    public ResponseHandlingParams(boolean detailedErrorsEnabled, GlobalParams errorTracesRequested) {
        this.errorTracingLevel = getTracingLevel(detailedErrorsEnabled, errorTracesRequested);
    }

    public TracingLevel getErrorTracingLevel() {
        return errorTracingLevel;
    }

    private static TracingLevel getTracingLevel(boolean detailedErrorsEnabled, GlobalParams globalParams) {
        validateErrorTracingConfiguration(detailedErrorsEnabled, globalParams);
        return globalParams.getErrorTrace() ? TracingLevel.DETAILED_TRACE : TracingLevel.SUMMARY;
    }

    public enum TracingLevel {
        SUMMARY,
        DETAILED_TRACE
    }

    /**
     * Validates if error tracing is allowed based on server configuration and request parameters.
     *
     * @param detailedErrorsEnabled Whether detailed errors are enabled on the server
     * @param globalRequestParams The global parameters from the gRPC request
     * @throws IllegalArgumentException if error tracing is requested but disabled by the server side
     */
    public static void validateErrorTracingConfiguration(boolean detailedErrorsEnabled, GlobalParams globalRequestParams) {
        if (detailedErrorsEnabled == false && globalRequestParams.getErrorTrace()) {
            throw new IllegalArgumentException("error traces in responses are disabled.");
        }
    }

}
