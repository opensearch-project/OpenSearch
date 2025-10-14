/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

public class ResponseHandlingParams {

    private final TracingLevel errorTracingLevel;

    public ResponseHandlingParams(boolean detailedErrorsEnabled, boolean errorTracesRequested) {
        this.errorTracingLevel = getTracingLevel(detailedErrorsEnabled, errorTracesRequested);
    }

    public TracingLevel getErrorTracingLevel() {
        return errorTracingLevel;
    }

    private static TracingLevel getTracingLevel(boolean detailedErrorsEnabled, boolean errorTracesRequested) {
        if (detailedErrorsEnabled == false && errorTracesRequested) {
            return TracingLevel.TRACING_DISABLED;
        }
        return errorTracesRequested ? TracingLevel.DETAILED_TRACE : TracingLevel.SUMMARY;
    }

    public enum TracingLevel {
        SUMMARY,
        DETAILED_TRACE,
        TRACING_DISABLED
    }

}
