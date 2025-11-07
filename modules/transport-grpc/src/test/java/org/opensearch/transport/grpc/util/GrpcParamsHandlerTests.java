/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import static org.opensearch.transport.grpc.TestFixtures.ERROR_SUMMARY_REQUESTED;
import static org.opensearch.transport.grpc.TestFixtures.FULL_STACK_TRACE_REQUESTED;
import static org.opensearch.transport.grpc.TestFixtures.settingsWithGivenStackTraceConfig;

public class GrpcParamsHandlerTests extends OpenSearchTestCase {

    @After
    public void resetStackTraceSettings() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(true));
    }

    public void testValidationFailsWhenDetailedErrorsDisabledAndClientRequestedStackTrace() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(false));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GrpcParamsHandler.validateStackTraceDetailsConfiguration(FULL_STACK_TRACE_REQUESTED)
        );
        assertEquals("error traces in responses are disabled.", exception.getMessage());
    }

    public void testValidationPassesWhenDetailedErrorsDisabledAndClientDoesNotRequestStackTrace() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(false));

        try {
            GrpcParamsHandler.validateStackTraceDetailsConfiguration(ERROR_SUMMARY_REQUESTED);
        } catch (Exception e) {
            fail("Validation should pass without exceptions when stack traces are not requested.");
        }
    }

    public void testGrpcParamsHandlerPicksErrorTraceRequestParameter() {
        assertTrue(
            "Params handler must directly pick error_trace=true",
            GrpcParamsHandler.isDetailedStackTraceRequested(FULL_STACK_TRACE_REQUESTED)
        );
        assertFalse(
            "Params handler must directly pick error_trace=false",
            GrpcParamsHandler.isDetailedStackTraceRequested(ERROR_SUMMARY_REQUESTED)
        );
    }

}
