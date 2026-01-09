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

import static org.opensearch.transport.grpc.TestFixtures.GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE;
import static org.opensearch.transport.grpc.TestFixtures.GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE;
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
            () -> GrpcParamsHandler.validateStackTraceDetailsConfiguration(GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE)
        );
        assertEquals("error traces in responses are disabled.", exception.getMessage());
    }

    public void testValidationPassesWhenDetailedErrorsDisabledAndClientDoesNotRequestStackTrace() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(false));

        try {
            GrpcParamsHandler.validateStackTraceDetailsConfiguration(GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);
        } catch (Exception e) {
            fail("Validation should pass without exceptions when stack traces are not requested.");
        }
    }

    public void testGrpcParamsHandlerPicksErrorTraceRequestParameter() {
        assertTrue(
            "Params handler must directly pick error_trace=true",
            GrpcParamsHandler.isDetailedStackTraceRequested(GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE)
        );
        assertFalse(
            "Params handler must directly pick error_trace=false",
            GrpcParamsHandler.isDetailedStackTraceRequested(GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE)
        );
    }

}
