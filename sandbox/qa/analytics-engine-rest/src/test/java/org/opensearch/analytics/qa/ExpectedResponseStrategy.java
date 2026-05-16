/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Strategy for handling missing expected response files.
 */
public enum ExpectedResponseStrategy {
    /**
     * Default behavior: Execute query and verify 200 response with non-empty result.
     * Does NOT fail if expected response file is missing.
     */
    PASS_ON_MISSING,

    /**
     * Fail the test if expected response file is missing.
     * Use this to enforce that all queries have expected responses.
     */
    FAIL_ON_MISSING,

    /**
     * Skip validation entirely - only check for 200 response.
     * Equivalent to validateExpected=false in the old API.
     */
    SKIP_VALIDATION
}
