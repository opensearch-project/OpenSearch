/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

/**
 * Utility class for error handling in DataFusion operations.
 */
public class ErrorUtil {
    private ErrorUtil() {}

    static boolean containsError(String errString) {
        return errString != null && !errString.isEmpty();
    }
}
