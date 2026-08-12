/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.test.OpenSearchTestCase;

public class ValidationResultTests extends OpenSearchTestCase {

    public void testAcceptedFactoryProducesAcceptedResult() {
        ValidationResult result = ValidationResult.accepted();

        assertTrue(result.isAccepted());
        assertNull(result.reasonCode());
        assertNull(result.message());
    }

    public void testRejectedFactoryCarriesReasonCodeAndMessage() {
        ValidationResult result = ValidationResult.rejected("terms.boost", "Terms query does not support non-default boost");

        assertFalse(result.isAccepted());
        assertEquals("terms.boost", result.reasonCode());
        assertEquals("Terms query does not support non-default boost", result.message());
    }
}
