/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.core.common.Strings;
import org.opensearch.test.OpenSearchTestCase;

public class RequestUtilsTests extends OpenSearchTestCase {

    public void testGenerateID() {
        assertTrue(Strings.hasText(RequestUtils.generateID()));
    }

    public void testValidateRequestIdValid() {
        RequestUtils.validateRequestId("a1b2c3d4e5f67890abcdef1234567890");
        RequestUtils.validateRequestId("ABCDEF1234567890abcdef1234567890");
        RequestUtils.validateRequestId("00000000000000000000000000000000");
        RequestUtils.validateRequestId("ffffffffffffffffffffffffffffffff");
        RequestUtils.validateRequestId("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
    }

    public void testValidateRequestIdNull() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestUtils.validateRequestId(null));
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdEmpty() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestUtils.validateRequestId(""));
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdBlank() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestUtils.validateRequestId("   "));
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdTooShort() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("a1b2c3d4e5f67890")
        );
        assertEquals("Invalid X-Request-Id passed. Should be 32 hexadecimal characters: a1b2c3d4e5f67890", exception.getMessage());
    }

    public void testValidateRequestIdTooLong() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("a1b2c3d4e5f67890abcdef1234567890extra")
        );
        assertEquals(
            "Invalid X-Request-Id passed. Should be 32 hexadecimal characters: a1b2c3d4e5f67890abcdef1234567890extra",
            exception.getMessage()
        );
    }

    public void testValidateRequestIdInvalidCharacters() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("g1b2c3d4e5f67890abcdef1234567890")
        );
        assertEquals("Invalid X-Request-Id passed: g1b2c3d4e5f67890abcdef1234567890", exception.getMessage());
    }

    public void testValidateRequestIdWithSpecialCharacters() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        );
        assertEquals(
            "Invalid X-Request-Id passed. Should be 32 hexadecimal characters: a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            exception.getMessage()
        );
    }
}
