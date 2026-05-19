/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.test.OpenSearchTestCase;

public class RequestUtilsTests extends OpenSearchTestCase {

    private static final int DEFAULT_MAX_LENGTH = HttpTransportSettings.SETTING_HTTP_REQUEST_ID_MAX_LENGTH.getDefault(Settings.EMPTY);

    public void testGenerateID() {
        assertTrue(Strings.hasText(RequestUtils.generateID()));
    }

    public void testValidateRequestIdValid() {
        RequestUtils.validateRequestId("a1b2c3d4e5f67890abcdef1234567890", DEFAULT_MAX_LENGTH);
        RequestUtils.validateRequestId("ABCDEF1234567890abcdef1234567890", DEFAULT_MAX_LENGTH);
        RequestUtils.validateRequestId("00000000000000000000000000000000", DEFAULT_MAX_LENGTH);
        RequestUtils.validateRequestId("ffffffffffffffffffffffffffffffff", DEFAULT_MAX_LENGTH);
        RequestUtils.validateRequestId("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", DEFAULT_MAX_LENGTH);
    }

    public void testValidateRequestIdNull() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId(null, DEFAULT_MAX_LENGTH)
        );
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdEmpty() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("", DEFAULT_MAX_LENGTH)
        );
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdBlank() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("   ", DEFAULT_MAX_LENGTH)
        );
        assertEquals("X-Request-Id should not be null or empty", exception.getMessage());
    }

    public void testValidateRequestIdTooLong() {
        String tooLong = "a".repeat(DEFAULT_MAX_LENGTH + 1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId(tooLong, DEFAULT_MAX_LENGTH)
        );
        assertEquals(
            "X-Request-Id length [" + (DEFAULT_MAX_LENGTH + 1) + "] exceeds maximum allowed length [" + DEFAULT_MAX_LENGTH + "]",
            exception.getMessage()
        );
    }

    public void testValidateRequestIdNonHexCharactersAllowed() {
        // Previously rejected, now allowed
        RequestUtils.validateRequestId("g1b2c3d4e5f67890abcdef1234567890", DEFAULT_MAX_LENGTH);
    }

    public void testValidateRequestIdWithSpecialCharactersAllowed() {
        // UUID with dashes - previously rejected, now allowed
        RequestUtils.validateRequestId("a1b2c3d4-e5f6-7890-abcd-ef1234567890", DEFAULT_MAX_LENGTH);
    }

    public void testValidateRequestIdExactlyAtMaxLength() {
        RequestUtils.validateRequestId("a".repeat(DEFAULT_MAX_LENGTH), DEFAULT_MAX_LENGTH);
    }

    public void testValidateRequestIdCustomMaxLength() {
        RequestUtils.validateRequestId("a".repeat(256), 256);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RequestUtils.validateRequestId("a".repeat(33), 32)
        );
        assertEquals("X-Request-Id length [33] exceeds maximum allowed length [32]", exception.getMessage());
    }
}
