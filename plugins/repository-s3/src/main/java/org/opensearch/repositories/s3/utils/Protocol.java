/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

/**
 * Represents the communication protocol to use when sending requests to AWS.
 * <p>
 * Communication over HTTPS is the default, and is more secure than HTTP, which
 * is why AWS recommends using HTTPS. HTTPS connections can use more system
 * resources because of the extra work to encrypt network traffic, so the option
 * to use HTTP is available in case users need it.
 */
public enum Protocol {

    /**
     * HTTP Protocol - Using the HTTP protocol is less secure than HTTPS, but
     * can slightly reduce the system resources used when communicating with
     * AWS.
     */
    HTTP("http"),

    /**
     * HTTPS Protocol - Using the HTTPS protocol is more secure than using the
     * HTTP protocol, but may use slightly more system resources. AWS recommends
     * using HTTPS for maximize security.
     */
    HTTPS("https");

    private final String protocol;

    private Protocol(String protocol) {
        this.protocol = protocol;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return protocol;
    }
}
