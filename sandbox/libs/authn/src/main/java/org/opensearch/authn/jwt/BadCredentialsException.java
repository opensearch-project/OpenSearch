/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.jwt;

public class BadCredentialsException extends Exception {

    private static final long serialVersionUID = 9092575587366580869L;

    public BadCredentialsException() {
        super();
    }

    public BadCredentialsException(String message, Throwable cause, boolean enableSuppression,
                                   boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public BadCredentialsException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadCredentialsException(String message) {
        super(message);
    }

    public BadCredentialsException(Throwable cause) {
        super(cause);
    }
}
