/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.exception;

public class ExceptionUtils {

    public static Throwable getRootCause(final Throwable e) {

        if(e == null) {
            return null;
        }

        final Throwable cause = e.getCause();
        if(cause == null) {
            return e;
        }
        return getRootCause(cause);
    }

    public static Throwable findMsg(final Throwable e, String msg) {

        if(e == null) {
            return null;
        }

        if(e.getMessage() != null && e.getMessage().contains(msg)) {
            return e;
        }

        final Throwable cause = e.getCause();
        if(cause == null) {
            return null;
        }
        return findMsg(cause, msg);
    }
}

