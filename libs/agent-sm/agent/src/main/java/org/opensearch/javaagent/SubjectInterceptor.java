/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import javax.security.auth.Subject;

import java.security.AccessControlContext;

/**
 * {@link Subject#getSubject} interceptor
 */
public class SubjectInterceptor {
    /**
     * SubjectInterceptor
     */
    public SubjectInterceptor() {}

    /**
     * Replace Subject::getSubject
     * @param acc context
     * @return current subject
     */
    @SuppressWarnings("removal")
    public static Subject intercept(AccessControlContext acc) {
        return Subject.current();
    }
}
