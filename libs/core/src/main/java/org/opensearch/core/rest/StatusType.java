/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.rest;

import java.util.Locale;

/**
 * Enum for status type which keeps track of potential response families in {@link RestStatus}
 *
 * @opensearch.api
 */
public enum StatusType {
    SUCCESS,
    USER_ERROR,
    SYSTEM_FAILURE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
