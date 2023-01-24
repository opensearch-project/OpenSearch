/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

public class Utils {
    @SuppressWarnings("unchecked")
    public static <T> T coalesce(T first, T... more) {
        if (first != null) {
            return first;
        }

        if (more == null || more.length == 0) {
            return null;
        }

        for (int i = 0; i < more.length; i++) {
            T t = more[i];
            if (t != null) {
                return t;
            }
        }

        return null;
    }
}
