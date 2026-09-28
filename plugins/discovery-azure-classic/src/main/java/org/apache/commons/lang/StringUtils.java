/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.commons.lang;

import java.util.Collection;
import java.util.Iterator;

/**
 * Minimal shim to satisfy Azure Classic which expects commons-lang 2.x.
 * Delegates to commons-lang3.
 */
public final class StringUtils {
    private StringUtils() {}

    // === Overloads Azure Classic expects (commons-lang 2.x API) ===
    public static String join(final Collection<?> collection, final String separator) {
        if (collection == null) return null;
        return org.apache.commons.lang3.StringUtils.join(collection, separator);
    }

    public static String join(final Iterator<?> iterator, final String separator) {
        if (iterator == null) return null;
        return org.apache.commons.lang3.StringUtils.join(iterator, separator);
    }

    public static String join(final Object[] array, final String separator) {
        if (array == null) return null;
        return org.apache.commons.lang3.StringUtils.join(array, separator);
    }

    public static String join(final Iterable<?> iterable, final String separator) {
        if (iterable == null) return null;
        return org.apache.commons.lang3.StringUtils.join(iterable, separator);
    }

    public static boolean isBlank(final String s) {
        return org.apache.commons.lang3.StringUtils.isBlank(s);
    }

    public static boolean isEmpty(final String s) {
        return org.apache.commons.lang3.StringUtils.isEmpty(s);
    }
}
