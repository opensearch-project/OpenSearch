/*
 * Copyright (c) 2003, 2024, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;

/**
 * Adapted from: https://github.com/openjdk/jdk23u/blob/master/src/java.base/share/classes/sun/security/util/PropertyExpander.java
 */
public class PropertyExpander {

    public static class ExpandException extends GeneralSecurityException {
        private static final long serialVersionUID = -1L;

        public ExpandException(String msg) {
            super(msg);
        }
    }

    public static String expand(String value) throws ExpandException {
        return expand(value, false);
    }

    public static String expand(String value, boolean encodeURL) throws ExpandException {
        if (value == null) return null;

        int p = value.indexOf("${");

        // no special characters
        if (p == -1) return value;

        StringBuilder sb = new StringBuilder(value.length());
        int max = value.length();
        int i = 0;  // index of last character we copied

        scanner: while (p < max) {
            if (p > i) {
                // copy in anything before the special stuff
                sb.append(value.substring(i, p));
            }
            int pe = p + 2;

            // do not expand ${{ ... }}
            if (pe < max && value.charAt(pe) == '{') {
                pe = value.indexOf("}}", pe);
                if (pe == -1 || pe + 2 == max) {
                    // append remaining chars
                    sb.append(value.substring(p));
                    break scanner;
                } else {
                    // append as normal text
                    pe++;
                    sb.append(value.substring(p, pe + 1));
                }
            } else {
                while ((pe < max) && (value.charAt(pe) != '}')) {
                    pe++;
                }
                if (pe == max) {
                    // no matching '}' found, just add in as normal text
                    sb.append(value.substring(p, pe));
                    break scanner;
                }
                String prop = value.substring(p + 2, pe);
                if (prop.equals("/")) {
                    sb.append(java.io.File.separatorChar);
                } else {
                    String val = System.getProperty(prop);
                    if (val != null) {
                        if (encodeURL) {
                            // encode 'val' unless it's an absolute URI
                            // at the beginning of the string buffer
                            try {
                                if (sb.length() > 0 || !(new URI(val)).isAbsolute()) {
                                    val = ParseUtil.encodePath(val);
                                }
                            } catch (URISyntaxException use) {
                                val = ParseUtil.encodePath(val);
                            }
                        }
                        sb.append(val);
                    } else {
                        throw new ExpandException("unable to expand property " + prop);
                    }
                }
            }
            i = pe + 1;
            p = value.indexOf("${", i);
            if (p == -1) {
                // no more to expand. copy in any extra
                if (i < max) {
                    sb.append(value.substring(i, max));
                }
                // break out of loop
                break scanner;
            }
        }
        return sb.toString();
    }
}
