/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.core;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentLocation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Holds a field that can be found in a request while parsing and its different
 * variants, which may be deprecated.
 *
 * @opensearch.api
 *
 */
@PublicApi(since = "1.0.0")
public class ParseField {
    private final String name;
    private final String[] deprecatedNames;
    private String allReplacedWith = null;
    private final String[] allNames;
    private boolean fullyDeprecated = false;

    private static final String[] EMPTY = new String[0];

    /**
     * @param name
     *            the primary name for this field. This will be returned by
     *            {@link #getPreferredName()}
     * @param deprecatedNames
     *            names for this field which are deprecated and will not be
     *            accepted when strict matching is used.
     */
    public ParseField(String name, String... deprecatedNames) {
        this.name = name;
        if (deprecatedNames == null || deprecatedNames.length == 0) {
            this.deprecatedNames = EMPTY;
        } else {
            final HashSet<String> set = new HashSet<>();
            Collections.addAll(set, deprecatedNames);
            this.deprecatedNames = set.toArray(new String[0]);
        }
        Set<String> allNames = new HashSet<>();
        allNames.add(name);
        Collections.addAll(allNames, this.deprecatedNames);
        this.allNames = allNames.toArray(new String[0]);
    }

    /**
     * @return the preferred name used for this field
     */
    public String getPreferredName() {
        return name;
    }

    /**
     * @return All names for this field regardless of whether they are
     *         deprecated
     */
    public String[] getAllNamesIncludedDeprecated() {
        return allNames;
    }

    /**
     * @param deprecatedNames
     *            deprecated names to include with the returned
     *            {@link ParseField}
     * @return a new {@link ParseField} using the preferred name from this one
     *         but with the specified deprecated names
     */
    public ParseField withDeprecation(String... deprecatedNames) {
        return new ParseField(this.name, deprecatedNames);
    }

    /**
     * Return a new ParseField where all field names are deprecated and replaced
     * with {@code allReplacedWith}.
     */
    public ParseField withAllDeprecated(String allReplacedWith) {
        ParseField parseField = this.withDeprecation(getAllNamesIncludedDeprecated());
        parseField.allReplacedWith = allReplacedWith;
        return parseField;
    }

    /**
     * Return a new ParseField where all field names are deprecated with no replacement
     */
    public ParseField withAllDeprecated() {
        ParseField parseField = this.withDeprecation(getAllNamesIncludedDeprecated());
        parseField.fullyDeprecated = true;
        return parseField;
    }

    /**
     * Does {@code fieldName} match this field?
     * @param fieldName
     *            the field name to match against this {@link ParseField}
     * @param deprecationHandler called if {@code fieldName} is deprecated
     * @return true if <code>fieldName</code> matches any of the acceptable
     *         names for this {@link ParseField}.
     */
    public boolean match(String fieldName, DeprecationHandler deprecationHandler) {
        return match(null, () -> XContentLocation.UNKNOWN, fieldName, deprecationHandler);
    }

    /**
     * Does {@code fieldName} match this field?
     * @param parserName
     *            the name of the parent object holding this field
     * @param location
     *            the XContentLocation of the field
     * @param fieldName
     *            the field name to match against this {@link ParseField}
     * @param deprecationHandler called if {@code fieldName} is deprecated
     * @return true if <code>fieldName</code> matches any of the acceptable
     *         names for this {@link ParseField}.
     */
    public boolean match(String parserName, Supplier<XContentLocation> location, String fieldName, DeprecationHandler deprecationHandler) {
        Objects.requireNonNull(fieldName, "fieldName cannot be null");
        // if this parse field has not been completely deprecated then try to
        // match the preferred name
        if (fullyDeprecated == false && allReplacedWith == null && fieldName.equals(name)) {
            return true;
        }
        // Now try to match against one of the deprecated names. Note that if
        // the parse field is entirely deprecated (allReplacedWith != null) all
        // fields will be in the deprecatedNames array
        for (String depName : deprecatedNames) {
            if (fieldName.equals(depName)) {
                if (fullyDeprecated) {
                    deprecationHandler.usedDeprecatedField(parserName, location, fieldName);
                } else if (allReplacedWith == null) {
                    deprecationHandler.usedDeprecatedName(parserName, location, fieldName, name);
                } else {
                    deprecationHandler.usedDeprecatedField(parserName, location, fieldName, allReplacedWith);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getPreferredName();
    }

    /**
     * @return the message to use if this {@link ParseField} has been entirely
     *         deprecated in favor of something else. This method will return
     *         <code>null</code> if the ParseField has not been completely
     *         deprecated.
     */
    public String getAllReplacedWith() {
        return allReplacedWith;
    }

    /**
     * @return an array of the names for the {@link ParseField} which are
     *         deprecated.
     */
    public String[] getDeprecatedNames() {
        return deprecatedNames;
    }

    /**
     * Common fields shared throughout the project
     *
     * @opensearch.internal
     **/
    public static class CommonFields {
        public static final ParseField FIELD = new ParseField("field");
        public static final ParseField FIELDS = new ParseField("fields");
        public static final ParseField FORMAT = new ParseField("format");
        public static final ParseField MISSING = new ParseField("missing");
        public static final ParseField TIME_ZONE = new ParseField("time_zone");
    }
}
