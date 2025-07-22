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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.xcontent;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentLocation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry for looking things up using ParseField semantics.
 *
 * @opensearch.internal
 */
public class ParseFieldRegistry<T> {
    private final Map<String, Tuple<ParseField, T>> registry = new HashMap<>();
    private final String registryName;

    /**
     * Build the registry.
     * @param registryName used for error messages
     */
    public ParseFieldRegistry(String registryName) {
        this.registryName = registryName;
    }

    /**
     * All the names under which values are registered. Expect this to be used mostly for testing.
     */
    public Set<String> getNames() {
        return registry.keySet();
    }

    /**
     * Register a parser.
     */
    public void register(T value, String name) {
        register(value, new ParseField(name));
    }

    /**
     * Register a parser.
     */
    public void register(T value, ParseField parseField) {
        Tuple<ParseField, T> parseFieldParserTuple = new Tuple<>(parseField, value);
        for (String name : parseField.getAllNamesIncludedDeprecated()) {
            Tuple<ParseField, T> previousValue = registry.putIfAbsent(name, parseFieldParserTuple);
            if (previousValue != null) {
                throw new IllegalArgumentException(
                    "["
                        + previousValue.v2()
                        + "] already registered for ["
                        + registryName
                        + "]["
                        + name
                        + "] while trying to register ["
                        + value
                        + "]"
                );
            }
        }
    }

    /**
     * Lookup a value from the registry by name while checking that the name matches the ParseField.
     *
     * @param name The name of the thing to look up.
     * @return The value being looked up. Never null.
     * @throws ParsingException if the named thing isn't in the registry or the name was deprecated and deprecated names aren't supported.
     */
    public T lookup(String name, XContentLocation xContentLocation, DeprecationHandler deprecationHandler) {
        T value = lookupReturningNullIfNotFound(name, deprecationHandler);
        if (value == null) {
            throw new ParsingException(xContentLocation, "no [" + registryName + "] registered for [" + name + "]");
        }
        return value;
    }

    /**
     * Lookup a value from the registry by name while checking that the name matches the ParseField.
     *
     * @param name The name of the thing to look up.
     * @return The value being looked up or null if it wasn't found.
     * @throws ParsingException if the named thing isn't in the registry or the name was deprecated and deprecated names aren't supported.
     */
    public T lookupReturningNullIfNotFound(String name, DeprecationHandler deprecationHandler) {
        Tuple<ParseField, T> parseFieldAndValue = registry.get(name);
        if (parseFieldAndValue == null) {
            return null;
        }
        ParseField parseField = parseFieldAndValue.v1();
        T value = parseFieldAndValue.v2();
        boolean match = parseField.match(name, deprecationHandler);
        // this is always expected to match, ParseField is useful for deprecation warnings etc. here
        assert match : "ParseField did not match registered name [" + name + "][" + registryName + "]";
        return value;
    }
}
