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
 * Modifications Copyright OpenSearch Contributors.
 * See GitHub history for details.
 */

package org.opensearch.gradle;

import org.gradle.process.CommandLineArgumentProvider;
import java.util.Collections;
import java.util.List;

/**
 * Provides a fixed list of command-line arguments to Gradle tasks.
 * This implementation does not track these arguments as inputs,
 * which is useful for arguments that should not affect Gradle input snapshotting.
 */
public class SimpleCommandLineArgumentProvider implements CommandLineArgumentProvider {

    private final List<String> arguments;

    /**
     * Constructs a new SimpleCommandLineArgumentProvider.
     *
     * @param arguments the command-line arguments to provide; may be null or empty.
     */
    public SimpleCommandLineArgumentProvider(String... arguments) {
        if (arguments == null) {
            this.arguments = Collections.emptyList();
        } else {
            this.arguments = List.of(arguments); // Immutable list (Java 9+)
        }
    }

    @Override
    public Iterable<String> asArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "SimpleCommandLineArgumentProvider{" +
                "arguments=" + arguments +
                '}';
    }
}
