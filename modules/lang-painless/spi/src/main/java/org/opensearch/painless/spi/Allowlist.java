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

package org.opensearch.painless.spi;

import org.opensearch.painless.spi.annotation.AllowlistAnnotationParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Allowlist contains data structures designed to be used to generate an allowlist of Java classes,
 * constructors, methods, and fields that can be used within a Painless script at both compile-time
 * and run-time.
 * <p>
 * A Allowlist consists of several pieces with {@link AllowlistClass}s as the top level. Each
 * {@link AllowlistClass} will contain zero-to-many {@link AllowlistConstructor}s, {@link AllowlistMethod}s, and
 * {@link AllowlistField}s which are what will be available with a Painless script.  See each individual
 * allowlist object for more detail.
 */
public final class Allowlist {

    private static final String[] BASE_ALLOWLIST_FILES = new String[] {
        "org.opensearch.txt",
        "java.lang.txt",
        "java.math.txt",
        "java.text.txt",
        "java.time.txt",
        "java.time.chrono.txt",
        "java.time.format.txt",
        "java.time.temporal.txt",
        "java.time.zone.txt",
        "java.util.txt",
        "java.util.function.txt",
        "java.util.regex.txt",
        "java.util.stream.txt" };

    public static final List<Allowlist> BASE_ALLOWLISTS = Collections.singletonList(
        AllowlistLoader.loadFromResourceFiles(Allowlist.class, AllowlistAnnotationParser.BASE_ANNOTATION_PARSERS, BASE_ALLOWLIST_FILES)
    );

    /** The {@link ClassLoader} used to look up the allowlisted Java classes, constructors, methods, and fields. */
    public final ClassLoader classLoader;

    /** The {@link List} of all the allowlisted Painless classes. */
    public final List<AllowlistClass> allowlistClasses;

    /** The {@link List} of all the allowlisted static Painless methods. */
    public final List<AllowlistMethod> allowlistImportedMethods;

    /** The {@link List} of all the allowlisted Painless class bindings. */
    public final List<AllowlistClassBinding> allowlistClassBindings;

    /** The {@link List} of all the allowlisted Painless instance bindings. */
    public final List<AllowlistInstanceBinding> allowlistInstanceBindings;

    /** Standard constructor. All values must be not {@code null}. */
    public Allowlist(
        ClassLoader classLoader,
        List<AllowlistClass> allowlistClasses,
        List<AllowlistMethod> allowlistImportedMethods,
        List<AllowlistClassBinding> allowlistClassBindings,
        List<AllowlistInstanceBinding> allowlistInstanceBindings
    ) {

        this.classLoader = Objects.requireNonNull(classLoader);
        this.allowlistClasses = Collections.unmodifiableList(Objects.requireNonNull(allowlistClasses));
        this.allowlistImportedMethods = Collections.unmodifiableList(Objects.requireNonNull(allowlistImportedMethods));
        this.allowlistClassBindings = Collections.unmodifiableList(Objects.requireNonNull(allowlistClassBindings));
        this.allowlistInstanceBindings = Collections.unmodifiableList(Objects.requireNonNull(allowlistInstanceBindings));
    }
}
