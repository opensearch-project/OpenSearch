/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless.spi;

import org.opensearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Allowlist contains data structures designed to be used to generate an allowlist of Java classes,
 * constructors, methods, and fields that can be used within a Painless script at both compile-time
 * and run-time.
 *
 * An Allowlist consists of several pieces with {@link AllowlistClass}s as the top level. Each
 * {@link AllowlistClass} will contain zero-to-many {@link AllowlistConstructor}s, {@link AllowlistMethod}s, and
 * {@link AllowlistField}s which are what will be available with a Painless script.  See each individual
 * allowlist object for more detail.
 */
public final class Allowlist extends Whitelist {

    public static final List<Allowlist> BASE_ALLOWLISTS = Collections.singletonList(
        AllowlistLoader.loadFromResourceFiles(Allowlist.class, WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS, BASE_ALLOWLIST_FILES)
    );

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
        super(
            classLoader,
            allowlistClasses.stream().map(e -> (WhitelistClass) e).collect(Collectors.toList()),
            allowlistImportedMethods.stream().map(e -> (WhitelistMethod) e).collect(Collectors.toList()),
            allowlistClassBindings.stream().map(e -> (WhitelistClassBinding) e).collect(Collectors.toList()),
            allowlistInstanceBindings.stream().map(e -> (WhitelistInstanceBinding) e).collect(Collectors.toList())
        );

        this.allowlistClasses = Collections.unmodifiableList(Objects.requireNonNull(allowlistClasses));
        this.allowlistImportedMethods = Collections.unmodifiableList(Objects.requireNonNull(allowlistImportedMethods));
        this.allowlistClassBindings = Collections.unmodifiableList(Objects.requireNonNull(allowlistClassBindings));
        this.allowlistInstanceBindings = Collections.unmodifiableList(Objects.requireNonNull(allowlistInstanceBindings));
    }
}
