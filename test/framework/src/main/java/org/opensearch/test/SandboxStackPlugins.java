/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.opensearch.common.Booleans;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared resolver for the sandbox "mustang" engine stack (arrow + analytics + DSL + parquet plugins) that the test
 * framework loads in-JVM on test nodes when {@code -Dsandbox.enabled=true} is forwarded into the forked test worker.
 * <p>
 * The eight plugin classes are resolved <em>reflectively</em> (via {@link Class#forName(String)}) on purpose:
 * {@code test:framework} compiles its main source set with {@code --release 21}, but the sandbox plugins are JDK 25
 * class files. A compile-time dependency on them would force the whole framework to JDK 25 and break every JDK 21
 * consumer. Reflection lets the framework compile without the plugins on its classpath and pick them up only in the
 * modules (server unit tests, internal cluster tests) that actually put the plugin jars on the test runtime classpath.
 * <p>
 * This class is intentionally package-scoped to {@code org.opensearch.test}: it is a test-framework internal shared by
 * {@link OpenSearchSingleNodeTestCase} (and potentially other base test cases).
 */
class SandboxStackPlugins {

    /**
     * Main-class names of the eight sandbox stack plugins, in {@code extendedPlugins} order (arrow-base first, so the
     * Arrow allocator/transport are present before the data-format and DSL plugins that depend on them). These are the
     * exact {@code classname} values declared in each plugin's {@code build.gradle}.
     */
    private static final List<String> SANDBOX_STACK_PLUGINS = List.of(
        "org.opensearch.arrow.allocator.ArrowBasePlugin",
        "org.opensearch.arrow.flight.transport.FlightStreamPlugin",
        "org.opensearch.analytics.AnalyticsPlugin",
        "org.opensearch.composite.CompositeDataFormatPlugin",
        "org.opensearch.parquet.ParquetDataFormatPlugin",
        "org.opensearch.be.datafusion.DataFusionPlugin",
        "org.opensearch.be.lucene.LucenePlugin",
        "org.opensearch.dsl.DslQueryExecutorPlugin"
    );

    /**
     * Memoised SUCCESSFUL result of {@link #resolve()} (either the empty none-resolvable list or the full stack). The
     * partial/failure path is deliberately NOT cached so it keeps throwing at the call site on every call.
     */
    private static volatile List<Class<? extends Plugin>> resolved;

    private SandboxStackPlugins() {}

    /**
     * @return true only when the build forwarded {@code -Dsandbox.enabled=true} into the forked test JVM. Gradle does
     * not propagate the {@code -D} build flag automatically; the server test task forwards it explicitly.
     */
    static boolean isEnabled() {
        return Booleans.parseBoolean(System.getProperty("sandbox.enabled"), false);
    }

    /**
     * Resolve the sandbox stack reflectively.
     *
     * @return an empty list when {@code sandbox.enabled} is not set, or when none of the plugin classes are on the
     * classpath (the intended no-op in modules that do not put the plugins on the test runtime classpath); otherwise
     * the full ordered list of eight plugin classes.
     * @throws IllegalStateException if some plugin classes resolve but others do not — that means a plugin was renamed
     * or moved, and silently dropping engine coverage would be worse than failing loudly.
     */
    @SuppressWarnings("unchecked")
    static List<Class<? extends Plugin>> resolve() {
        if (isEnabled() == false) {
            return Collections.emptyList();
        }
        List<Class<? extends Plugin>> cached = resolved;
        if (cached != null) {
            return cached;
        }
        ArrayList<Class<? extends Plugin>> found = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String className : SANDBOX_STACK_PLUGINS) {
            try {
                found.add((Class<? extends Plugin>) Class.forName(className));
            } catch (ClassNotFoundException e) {
                missing.add(className);
            }
        }
        if (found.isEmpty()) {
            resolved = Collections.emptyList();
            return resolved;
        }
        if (missing.isEmpty() == false) {
            // Deliberately do NOT cache this partial/failure path: it must keep throwing at the CALL SITE on every
            // call so the diagnostic stays attached to the caller rather than degrading into ExceptionInInitializerError.
            throw new IllegalStateException(
                "Sandbox is enabled and some sandbox plugins loaded, but these were not found on the test "
                    + "classpath (renamed or moved?): "
                    + missing
            );
        }
        resolved = Collections.unmodifiableList(found);
        return resolved;
    }

    /**
     * @return node settings that enable the two sandbox feature flags the stack requires: the pluggable data-format
     * flag and the stream-transport flag. These are applied via <em>node settings</em> (not JVM-wide system
     * properties) so that flag-off unit tests running in the same fork are unaffected.
     */
    static Settings featureFlagSettings() {
        return Settings.builder()
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }
}
