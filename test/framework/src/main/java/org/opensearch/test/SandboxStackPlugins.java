/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.Booleans;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared helpers for installing the sandbox engine stack (arrow-base, arrow-flight, analytics-engine, the data-format
 * plugins, and the datafusion/lucene analytics backends) onto test nodes when the build forwards
 * {@code -Dsandbox.enabled=true}. Extracted from {@link OpenSearchIntegTestCase} so both it and
 * {@link OpenSearchSingleNodeTestCase} inject the SAME stack with the SAME feature-flag / Flight-port coupling and the
 * SAME {@code installSandboxPlugins()} opt-out, rather than each base class carrying its own private copy.
 * <p>
 * This is the single helper shared with the {@code :server:test} wiring added by PR #23153, which introduced the
 * class for {@link OpenSearchSingleNodeTestCase}. The two server source sets differ only in whether the DSL query
 * executor is on the test runtime classpath: {@code :server:test} puts it there, so it is installed as an optional
 * stack member (see {@link #OPTIONAL_SANDBOX_STACK_PLUGINS}); server/build.gradle keeps it off the
 * {@code :server:internalClusterTest} classpath, so internal cluster tests get the stack without it and every search
 * takes the normal (fallback) path. test:framework has no compile dependency on the JDK 25 sandbox plugins, so every
 * plugin is referenced by its String FQCN and resolved reflectively in {@link #resolve()}.
 */
public final class SandboxStackPlugins {

    private SandboxStackPlugins() {}

    // Sandbox stack plugin FQCNs. test:framework has no compile dependency on the JDK 25 sandbox plugins (they are
    // resolved reflectively in resolve()), so every reference is a String FQCN, never a Class literal. The
    // PluginInfo.name of a classpath plugin is its FQCN, and PluginsService.loadExtensions matches extenders to the
    // plugin they extend BY NAME, so the extendedPlugins values in SANDBOX_STACK_PLUGINS below must likewise be FQCNs
    // (not the gradle plugin names used in each plugin's build.gradle extendedPlugins block).
    static final String ARROW_BASE_PLUGIN = "org.opensearch.arrow.allocator.ArrowBasePlugin";
    static final String STREAM_TRANSPORT_PLUGIN = "org.opensearch.arrow.flight.transport.FlightStreamPlugin";
    static final String ANALYTICS_PLUGIN = "org.opensearch.analytics.AnalyticsPlugin";
    static final String COMPOSITE_DATAFORMAT_PLUGIN = "org.opensearch.composite.CompositeDataFormatPlugin";
    static final String PARQUET_DATAFORMAT_PLUGIN = "org.opensearch.parquet.ParquetDataFormatPlugin";
    static final String DATAFUSION_PLUGIN = "org.opensearch.be.datafusion.DataFusionPlugin";
    static final String LUCENE_PLUGIN = "org.opensearch.be.lucene.LucenePlugin";
    static final String DSL_QUERY_EXECUTOR_PLUGIN = "org.opensearch.dsl.DslQueryExecutorPlugin";

    /**
     * The sandbox engine stack in fixed load order (arrow-base before the data-format/backend plugins), mapping each
     * plugin FQCN to the FQCNs of the plugins it extends — mirroring each plugin's build.gradle {@code extendedPlugins}
     * exactly, including the analytics-engine edge for the DataFusion/Lucene backends (see the shared-classloader SPI
     * note in the static initializer below).
     * <p>
     * The stack is supplied to the cluster as {@link PluginInfo}s carrying this extension metadata (see
     * {@link #pluginInfos()}), NOT as bare plugin classes. A bare classpath class is wrapped by
     * InternalTestCluster#buildNode with an EMPTY extendedPlugins list, so PluginsService.loadExtensions would never
     * hand the backend plugins (DataFusion/Lucene) to AnalyticsPlugin and its backend list would stay empty. Keyed by
     * FQCN because that is both the PluginInfo.name used for extender matching and the key {@link #resolve()} resolves
     * reflectively.
     */
    private static final Map<String, List<String>> SANDBOX_STACK_PLUGINS;
    static {
        LinkedHashMap<String, List<String>> stack = new LinkedHashMap<>();
        stack.put(ARROW_BASE_PLUGIN, List.of());
        stack.put(STREAM_TRANSPORT_PLUGIN, List.of(ARROW_BASE_PLUGIN));
        stack.put(ANALYTICS_PLUGIN, List.of(ARROW_BASE_PLUGIN));
        stack.put(COMPOSITE_DATAFORMAT_PLUGIN, List.of());
        stack.put(PARQUET_DATAFORMAT_PLUGIN, List.of(ARROW_BASE_PLUGIN, COMPOSITE_DATAFORMAT_PLUGIN));
        // These edges mirror each backend plugin's build.gradle extendedPlugins exactly: DataFusion extends
        // analytics-engine; Lucene extends analytics-engine and composite-engine. Both backends expose an
        // AnalyticsSearchBackendPlugin SPI service, and under the single shared test classloader PluginsService sees
        // both at once; PluginsService tolerates the sibling SPI entry (see PluginsService#isExtensionOfSiblingPlugin),
        // so DataFusion and Lucene both register as analytics backends and the node still starts.
        stack.put(DATAFUSION_PLUGIN, List.of(ANALYTICS_PLUGIN));
        stack.put(LUCENE_PLUGIN, List.of(ANALYTICS_PLUGIN, COMPOSITE_DATAFORMAT_PLUGIN));
        SANDBOX_STACK_PLUGINS = Collections.unmodifiableMap(stack);
    }

    /**
     * Stack members installed only when their class is on the test runtime classpath, appended after
     * {@link #SANDBOX_STACK_PLUGINS} and mapped to the FQCNs of the plugins they extend in the same way.
     * A missing optional member is never reported as a partial resolution by {@link #resolve()}.
     * <p>
     * The DSL query executor is optional because the two server source sets disagree on it on purpose:
     * {@code :server:test} declares it (PR #23153), while server/build.gradle excludes it from the
     * {@code :server:internalClusterTest} classpath so no action filter intercepts _search there. Its edge mirrors the
     * plugin's build.gradle {@code extendedPlugins = ['analytics-engine']}.
     */
    private static final Map<String, List<String>> OPTIONAL_SANDBOX_STACK_PLUGINS = Map.of(
        DSL_QUERY_EXECUTOR_PLUGIN,
        List.of(ANALYTICS_PLUGIN)
    );

    /** Data-format plugins whose presence should turn on PLUGGABLE_DATAFORMAT (the real ones, not test mocks). */
    private static final Set<String> SANDBOX_DATAFORMAT_PLUGINS = Set.of(PARQUET_DATAFORMAT_PLUGIN, COMPOSITE_DATAFORMAT_PLUGIN);

    /**
     * Memoised SUCCESSFUL result of {@link #resolve()} (either the empty none-resolvable list or the full stack). The
     * partial/failure path is deliberately NOT cached so it keeps throwing at the call site.
     */
    private static volatile Collection<Class<? extends Plugin>> resolvedSandboxStackPlugins;

    /**
     * @return true only when the build forwarded {@code -Dsandbox.enabled=true} into the forked test JVM. Gradle does
     * not propagate the {@code -D} build flag automatically; the server test tasks forward it explicitly.
     */
    public static boolean isEnabled() {
        return Booleans.parseBoolean(System.getProperty("sandbox.enabled"), false);
    }

    /**
     * The sandbox parquet/analytics plugin stack resolved reflectively (test:framework cannot depend on the JDK 25
     * sandbox plugins): every {@link #SANDBOX_STACK_PLUGINS} class, followed by each
     * {@link #OPTIONAL_SANDBOX_STACK_PLUGINS} class that is on the classpath. Empty when {@code sandbox.enabled} is
     * not set, or in modules where none of the required classes are on the classpath (the intended no-op). If some
     * required classes resolve but others do not, a plugin was renamed or moved: fail loudly rather than silently
     * dropping engine coverage.
     *
     * @throws IllegalStateException if some {@link #SANDBOX_STACK_PLUGINS} classes resolve but others do not
     */
    @SuppressWarnings("unchecked")
    public static Collection<Class<? extends Plugin>> resolve() {
        if (isEnabled() == false) {
            return Collections.emptyList();
        }
        Collection<Class<? extends Plugin>> cached = resolvedSandboxStackPlugins;
        if (cached != null) {
            return cached;
        }
        ArrayList<Class<? extends Plugin>> resolved = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String className : SANDBOX_STACK_PLUGINS.keySet()) {
            try {
                resolved.add((Class<? extends Plugin>) Class.forName(className));
            } catch (ClassNotFoundException e) {
                missing.add(className);
            }
        }
        if (resolved.isEmpty()) {
            resolvedSandboxStackPlugins = Collections.emptyList();
            return resolvedSandboxStackPlugins;
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
        for (String className : OPTIONAL_SANDBOX_STACK_PLUGINS.keySet()) {
            try {
                resolved.add((Class<? extends Plugin>) Class.forName(className));
            } catch (ClassNotFoundException e) {
                // Not on this source set's classpath (e.g. dsl-query-executor in internalClusterTest): not installed.
            }
        }
        resolvedSandboxStackPlugins = Collections.unmodifiableList(resolved);
        return resolvedSandboxStackPlugins;
    }

    /**
     * The sandbox stack rendered as classpath {@link PluginInfo}s, each carrying the {@code extendedPlugins} metadata
     * from {@link #SANDBOX_STACK_PLUGINS} (or {@link #OPTIONAL_SANDBOX_STACK_PLUGINS} for an optional member). This is
     * the ONLY place the stack's extension wiring is preserved: a bare
     * class handed to InternalTestCluster#buildNode is wrapped with an EMPTY extendedPlugins list, so without these
     * PluginInfos PluginsService.loadExtensions would never hand DataFusionPlugin/LucenePlugin to AnalyticsPlugin and
     * its backend list would stay empty. Empty when none of the classes resolve; callers gate on the opt-out
     * ({@code installSandboxPlugins()}) before calling this.
     */
    public static Collection<PluginInfo> pluginInfos() {
        Collection<Class<? extends Plugin>> resolved = resolve();
        if (resolved.isEmpty()) {
            return Collections.emptyList();
        }
        List<PluginInfo> infos = new ArrayList<>(resolved.size());
        for (Class<? extends Plugin> cls : resolved) {
            // extendedPlugins values are FQCNs: PluginInfo.name of a classpath plugin is its FQCN and
            // loadExtensions matches extenders to the extended plugin by that name (see SANDBOX_STACK_PLUGINS).
            List<String> extendedPlugins = SANDBOX_STACK_PLUGINS.containsKey(cls.getName())
                ? SANDBOX_STACK_PLUGINS.get(cls.getName())
                : OPTIONAL_SANDBOX_STACK_PLUGINS.getOrDefault(cls.getName(), List.of());
            // Same 9-arg classpath-plugin shape as ClasspathPluginIT / BaseScalarFunctionIT (name == classname == FQCN);
            // the extendedPlugins list is the metadata buildNode() cannot infer for a bare class.
            infos.add(
                new PluginInfo(cls.getName(), "classpath plugin", "NA", Version.CURRENT, "1.8", cls.getName(), null, extendedPlugins, false)
            );
        }
        return infos;
    }

    /**
     * Whether the given plugin class is part of the sandbox stack (matched by FQCN against
     * {@link #SANDBOX_STACK_PLUGINS} and {@link #OPTIONAL_SANDBOX_STACK_PLUGINS}).
     */
    public static boolean isStackPlugin(Class<?> plugin) {
        return plugin != null
            && (SANDBOX_STACK_PLUGINS.containsKey(plugin.getName()) || OPTIONAL_SANDBOX_STACK_PLUGINS.containsKey(plugin.getName()));
    }

    /** Whether FlightStreamPlugin (the stream transport) is among the given loaded plugins. */
    static boolean loadsStreamTransport(Collection<Class<? extends Plugin>> loadedPlugins) {
        return loadedPlugins.stream().anyMatch(c -> STREAM_TRANSPORT_PLUGIN.equals(c.getName()));
    }

    /** Whether a real data-format plugin (parquet/composite) is among the given loaded plugins. */
    static boolean loadsDataFormat(Collection<Class<? extends Plugin>> loadedPlugins) {
        return loadedPlugins.stream().anyMatch(c -> SANDBOX_DATAFORMAT_PLUGINS.contains(c.getName()));
    }

    /**
     * Mirrors {@code OpenSearchIntegTestCase.featureFlagSettings()}'s suppression for base classes (like
     * {@link OpenSearchSingleNodeTestCase}) that always emit every built-in flag at its default: when a sandbox plugin
     * that OWNS a feature flag is loaded, drop that flag from the given feature-flag settings so
     * {@link #applyNodeSettings} can own it per node. Without this the base class's default-false emission would make
     * {@link #applyNodeSettings}'s "test disabled the flag" warning fire on every suite even though no test touched the
     * flag, and the snapshot the warning reads could not tell a deliberate opt-out from the default.
     */
    static Settings suppressOwnedFlags(Settings featureFlagSettings, Collection<Class<? extends Plugin>> loadedPlugins) {
        boolean dropStreamTransport = loadsStreamTransport(loadedPlugins);
        boolean dropPluggableDataformat = loadsDataFormat(loadedPlugins);
        if (dropStreamTransport == false && dropPluggableDataformat == false) {
            return featureFlagSettings;
        }
        return featureFlagSettings.filter(
            key -> (dropStreamTransport && FeatureFlags.STREAM_TRANSPORT.equals(key)) == false
                && (dropPluggableDataformat && FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG.equals(key)) == false
        );
    }

    /**
     * Couple the sandbox feature flags to the plugins actually loaded on a node, exactly as the
     * {@code OpenSearchIntegTestCase} node-config wrapper does. Forces each flag true only when its plugin is loaded
     * (and gives Flight a per-worker port range), so flag and plugin stay in lockstep and a loaded plugin can never run
     * with its flag silently off. Must be applied to the settings builder AFTER the test's own node settings and
     * feature-flag settings so this coupling wins: it is deliberately un-overridable — opt out of the whole stack via
     * {@code installSandboxPlugins()} instead of disabling a flag whose plugin is loaded.
     *
     * @param builder         node settings being assembled; mutated in place
     * @param loadedPlugins   the plugin classes actually loaded on this node (test plugins + resolved stack)
     * @param flightPortRange per-worker Flight port range (e.g. {@link OpenSearchTestCase#getPortRange()})
     * @param logger          logger for the warn-on-conflict messages
     */
    public static void applyNodeSettings(
        Settings.Builder builder,
        Collection<Class<? extends Plugin>> loadedPlugins,
        String flightPortRange,
        Logger logger
    ) {
        // Snapshot of the merged test settings, captured BEFORE the coupling forces flags below.
        // Used only to detect an author who explicitly disabled a sandbox flag whose plugin is actually loaded.
        Settings mergedTestSettings = builder.build();
        if (loadsStreamTransport(loadedPlugins)) {
            // When FlightStreamPlugin is loaded the wrapper must enable STREAM_TRANSPORT: the flag registers
            // aux.transport.transport-flight.port (set just below) and FlightStreamPlugin only binds
            // StreamTransportService when the flag is enabled, so honoring a test-supplied false while the
            // plugin is loaded would fail node startup. A test that overrides featureFlagSettings() without
            // calling super re-emits every built-in flag at its default (STREAM_TRANSPORT defaults to false)
            // and lands here; warn and force the flag true rather than failing, matching the prior behavior.
            if (mergedTestSettings.hasValue(FeatureFlags.STREAM_TRANSPORT)
                && mergedTestSettings.getAsBoolean(FeatureFlags.STREAM_TRANSPORT, true) == false) {
                logger.warn(
                    "Test set feature flag [{}]=false while sandbox plugin [{}] is loaded; forcing it to true "
                        + "because the flag registers aux.transport.transport-flight.port and FlightStreamPlugin binds "
                        + "StreamTransportService only when the flag is enabled, so honoring false would fail node startup. "
                        + "Override installSandboxPlugins() to return false if you genuinely need the node without the stack, "
                        + "or call super.featureFlagSettings() so the base suppression applies.",
                    FeatureFlags.STREAM_TRANSPORT,
                    STREAM_TRANSPORT_PLUGIN
                );
            }
            builder.put(FeatureFlags.STREAM_TRANSPORT, true);
            // Give Flight a per-worker port range: the 9400-9500 default is shared across parallel forks,
            // and exhausting it makes bind fail, the node leak threads, and later suites get skipped.
            // getPortRange() hands each worker a disjoint slice. Test-only; production keeps the default.
            builder.put("aux.transport.transport-flight.port", flightPortRange);
        }
        if (loadsDataFormat(loadedPlugins)) {
            // Same coupling as above: don't let a loaded data-format plugin run with its flag silently forced
            // off. A test that overrides featureFlagSettings() without calling super re-emits built-in flags at
            // their defaults and lands here; warn and force the flag true rather than failing.
            if (mergedTestSettings.hasValue(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
                && mergedTestSettings.getAsBoolean(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true) == false) {
                String loadedDataFormatPlugin = loadedPlugins.stream()
                    .map(Class::getName)
                    .filter(SANDBOX_DATAFORMAT_PLUGINS::contains)
                    .findFirst()
                    .orElse("a data-format plugin");
                logger.warn(
                    "Test set feature flag [{}]=false while sandbox plugin [{}] is loaded; forcing it to true "
                        + "because the data-format plugin requires the flag to be enabled, so honoring false while the "
                        + "plugin is loaded would fail node startup. Override installSandboxPlugins() to return false if you "
                        + "genuinely need the node without the stack, or call super.featureFlagSettings() so the base "
                        + "suppression applies.",
                    FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG,
                    loadedDataFormatPlugin
                );
            }
            builder.put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true);
        }
    }
}
