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

package org.opensearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.SPIClassIterator;
import org.opensearch.Build;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModulesWithStatus;
import org.opensearch.bootstrap.JarHell;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Module;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.service.ReportingService;
import org.opensearch.index.IndexModule;
import org.opensearch.semver.SemverRange;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.transport.TransportSettings;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.core.util.FileSystemUtils.isAccessibleDirectory;

/**
 * Service responsible for loading plugins and modules (internal and external)
 *
 * @opensearch.internal
 */
public class PluginsService implements ReportingService<PluginsAndModules> {

    private static final Logger logger = LogManager.getLogger(PluginsService.class);

    private final Settings settings;
    private final Path configPath;

    /**
     * We keep around a list of plugins and modules
     */
    private volatile List<Tuple<PluginInfo, Plugin>> plugins;
    private volatile PluginsAndModules info;

    /**
     * Map to track plugin load status for hot reload functionality
     */
    private final Map<String, PluginLoadStatus> pluginLoadStatusMap = new ConcurrentHashMap<>();

    public static final Setting<List<String>> MANDATORY_SETTING = Setting.listSetting(
        "plugin.mandatory",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public List<Setting<?>> getPluginSettings() {
        return plugins.stream().flatMap(p -> p.v2().getSettings().stream()).collect(Collectors.toList());
    }

    public List<String> getPluginSettingsFilter() {
        return plugins.stream().flatMap(p -> p.v2().getSettingsFilter().stream()).collect(Collectors.toList());
    }

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public PluginsService(
        Settings settings,
        Path configPath,
        Path modulesDirectory,
        Path pluginsDirectory,
        Collection<Class<? extends Plugin>> classpathPlugins
    ) {
        this.settings = settings;
        this.configPath = configPath;

        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = new ArrayList<>();
        List<PluginInfo> pluginsList = new ArrayList<>();
        // we need to build a List of plugins for checking mandatory plugins
        final List<String> pluginsNames = new ArrayList<>();
        // first we load plugins that are on the classpath. this is for tests
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginInfo pluginInfo = new PluginInfo(
                pluginClass.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                pluginClass.getName(),
                null,
                Collections.emptyList(),
                false
            );
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new Tuple<>(pluginInfo, plugin));
            pluginsList.add(pluginInfo);
            pluginsNames.add(pluginInfo.getName());
        }

        Set<Bundle> seenBundles = new LinkedHashSet<>();
        List<PluginInfo> modulesList = new ArrayList<>();
        // load modules
        if (modulesDirectory != null) {
            try {
                Set<Bundle> modules = getModuleBundles(modulesDirectory);
                for (Bundle bundle : modules) {
                    modulesList.add(bundle.plugin);
                }
                seenBundles.addAll(modules);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // now, find all the ones that are in plugins/
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    checkForFailedPluginRemovals(pluginsDirectory);
                    Set<Bundle> plugins = getPluginBundles(pluginsDirectory);
                    for (final Bundle bundle : plugins) {
                        pluginsList.add(bundle.plugin);
                        pluginsNames.add(bundle.plugin.getName());
                    }
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        List<Tuple<PluginInfo, Plugin>> loaded = loadBundles(seenBundles);
        pluginsLoaded.addAll(loaded);

        this.info = new PluginsAndModules(pluginsList, modulesList);
        this.plugins = Collections.unmodifiableList(pluginsLoaded);

        // Checking expected plugins
        List<String> mandatoryPlugins = MANDATORY_SETTING.get(settings);
        if (mandatoryPlugins.isEmpty() == false) {
            Set<String> missingPlugins = new HashSet<>();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!pluginsNames.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                final String message = String.format(
                    Locale.ROOT,
                    "missing mandatory plugins [%s], found plugins [%s]",
                    Strings.collectionToDelimitedString(missingPlugins, ", "),
                    Strings.collectionToDelimitedString(pluginsNames, ", ")
                );
                throw new IllegalStateException(message);
            }
        }

        // Initialize plugin load status for all initially loaded plugins
        for (PluginInfo pluginInfo : pluginsList) {
            pluginLoadStatusMap.put(pluginInfo.getName(), PluginLoadStatus.INITIAL);
        }

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        logPluginInfo(info.getModuleInfos(), "module", logger);
        logPluginInfo(info.getPluginInfos(), "plugin", logger);
    }

    private static void logPluginInfo(final List<PluginInfo> pluginInfos, final String type, final Logger logger) {
        assert pluginInfos != null;
        if (pluginInfos.isEmpty()) {
            logger.info("no " + type + "s loaded");
        } else {
            for (final String name : pluginInfos.stream().map(PluginInfo::getName).sorted().collect(Collectors.toList())) {
                logger.info("loaded " + type + " [" + name + "]");
            }
        }
    }

    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Map<String, String> features = new TreeMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            Settings settings = plugin.v2().additionalSettings();
            for (String setting : settings.keySet()) {
                String oldPlugin = foundSettings.put(setting, plugin.v1().getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException(
                        "Cannot have additional setting ["
                            + setting
                            + "] "
                            + "in plugin ["
                            + plugin.v1().getName()
                            + "], already added in plugin ["
                            + oldPlugin
                            + "]"
                    );
                }
            }
            builder.put(settings);
            final Optional<String> maybeFeature = plugin.v2().getFeature();
            if (maybeFeature.isPresent()) {
                final String feature = maybeFeature.get();
                if (features.containsKey(feature)) {
                    final String message = String.format(
                        Locale.ROOT,
                        "duplicate feature [%s] in plugin [%s], already added in [%s]",
                        feature,
                        plugin.v1().getName(),
                        features.get(feature)
                    );
                    throw new IllegalArgumentException(message);
                }
                features.put(feature, plugin.v1().getName());
            }
        }
        for (final String feature : features.keySet()) {
            builder.put(TransportSettings.FEATURE_PREFIX + "." + feature, true);
        }
        return builder.put(this.settings).build();
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().createGuiceModules());
        }
        return modules;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final ArrayList<ExecutorBuilder<?>> builders = new ArrayList<>();
        for (final Tuple<PluginInfo, Plugin> plugin : plugins) {
            builders.addAll(plugin.v2().getExecutorBuilders(settings));
        }
        return builders;
    }

    /** Returns all classes injected into guice by plugins which extend {@link LifecycleComponent}. */
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().getGuiceServiceClasses());
        }
        return services;
    }

    public void onIndexModule(IndexModule indexModule) {
        logger.info("PluginService:onIndexModule index:" + indexModule.getIndex());
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            plugin.v2().onIndexModule(indexModule);
        }
    }

    /**
     * Get information about plugins and modules
     */
    @Override
    public PluginsAndModules info() {
        return info;
    }

    /**
     * Get information about plugins and modules with load status information
     */
    public PluginsAndModulesWithStatus infoWithStatus() {
        return new PluginsAndModulesWithStatus(info.getPluginInfos(), info.getModuleInfos(), pluginLoadStatusMap);
    }

    // a "bundle" is a group of jars in a single classloader
    static class Bundle {
        final PluginInfo plugin;
        final Set<URL> urls;

        Bundle(PluginInfo plugin, Path dir) throws IOException {
            this.plugin = Objects.requireNonNull(plugin);
            Set<URL> urls = new LinkedHashSet<>();
            // gather urls for jar files
            try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(dir, "*.jar")) {
                for (Path jar : jarStream) {
                    // normalize with toRealPath to get symlinks out of our hair
                    URL url = jar.toRealPath().toUri().toURL();
                    if (urls.add(url) == false) {
                        throw new IllegalStateException("duplicate codebase: " + url);
                    }
                }
            }
            this.urls = Objects.requireNonNull(urls);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bundle bundle = (Bundle) o;
            return Objects.equals(plugin, bundle.plugin);
        }

        @Override
        public int hashCode() {
            return Objects.hash(plugin);
        }
    }

    /**
     * Extracts all installed plugin directories from the provided {@code rootPath}.
     *
     * @param rootPath the path where the plugins are installed
     * @return a list of all plugin paths installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the directories
     */
    public static List<Path> findPluginDirs(final Path rootPath) throws IOException {
        final List<Path> plugins = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        if (Files.exists(rootPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
                for (Path plugin : stream) {
                    if (plugin.getFileName().toString().startsWith(".") && !Files.isDirectory(plugin)) {
                        logger.warn(
                            "Non-plugin file located in the plugins folder with the following name: [" + plugin.getFileName() + "]"
                        );
                        continue;
                    }
                    if (seen.add(plugin.getFileName().toString()) == false) {
                        throw new IllegalStateException("duplicate plugin: " + plugin);
                    }
                    plugins.add(plugin);
                }
            }
        }
        return plugins;
    }

    /**
     * Verify the given plugin is compatible with the current OpenSearch installation.
     */
    static void verifyCompatibility(PluginInfo info) {
        if (!isPluginVersionCompatible(info, Version.CURRENT)) {
            throw new IllegalArgumentException(
                "Plugin ["
                    + info.getName()
                    + "] was built for OpenSearch version "
                    + info.getOpenSearchVersionRangesString()
                    + " but version "
                    + Version.CURRENT
                    + " is running"
            );
        }
        JarHell.checkJavaVersion(info.getName(), info.getJavaVersion());
    }

    public static boolean isPluginVersionCompatible(final PluginInfo pluginInfo, final Version coreVersion) {
        // Core version must satisfy the semver range in plugin info
        for (SemverRange range : pluginInfo.getOpenSearchVersionRanges()) {
            if (!range.isSatisfiedBy(coreVersion)) {
                return false;
            }
        }
        return true;
    }

    static void checkForFailedPluginRemovals(final Path pluginsDirectory) throws IOException {
        /*
         * Check for the existence of a marker file that indicates any plugins are in a garbage state from a failed attempt to remove the
         * plugin.
         */
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory, ".removing-*")) {
            final Iterator<Path> iterator = stream.iterator();
            if (iterator.hasNext()) {
                final Path removing = iterator.next();
                final String fileName = removing.getFileName().toString();
                final String name = fileName.substring(1 + fileName.indexOf("-"));
                final String message = String.format(
                    Locale.ROOT,
                    "found file [%s] from a failed attempt to remove the plugin [%s]; execute [opensearch-plugin remove %2$s]",
                    removing,
                    name
                );
                throw new IllegalStateException(message);
            }
        }
    }

    /** Get bundles for plugins installed in the given modules directory. */
    static Set<Bundle> getModuleBundles(Path modulesDirectory) throws IOException {
        return findBundles(modulesDirectory, "module");
    }

    /** Get bundles for plugins installed in the given plugins directory. */
    static Set<Bundle> getPluginBundles(final Path pluginsDirectory) throws IOException {
        return findBundles(pluginsDirectory, "plugin");
    }

    // searches subdirectories under the given directory for plugin directories
    private static Set<Bundle> findBundles(final Path directory, String type) throws IOException {
        final Set<Bundle> bundles = new HashSet<>();
        for (final Path plugin : findPluginDirs(directory)) {
            final Bundle bundle = readPluginBundle(bundles, plugin, type);
            bundles.add(bundle);
        }

        return bundles;
    }

    // get a bundle for a single plugin dir
    private static Bundle readPluginBundle(final Set<Bundle> bundles, final Path plugin, String type) throws IOException {
        LogManager.getLogger(PluginsService.class).trace("--- adding [{}] [{}]", type, plugin.toAbsolutePath());
        final PluginInfo info;
        try {
            info = PluginInfo.readFromProperties(plugin);
        } catch (final IOException e) {
            throw new IllegalStateException(
                "Could not load plugin descriptor for " + type + " directory [" + plugin.getFileName() + "]",
                e
            );
        }
        final Bundle bundle = new Bundle(info, plugin);
        if (bundles.add(bundle) == false) {
            throw new IllegalStateException("duplicate " + type + ": " + info);
        }
        if (type.equals("module") && info.getName().startsWith("test-") && Build.CURRENT.isSnapshot() == false) {
            throw new IllegalStateException("external test module [" + plugin.getFileName() + "] found in non-snapshot build");
        }
        return bundle;
    }

    /**
     * Return the given bundles, sorted in dependency loading order.
     * <p>
     * This sort is stable, so that if two plugins do not have any interdependency,
     * their relative order from iteration of the provided set will not change.
     *
     * @throws IllegalStateException if a dependency cycle is found
     */
    // pkg private for tests
    static List<Bundle> sortBundles(Set<Bundle> bundles) {
        Map<String, Bundle> namedBundles = bundles.stream().collect(Collectors.toMap(b -> b.plugin.getName(), Function.identity()));
        LinkedHashSet<Bundle> sortedBundles = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();
        for (Bundle bundle : bundles) {
            addSortedBundle(bundle, namedBundles, sortedBundles, dependencyStack);
        }
        return new ArrayList<>(sortedBundles);
    }

    // add the given bundle to the sorted bundles, first adding dependencies
    private static void addSortedBundle(
        Bundle bundle,
        Map<String, Bundle> bundles,
        LinkedHashSet<Bundle> sortedBundles,
        LinkedHashSet<String> dependencyStack
    ) {

        String name = bundle.plugin.getName();
        if (dependencyStack.contains(name)) {
            StringBuilder msg = new StringBuilder("Cycle found in plugin dependencies: ");
            dependencyStack.forEach(s -> {
                msg.append(s);
                msg.append(" -> ");
            });
            msg.append(name);
            throw new IllegalStateException(msg.toString());
        }
        if (sortedBundles.contains(bundle)) {
            // already added this plugin, via a dependency
            return;
        }

        dependencyStack.add(name);
        for (String dependency : bundle.plugin.getExtendedPlugins()) {
            Bundle depBundle = bundles.get(dependency);
            if (depBundle == null) {
                if (bundle.plugin.isExtendedPluginOptional(dependency)) {
                    logger.warn("Missing plugin [" + dependency + "], dependency of [" + name + "]");
                    logger.warn("Some features of this plugin may not function without the dependencies being installed.\n");
                    continue;
                } else {
                    throw new IllegalArgumentException("Missing plugin [" + dependency + "], dependency of [" + name + "]");
                }
            }
            addSortedBundle(depBundle, bundles, sortedBundles, dependencyStack);
            assert sortedBundles.contains(depBundle);
        }
        dependencyStack.remove(name);

        sortedBundles.add(bundle);
    }

    private List<Tuple<PluginInfo, Plugin>> loadBundles(Set<Bundle> bundles) {
        List<Tuple<PluginInfo, Plugin>> plugins = new ArrayList<>();
        Map<String, Plugin> loaded = new HashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<Bundle> sortedBundles = sortBundles(bundles);
        for (Bundle bundle : sortedBundles) {
            checkBundleJarHell(JarHell.parseClassPath(), bundle, transitiveUrls);

            final Plugin plugin = loadBundle(bundle, loaded);
            plugins.add(new Tuple<>(bundle.plugin, plugin));
        }

        loadExtensions(plugins);
        return Collections.unmodifiableList(plugins);
    }

    // package-private for test visibility
    static void loadExtensions(List<Tuple<PluginInfo, Plugin>> plugins) {
        Map<String, List<Plugin>> extendingPluginsByName = plugins.stream()
            .flatMap(t -> t.v1().getExtendedPlugins().stream().map(extendedPlugin -> Tuple.tuple(extendedPlugin, t.v2())))
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));
        for (Tuple<PluginInfo, Plugin> pluginTuple : plugins) {
            if (pluginTuple.v2() instanceof ExtensiblePlugin) {
                loadExtensionsForPlugin(
                    (ExtensiblePlugin) pluginTuple.v2(),
                    extendingPluginsByName.getOrDefault(pluginTuple.v1().getName(), Collections.emptyList())
                );
            }
        }
    }

    private static void loadExtensionsForPlugin(ExtensiblePlugin extensiblePlugin, List<Plugin> extendingPlugins) {
        ExtensiblePlugin.ExtensionLoader extensionLoader = new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                List<T> result = new ArrayList<>();
                for (Plugin extendingPlugin : extendingPlugins) {
                    result.addAll(createExtensions(extensionPointType, extendingPlugin));
                }
                return Collections.unmodifiableList(result);
            }
        };

        extensiblePlugin.loadExtensions(extensionLoader);
    }

    private static <T> List<? extends T> createExtensions(Class<T> extensionPointType, Plugin plugin) {
        SPIClassIterator<T> classIterator = SPIClassIterator.get(extensionPointType, plugin.getClass().getClassLoader());
        List<T> extensions = new ArrayList<>();
        while (classIterator.hasNext()) {
            Class<? extends T> extensionClass = classIterator.next();
            extensions.add(createExtension(extensionClass, extensionPointType, plugin));
        }
        return extensions;
    }

    // package-private for test visibility
    static <T> T createExtension(Class<? extends T> extensionClass, Class<T> extensionPointType, Plugin plugin) {
        // noinspection unchecked
        Constructor<T>[] constructors = (Constructor<T>[]) extensionClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public " + extensionConstructorMessage(extensionClass, extensionPointType));
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public " + extensionConstructorMessage(extensionClass, extensionPointType));
        }

        final Constructor<T> constructor = constructors[0];
        if (constructor.getParameterCount() > 1) {
            throw new IllegalStateException(extensionSignatureMessage(extensionClass, extensionPointType, plugin));
        }

        if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0] != plugin.getClass()) {
            throw new IllegalStateException(
                extensionSignatureMessage(extensionClass, extensionPointType, plugin)
                    + ", not ("
                    + constructor.getParameterTypes()[0].getName()
                    + ")"
            );
        }

        try {
            if (constructor.getParameterCount() == 0) {
                return constructor.newInstance();
            } else {
                return constructor.newInstance(plugin);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                "failed to create extension [" + extensionClass.getName() + "] of type [" + extensionPointType.getName() + "]",
                e
            );
        }
    }

    private static <T> String extensionSignatureMessage(Class<? extends T> extensionClass, Class<T> extensionPointType, Plugin plugin) {
        return "signature of "
            + extensionConstructorMessage(extensionClass, extensionPointType)
            + " must be either () or ("
            + plugin.getClass().getName()
            + ")";
    }

    private static <T> String extensionConstructorMessage(Class<? extends T> extensionClass, Class<T> extensionPointType) {
        return "constructor for extension [" + extensionClass.getName() + "] of type [" + extensionPointType.getName() + "]";
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case users mess with jar files manually
    static void checkBundleJarHell(Set<URL> classpath, Bundle bundle, Map<String, Set<URL>> transitiveUrls) {
        // invariant: any plugins this plugin bundle extends have already been added to transitiveUrls
        List<String> exts = bundle.plugin.getExtendedPlugins();

        try {
            final Logger logger = LogManager.getLogger(JarHell.class);
            Set<URL> urls = new HashSet<>();
            for (String extendedPlugin : exts) {
                Set<URL> pluginUrls = transitiveUrls.get(extendedPlugin);
                if (pluginUrls == null && bundle.plugin.isExtendedPluginOptional(extendedPlugin)) {
                    continue;
                }
                assert pluginUrls != null : "transitive urls should have already been set for " + extendedPlugin;

                Set<URL> intersection = new HashSet<>(urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException(
                        "jar hell! extended plugins " + exts + " have duplicate codebases with each other: " + intersection
                    );
                }

                intersection = new HashSet<>(bundle.urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException(
                        "jar hell! duplicate codebases with extended plugin [" + extendedPlugin + "]: " + intersection
                    );
                }

                urls.addAll(pluginUrls);
                JarHell.checkJarHell(urls, logger::debug); // check jarhell as we add each extended plugin's urls
            }

            urls.addAll(bundle.urls);
            JarHell.checkJarHell(urls, logger::debug); // check jarhell of each extended plugin against this plugin
            transitiveUrls.put(bundle.plugin.getName(), urls);

            // check we don't have conflicting codebases with core
            Set<URL> intersection = new HashSet<>(classpath);
            intersection.retainAll(bundle.urls);
            if (intersection.isEmpty() == false) {
                throw new IllegalStateException("jar hell! duplicate codebases between plugin and core: " + intersection);
            }
            // check we don't have conflicting classes
            Set<URL> union = new HashSet<>(classpath);
            union.addAll(bundle.urls);
            JarHell.checkJarHell(union, logger::debug);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " due to jar hell", e);
        }
    }

    @SuppressWarnings("removal")
    private Plugin loadBundle(Bundle bundle, Map<String, Plugin> loaded) {
        String name = bundle.plugin.getName();

        verifyCompatibility(bundle.plugin);

        // collect loaders of extended plugins
        List<ClassLoader> extendedLoaders = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            Plugin extendedPlugin = loaded.get(extendedPluginName);
            if (extendedPlugin == null && bundle.plugin.isExtendedPluginOptional(extendedPluginName)) {
                // extended plugin is optional and is not installed
                continue;
            }
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isInstance(extendedPlugin) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            extendedLoaders.add(extendedPlugin.getClass().getClassLoader());
        }

        // create a child to load the plugin in this bundle
        ClassLoader parentLoader = PluginLoaderIndirection.createLoader(getClass().getClassLoader(), extendedLoaders);
        ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), parentLoader);

        // reload SPI with any new services from the plugin
        reloadLuceneSPI(loader);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            // Set context class loader to plugin's class loader so that plugins
            // that have dependencies with their own SPI endpoints have a chance to load
            // and initialize them appropriately.
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                Thread.currentThread().setContextClassLoader(loader);
                return null;
            });

            logger.debug("Loading plugin [" + name + "]...");
            Class<? extends Plugin> pluginClass = loadPluginClass(bundle.plugin.getClassname(), loader);
            if (loader != pluginClass.getClassLoader()) {
                // For dynamic plugin loading, we're more lenient about class loader validation
                // Log a warning but allow the plugin to load if the class is available
                logger.warn("Plugin [{}] class [{}] was loaded by class loader [{}] instead of expected plugin class loader [{}]. " +
                           "This may indicate the plugin class is already available in the system classpath.",
                           name, bundle.plugin.getClassname(), pluginClass.getClassLoader(), loader);
                
                // Only throw an exception if the class loader is completely incompatible
                // (e.g., null class loader which indicates bootstrap class loader)
                if (pluginClass.getClassLoader() == null) {
                    throw new IllegalStateException(
                        "Plugin ["
                            + name
                            + "] class ["
                            + bundle.plugin.getClassname()
                            + "] was loaded by bootstrap class loader, which is not allowed for plugins"
                    );
                }
            }
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            loaded.put(name, plugin);
            return plugin;
        } finally {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                Thread.currentThread().setContextClassLoader(cl);
                return null;
            });
        }
    }

    /**
     * Reloads all Lucene SPI implementations using the new classloader.
     * This method must be called after the new classloader has been created to
     * register the services for use.
     */
    static void reloadLuceneSPI(ClassLoader loader) {
        // do NOT change the order of these method calls!

        // Codecs:
        PostingsFormat.reloadPostingsFormats(loader);
        DocValuesFormat.reloadDocValuesFormats(loader);
        KnnVectorsFormat.reloadKnnVectorsFormat(loader);
        Codec.reloadCodecs(loader);
    }

    private Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) {
        try {
            // Force loading from the specific plugin class loader, not parent
            // Use true for initialize parameter to ensure proper class initialization
            Class<?> clazz = Class.forName(className, true, loader);
            
            // If the class was loaded by a parent class loader instead of our plugin loader,
            // we need to handle this case for dynamic plugin loading
            if (clazz.getClassLoader() != loader && clazz.getClassLoader() != null) {
                logger.warn("Plugin class [{}] was loaded by parent class loader [{}] instead of plugin class loader [{}]. " +
                           "This may indicate the plugin class is already available in the system classpath.", 
                           className, clazz.getClassLoader(), loader);
                
                // For dynamic loading, we'll allow this but log a warning
                // In a production system, you might want to be more strict
            }
            
            return clazz.asSubclass(Plugin.class);
        } catch (Throwable t) {
            throw new OpenSearchException("Unable to load plugin class [" + className + "]", t);
        }
    }

    private Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings, Path configPath) {
        final Constructor<?>[] constructors = pluginClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + pluginClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + pluginClass.getName() + "]");
        }

        final Constructor<?> constructor = constructors[0];
        if (constructor.getParameterCount() > 2) {
            throw new IllegalStateException(signatureMessage(pluginClass));
        }

        final Class[] parameterTypes = constructor.getParameterTypes();
        try {
            if (constructor.getParameterCount() == 2 && parameterTypes[0] == Settings.class && parameterTypes[1] == Path.class) {
                return (Plugin) constructor.newInstance(settings, configPath);
            } else if (constructor.getParameterCount() == 1 && parameterTypes[0] == Settings.class) {
                return (Plugin) constructor.newInstance(settings);
            } else if (constructor.getParameterCount() == 0) {
                return (Plugin) constructor.newInstance();
            } else {
                throw new IllegalStateException(signatureMessage(pluginClass));
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private String signatureMessage(final Class<? extends Plugin> clazz) {
        return String.format(
            Locale.ROOT,
            "no public constructor of correct signature for [%s]; must be [%s], [%s], or [%s]",
            clazz.getName(),
            "(org.opensearch.common.settings.Settings,java.nio.file.Path)",
            "(org.opensearch.common.settings.Settings)",
            "()"
        );
    }

    public <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.v2().getClass())).map(p -> ((T) p.v2())).collect(Collectors.toList());
    }

    /**
     * Register a dynamically loaded plugin with the system so it appears in _cat/plugins
     * 
     * @param pluginInfo The plugin information
     * @param plugin The plugin instance
     */
    private synchronized void registerDynamicPlugin(PluginInfo pluginInfo, Plugin plugin) {
        try {
            // Check if plugin is already registered to prevent duplicates
            boolean pluginExists = false;
            for (Tuple<PluginInfo, Plugin> existingPlugin : plugins) {
                if (existingPlugin.v1().getName().equals(pluginInfo.getName())) {
                    pluginExists = true;
                    logger.warn("Plugin [{}] is already registered. Skipping duplicate registration.", pluginInfo.getName());
                    break;
                }
            }
            
            if (!pluginExists) {
                // Create new plugin tuple
                Tuple<PluginInfo, Plugin> pluginTuple = new Tuple<>(pluginInfo, plugin);
                
                // Create new plugins list with the dynamically loaded plugin
                List<Tuple<PluginInfo, Plugin>> newPluginsList = new ArrayList<>(plugins);
                newPluginsList.add(pluginTuple);
                
                // Update the plugins list
                this.plugins = Collections.unmodifiableList(newPluginsList);
                
                // Create new plugin info list for PluginsAndModules
                List<PluginInfo> newPluginInfoList = new ArrayList<>(info.getPluginInfos());
                newPluginInfoList.add(pluginInfo);
                
                // Update the info object
                this.info = new PluginsAndModules(newPluginInfoList, info.getModuleInfos());
                
                logger.info("Successfully registered dynamically loaded plugin [{}] with the system", pluginInfo.getName());
            }
        } catch (Exception e) {
            logger.warn("Failed to register dynamically loaded plugin [{}] with the system: {}", pluginInfo.getName(), e.getMessage(), e);
        }
    }

    /**
     * Dynamically load a plugin from the specified path
     *
     * @param pluginPath The path to the plugin directory
     * @return The loaded plugin instance
     * @throws IOException if plugin loading fails
     */
    @SuppressWarnings("removal")
    public Plugin loadPluginDynamically(Path pluginPath) throws IOException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Plugin>) () -> {
                if (!Files.exists(pluginPath) || !Files.isDirectory(pluginPath)) {
                    throw new IllegalArgumentException("Plugin path does not exist or is not a directory: " + pluginPath);
                }

                // Read plugin bundle from the specified path
                Set<Bundle> bundles = new HashSet<>();
                Bundle bundle = readPluginBundle(bundles, pluginPath, "plugin");
                bundles.add(bundle);

                // Load the plugin
                Map<String, Plugin> loaded = new HashMap<>();
                // Add existing plugins to the loaded map to handle dependencies
                for (Tuple<PluginInfo, Plugin> existingPlugin : plugins) {
                    loaded.put(existingPlugin.v1().getName(), existingPlugin.v2());
                }

                Plugin plugin = loadBundle(bundle, loaded);

                // Register the dynamically loaded plugin with the system
                registerDynamicPlugin(bundle.plugin, plugin);

                return plugin;
            });
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new IOException("Failed to load plugin dynamically", cause);
            }
        }
    }

    /**
     * Dynamically load a plugin by name from the plugins directory
     *
     * @param pluginName The name of the plugin to load
     * @param pluginsDirectory The plugins directory to search in
     * @return The loaded plugin instance
     * @throws IOException if plugin loading fails
     */
    @SuppressWarnings("removal")
    public Plugin loadPluginByName(String pluginName, Path pluginsDirectory) throws IOException {
        return AccessController.doPrivileged((PrivilegedAction<Plugin>) () -> {
            try {
                if (pluginsDirectory == null || !Files.exists(pluginsDirectory)) {
                    throw new IllegalArgumentException("Plugins directory does not exist: " + pluginsDirectory);
                }

                Path pluginPath = pluginsDirectory.resolve(pluginName);
                return loadPluginDynamically(pluginPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load plugin by name", e);
            }
        });
    }

    /**
     * Dynamically unload a plugin by name
     *
     * @param pluginName The name of the plugin to unload
     * @return true if the plugin was successfully unloaded, false if not found
     */
    public synchronized boolean unloadPluginDynamically(String pluginName) {
        try {
            // Find the plugin to unload
            Tuple<PluginInfo, Plugin> pluginToUnload = null;
            for (Tuple<PluginInfo, Plugin> plugin : plugins) {
                if (plugin.v1().getName().equals(pluginName) || 
                    plugin.v2().getClass().getSimpleName().equals(pluginName)) {
                    pluginToUnload = plugin;
                    break;
                }
            }

            if (pluginToUnload == null) {
                logger.warn("Plugin [{}] not found for unloading", pluginName);
                return false;
            }

            // Create new plugins list without the unloaded plugin
            List<Tuple<PluginInfo, Plugin>> newPluginsList = new ArrayList<>();
            for (Tuple<PluginInfo, Plugin> plugin : plugins) {
                if (!plugin.equals(pluginToUnload)) {
                    newPluginsList.add(plugin);
                }
            }

            // Update the plugins list
            this.plugins = Collections.unmodifiableList(newPluginsList);

            // Create new plugin info list for PluginsAndModules
            List<PluginInfo> newPluginInfoList = new ArrayList<>();
            for (PluginInfo pluginInfo : info.getPluginInfos()) {
                if (!pluginInfo.equals(pluginToUnload.v1())) {
                    newPluginInfoList.add(pluginInfo);
                }
            }

            // Update the info object
            this.info = new PluginsAndModules(newPluginInfoList, info.getModuleInfos());

            // Attempt to close class loader if it's a URLClassLoader
            try {
                ClassLoader pluginClassLoader = pluginToUnload.v2().getClass().getClassLoader();
                if (pluginClassLoader instanceof URLClassLoader) {
                    ((URLClassLoader) pluginClassLoader).close();
                    logger.debug("Closed class loader for plugin [{}]", pluginName);
                }
            } catch (Exception e) {
                logger.warn("Failed to close class loader for plugin [{}]: {}", pluginName, e.getMessage());
            }

            logger.info("Successfully unloaded plugin [{}]", pluginName);
            return true;

        } catch (Exception e) {
            logger.error("Failed to unload plugin [{}]: {}", pluginName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Get a plugin by name for unloading purposes
     *
     * @param pluginName The name of the plugin to find
     * @return The plugin tuple if found, null otherwise
     */
    public Tuple<PluginInfo, Plugin> getPluginByName(String pluginName) {
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            if (plugin.v1().getName().equals(pluginName) || 
                plugin.v2().getClass().getSimpleName().equals(pluginName)) {
                return plugin;
            }
        }
        return null;
    }
    
    /**
     * Update the load status of a plugin
     *
     * @param pluginName The name of the plugin
     * @param status The new load status
     */
    public void updatePluginLoadStatus(String pluginName, PluginLoadStatus status) {
        pluginLoadStatusMap.put(pluginName, status);
        logger.debug("Updated plugin [{}] load status to [{}]", pluginName, status.getDisplayName());
    }
    
    /**
     * Get the load status of a plugin
     *
     * @param pluginName The name of the plugin
     * @return The load status, or INITIAL if not found
     */
    public PluginLoadStatus getPluginLoadStatus(String pluginName) {
        return pluginLoadStatusMap.getOrDefault(pluginName, PluginLoadStatus.INITIAL);
    }
    
    /**
     * Get all plugins with their load status information
     *
     * @return List of PluginInfoWithStatus objects
     */
    public List<PluginInfoWithStatus> getPluginsWithStatus() {
        List<PluginInfoWithStatus> pluginsWithStatus = new ArrayList<>();
        for (PluginInfo pluginInfo : info.getPluginInfos()) {
            PluginLoadStatus status = getPluginLoadStatus(pluginInfo.getName());
            pluginsWithStatus.add(new PluginInfoWithStatus(pluginInfo, status));
        }
        return pluginsWithStatus;
    }
    
    /**
     * Mark a plugin as loaded via the load API
     *
     * @param pluginName The name of the plugin
     */
    public void markPluginAsLoaded(String pluginName) {
        updatePluginLoadStatus(pluginName, PluginLoadStatus.LOADED);
    }
    
    /**
     * Mark a plugin as active via the register API
     *
     * @param pluginName The name of the plugin
     */
    public void markPluginAsActive(String pluginName) {
        updatePluginLoadStatus(pluginName, PluginLoadStatus.ACTIVE);
    }
}
