/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensible.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Identity;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ExtensibleIdentityPlugin extends Plugin implements ExtensiblePlugin {
    private volatile Logger log = LogManager.getLogger(this.getClass());

    private static final String DEFAULT_AUTHENTICATION_MANAGER_CLASS =
        "org.opensearch.identity.authmanager.internal.InternalAuthenticationManager";

    private static final String NOOP_AUTHENTICATION_MANAGER_CLASS = "org.opensearch.identity.authmanager.noop.NoopAuthenticationManager";

    private volatile Map<String, AuthenticationManager> availableAuthManagers;
    private volatile Settings settings;

    private volatile Path configPath;

    @SuppressWarnings("removal")
    public ExtensibleIdentityPlugin(final Settings settings, final Path configPath) {
        this.configPath = configPath;

        if (this.configPath != null) {
            log.info("OpenSearch Config path is {}", this.configPath.toAbsolutePath());
        } else {
            log.info("OpenSearch Config path is not set");
        }

        this.settings = settings;
    }

    private static boolean isEnabled(final Settings settings) {
        return settings.getAsBoolean(ConfigConstants.IDENTITY_ENABLED, false);
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<Setting<?>>();
        settings.addAll(super.getSettings());
        settings.add(Setting.boolSetting(ConfigConstants.IDENTITY_ENABLED, false, Setting.Property.NodeScope, Setting.Property.Filtered));
        settings.add(
            Setting.simpleString(
                ConfigConstants.IDENTITY_AUTH_MANAGER_CLASS,
                DEFAULT_AUTHENTICATION_MANAGER_CLASS,
                Setting.Property.NodeScope,
                Setting.Property.Filtered
            )
        );

        return settings;
    }

    @Override
    public Collection<Object> createComponents(
        Client localClient,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.configPath = environment.configDir();
        this.settings = environment.settings();

        if (availableAuthManagers == null || availableAuthManagers.isEmpty()) {
            throw new OpenSearchException("No available authentication managers");
        }

        // TODO: revisit this
        final String authManagerClassName;

        if (isEnabled(settings)) {
            authManagerClassName = this.settings.get(ConfigConstants.IDENTITY_AUTH_MANAGER_CLASS, DEFAULT_AUTHENTICATION_MANAGER_CLASS);
        } else {
            authManagerClassName = NOOP_AUTHENTICATION_MANAGER_CLASS;
        }

        AuthenticationManager authManager = availableAuthManagers.get(authManagerClassName);
        if (authManager == null) {
            throw new OpenSearchException("Auth Manager with class " + authManagerClassName + " cannot be found");
        }

        try {
            Method method = authManager.getClass().getMethod("setThreadPool", ThreadPool.class);
            method.invoke(authManager, threadPool);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            /** ignore */
        }

        Identity.setAuthManager(authManager);

        return Collections.emptyList();
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<AuthenticationManager> authManagerImplementations = loader.loadExtensions(AuthenticationManager.class);
        if (authManagerImplementations != null && authManagerImplementations.size() > 0) {
            availableAuthManagers = new HashMap<>();
            for (AuthenticationManager authManager : authManagerImplementations) {
                availableAuthManagers.put(authManager.getClass().getCanonicalName(), authManager);
            }
        }
    }
}
