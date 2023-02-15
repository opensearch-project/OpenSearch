/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.EventBusBuilder;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.User;
import org.opensearch.identity.configuration.model.InternalUsersModel;
import org.opensearch.identity.extensions.ExtensionSecurityConfigStore;
import org.opensearch.identity.realm.InternalUsersStore;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamicConfigFactory implements ConfigurationChangeListener {

    public static final EventBusBuilder EVENT_BUS_BUILDER = EventBus.builder();

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ConfigurationRepository cr;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final EventBus eventBus = EVENT_BUS_BUILDER.build();
    private final Settings opensearchSettings;
    private final Path configPath;

    SecurityDynamicConfiguration<?> config;

    public DynamicConfigFactory(
        ConfigurationRepository cr,
        final Settings opensearchSettings,
        final Path configPath,
        Client client,
        ThreadPool threadPool,
        ClusterInfoHolder cih
    ) {
        super();
        this.cr = cr;
        this.opensearchSettings = opensearchSettings;
        this.configPath = configPath;

        registerDCFListener(InternalUsersStore.getInstance());
        registerDCFListener(ExtensionSecurityConfigStore.getInstance());
        this.cr.subscribeOnChange(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onChange(Map<CType, SecurityDynamicConfiguration<?>> typeToConfig) {

        SecurityDynamicConfiguration<?> internalusers = cr.getConfiguration(CType.INTERNALUSERS);

        if (log.isDebugEnabled()) {
            String logmsg = "current config (because of "
                + typeToConfig.keySet()
                + ")\n"
                + " internalusers: "
                + internalusers.getImplementingClass()
                + " with "
                + internalusers.getCEntries().size()
                + " entries\n";
            log.debug(logmsg);

        }

        final InternalUsersModel ium = new InternalUsersModel((SecurityDynamicConfiguration<User>) internalusers);

        eventBus.post(ium);
        eventBus.post(cr.getConfiguration(CType.EXTENSIONSECURITY));

        if (!isInitialized()) {
            initialized.set(true);
            cr.initializeExtensionsSecurity();
        }

    }

    public final boolean isInitialized() {
        return initialized.get();
    }

    public void registerDCFListener(Object listener) {
        eventBus.register(listener);
    }

    public void unregisterDCFListener(Object listener) {
        eventBus.unregister(listener);
    }
}
