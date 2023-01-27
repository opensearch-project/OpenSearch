/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.authn.DefaultObjectMapper;
import org.opensearch.authn.realm.InternalRealm;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.exception.ConfigUpdateAlreadyInProgressException;
import org.opensearch.identity.exception.ExceptionUtils;
import org.opensearch.identity.exception.InvalidConfigException;
import org.opensearch.identity.support.ConfigHelper;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.opensearch.OpenSearchException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.env.Environment;
//import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.identity.utils.Hasher;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.threadpool.ThreadPool;

/**
 * This class loads identity configuration on startup and initializes the identity system index if it does not exist
 */
public class ConfigurationRepository {
    private static final Logger LOGGER = LogManager.getLogger(ConfigurationRepository.class);

    private final String identityIndex;
    private final Client client;
    private final Map<CType, SecurityDynamicConfiguration<?>> configCache = new HashMap<>();
    private final List<ConfigurationChangeListener> configurationChangedListener;
    private final ConfigurationLoader cl;
    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private DynamicConfigFactory dynamicConfigFactory;
    private static final int DEFAULT_CONFIG_VERSION = 1;
    private final Thread bgThread;
    private final AtomicBoolean installDefaultConfig = new AtomicBoolean();

    private ConfigurationRepository(
        Settings settings,
        final Path configPath,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        this.identityIndex = settings.get(ConfigConstants.IDENTITY_CONFIG_INDEX_NAME, ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
        this.settings = settings;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.configurationChangedListener = new ArrayList<>();
        cl = new ConfigurationLoader(client, threadPool, settings, clusterService);

        bgThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Background init thread started. Install default config?: " + installDefaultConfig.get());

                    if (installDefaultConfig.get()) {

                        try {
                            String lookupDir = System.getProperty("identity.default_init.dir");
                            final String cd = lookupDir != null
                                ? (lookupDir + "/")
                                : new Environment(settings, configPath).configDir().toAbsolutePath().toString() + "/";
                            Path confFile = Path.of(cd + "internal_users.yml");
                            if (Files.exists(confFile)) {
                                final ThreadContext threadContext = threadPool.getThreadContext();
                                try (StoredContext ctx = threadContext.stashContext()) {
                                    threadContext.putHeader(ConfigConstants.IDENTITY_CONF_REQUEST_HEADER, "true");

                                    createSecurityIndexIfAbsent();
                                    waitForSecurityIndexToBeAtLeastYellow();


                                    String adminPassword = ConfigHelper.generateCommonLangPassword();
                                    LOGGER.always().log("Generated a random admin password: {}", adminPassword);
                                    char [] clearAdminPassword = adminPassword.toCharArray();
                                    String hashedAdminPassword = Hasher.hash(clearAdminPassword);
                                    Map<String, Object> adminUserMap = Map.of("admin", Map.of("hash", hashedAdminPassword));
                                    final IndexRequest indexRequest = new IndexRequest(identityIndex).id(CType.INTERNALUSERS.toLCString())
                                        .opType(DocWriteRequest.OpType.INDEX)
                                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                        .source(CType.INTERNALUSERS.toLCString(), ConfigHelper.readXContent(new StringReader(DefaultObjectMapper.writeValueAsString(adminUserMap, false)), XContentType.JSON));
                                    IndexResponse response = client.index(indexRequest).actionGet();
                                    System.out.println(response);
                                    InternalRealm.INSTANCE.createUser("admin", hashedAdminPassword, null);

//                                    ConfigHelper.uploadFile(
//                                        client,
//                                        cd + "internal_users.yml",
//                                        identityIndex,
//                                        CType.INTERNALUSERS,
//                                        DEFAULT_CONFIG_VERSION
//                                    );
                                }
                            } else {
                                LOGGER.error("{} does not exist", confFile.toAbsolutePath().toString());
                            }
                        } catch (VersionConflictEngineException versionConflictEngineException) {
                            LOGGER.info("Index {} already contains doc with id {}, skipping update.", identityIndex, CType.INTERNALUSERS.toLCString());
                        } catch (Exception e) {
                            LOGGER.error("Cannot apply default config (this is maybe not an error!)", e);
                        }
                        //TODO: merge the admin into internal_user config
                    }

                    while (!dynamicConfigFactory.isInitialized()) {
                        try {
                            LOGGER.debug("Try to load config ...");
                            reloadConfiguration(Arrays.asList(CType.values()));
                            break;
                        } catch (Exception e) {
                            LOGGER.debug("Unable to load configuration due to {}", String.valueOf(ExceptionUtils.getRootCause(e)));
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                                Thread.currentThread().interrupt();
                                LOGGER.debug("Thread was interrupted so we cancel initialization");
                                break;
                            }
                        }
                    }

                    LOGGER.info("Node '{}' initialized", clusterService.localNode().getName());

                } catch (Exception e) {
                    LOGGER.error("Unexpected exception while initializing node " + e, e);
                }
            }
        });

    }

    private boolean createSecurityIndexIfAbsent() {
        try {
            final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(identityIndex).settings(indexSettings);
            final boolean ok = client.admin().indices().create(createIndexRequest).actionGet().isAcknowledged();
            LOGGER.info("Index {} created?: {}", identityIndex, ok);
            return ok;
        } catch (ResourceAlreadyExistsException resourceAlreadyExistsException) {
            LOGGER.info("Index {} already exists", identityIndex);
            return false;
        }
    }

    private void waitForSecurityIndexToBeAtLeastYellow() {
        LOGGER.info("Node started, try to initialize it. Wait for at least yellow cluster state....");
        ClusterHealthResponse response = null;
        try {
            response = client.admin()
                .cluster()
                .health(new ClusterHealthRequest(identityIndex).waitForActiveShards(1).waitForYellowStatus())
                .actionGet();
        } catch (Exception e) {
            LOGGER.debug("Caught a {} but we just try again ...", e.toString());
        }

        while (response == null || response.isTimedOut() || response.getStatus() == ClusterHealthStatus.RED) {
            LOGGER.debug(
                "index '{}' not healthy yet, we try again ... (Reason: {})",
                identityIndex,
                response == null ? "no response" : (response.isTimedOut() ? "timeout" : "other, maybe red cluster")
            );
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // ignore
                Thread.currentThread().interrupt();
            }
            try {
                response = client.admin().cluster().health(new ClusterHealthRequest(identityIndex).waitForYellowStatus()).actionGet();
            } catch (Exception e) {
                LOGGER.debug("Caught again a {} but we just try again ...", e.toString());
            }
        }
    }

    public void initOnNodeStart() {
        try {
            if (settings.getAsBoolean(ConfigConstants.IDENTITY_ALLOW_DEFAULT_INIT_SECURITYINDEX, true)) {
                LOGGER.info("Will attempt to create index {} and default configs if they are absent", identityIndex);
                installDefaultConfig.set(true);
                bgThread.start();
            } else {
                LOGGER.info(
                    "Will not attempt to create index {} and default configs if they are absent. Will not perform background initialization",
                    identityIndex
                );
            }
        } catch (Throwable e2) {
            LOGGER.error("Error during node initialization", e2);
            bgThread.start();
        }
    }

    public static ConfigurationRepository create(
        Settings settings,
        final Path configPath,
        final ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        final ConfigurationRepository repository = new ConfigurationRepository(settings, configPath, threadPool, client, clusterService);
        return repository;
    }

    public void setDynamicConfigFactory(DynamicConfigFactory dynamicConfigFactory) {
        this.dynamicConfigFactory = dynamicConfigFactory;
    }

    public SecurityDynamicConfiguration<?> getConfiguration(CType configurationType) {
        SecurityDynamicConfiguration<?> conf = configCache.get(configurationType);
        if (conf != null) {
            return conf.deepClone();
        }
        return SecurityDynamicConfiguration.empty();
    }

    private final Lock LOCK = new ReentrantLock();

    public void reloadConfiguration(Collection<CType> configTypes) throws ConfigUpdateAlreadyInProgressException {
        try {
            if (LOCK.tryLock(60, TimeUnit.SECONDS)) {
                try {
                    reloadConfiguration0(configTypes);
                } finally {
                    LOCK.unlock();
                }
            } else {
                throw new ConfigUpdateAlreadyInProgressException("A config update is already in progress");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConfigUpdateAlreadyInProgressException("Interrupted config update");
        }
    }

    private void reloadConfiguration0(Collection<CType> configTypes) {
        final Map<CType, SecurityDynamicConfiguration<?>> loaded = getConfigurationsFromIndex(configTypes);
        configCache.putAll(loaded);
        notifyAboutChanges(loaded);
    }

    public synchronized void subscribeOnChange(ConfigurationChangeListener listener) {
        configurationChangedListener.add(listener);
    }

    private synchronized void notifyAboutChanges(Map<CType, SecurityDynamicConfiguration<?>> typeToConfig) {
        for (ConfigurationChangeListener listener : configurationChangedListener) {
            try {
                LOGGER.debug("Notify {} listener about change configuration", listener);
                listener.onChange(typeToConfig);
            } catch (Exception e) {
                String errorMsg = String.format(Locale.ROOT, "%s listener errored", listener);
                LOGGER.error(errorMsg, e);
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    public Map<CType, SecurityDynamicConfiguration<?>> getConfigurationsFromIndex(Collection<CType> configTypes) {

        final ThreadContext threadContext = threadPool.getThreadContext();
        final Map<CType, SecurityDynamicConfiguration<?>> retVal = new HashMap<>();

        try (StoredContext ctx = threadContext.stashContext()) {
            threadContext.putHeader(ConfigConstants.IDENTITY_CONF_REQUEST_HEADER, "true");

            IndexMetadata securityMetadata = clusterService.state().metadata().index(this.identityIndex);
            MappingMetadata mappingMetadata = securityMetadata == null ? null : securityMetadata.mapping();

            if (securityMetadata != null && mappingMetadata != null) {
                LOGGER.debug("identity index exists");
                retVal.putAll(validate(cl.load(configTypes.toArray(new CType[0]), 5, TimeUnit.SECONDS), configTypes.size()));

            } else {
                // wait (and use new layout)
                LOGGER.debug("identity index not exists (yet)");
                retVal.putAll(validate(cl.load(configTypes.toArray(new CType[0]), 5, TimeUnit.SECONDS), configTypes.size()));
            }

        } catch (Exception e) {
            throw new OpenSearchException(e);
        }

        return retVal;
    }

    private Map<CType, SecurityDynamicConfiguration<?>> validate(Map<CType, SecurityDynamicConfiguration<?>> conf, int expectedSize)
        throws InvalidConfigException {

        if (conf == null || conf.size() != expectedSize) {
            throw new InvalidConfigException("Retrieved only partial configuration");
        }

        return conf;
    }
}
