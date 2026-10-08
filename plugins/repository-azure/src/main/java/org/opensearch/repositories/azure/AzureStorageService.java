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

package org.opensearch.repositories.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.implementation.connectionstring.StorageEndpoint;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.Mono;

import static java.util.Collections.emptyMap;

public class AzureStorageService implements AutoCloseable {
    private final ClientLogger logger = new ClientLogger(AzureStorageService.class);

    /**
     * Maximum blob's block size size
     */
    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * Maximum allowed blob's block size in Azure blob store.
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(
        BlockBlobAsyncClient.MAX_STAGE_BLOCK_BYTES_LONG,
        ByteSizeUnit.BYTES
    );

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();
    private final Map<AzureStorageSettings, ClientState> clients = new ConcurrentHashMap<>();
    private final Map<String, PrimaryClientState> primaryClients = new ConcurrentHashMap<>();
    private final Object clientLifecycleMutex = new Object();
    private final ExecutorService executor;
    private boolean closed;

    private static final class IdentityClientThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        @SuppressWarnings("removal")
        IdentityClientThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(
                group,
                () -> AccessController.doPrivileged(r),
                namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
                0
            );
            t.setDaemon(true);
            return t;
        }
    }

    static {
        // See please:
        // - https://github.com/Azure/azure-sdk-for-java/issues/24373
        // - https://github.com/Azure/azure-sdk-for-java/pull/25004
        // - https://github.com/Azure/azure-sdk-for-java/pull/24374
        Configuration.getGlobalConfiguration().put("AZURE_JACKSON_ADAPTER_USE_ACCESS_HELPER", "true");
        // See please:
        // - https://github.com/Azure/azure-sdk-for-java/issues/37464
        Configuration.getGlobalConfiguration().put("AZURE_ENABLE_SHUTDOWN_HOOK_WITH_PRIVILEGE", "true");
    }

    public AzureStorageService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshAndClearCache(clientsSettings);
        executor = AccessController.doPrivileged(
            () -> Executors.newCachedThreadPool(new IdentityClientThreadFactory("azure-identity-client"))
        );
    }

    /**
     * Obtains a {@code BlobServiceClient} on each invocation using the current client
     * settings. BlobServiceClient is thread safe and could be cached but the settings
     * can change, therefore the instance might be recreated from scratch.
     *
     * @param clientName client name
     * @return the {@code BlobServiceClient} instance and context
     */
    public Tuple<BlobServiceClient, Supplier<Context>> client(String clientName) {
        return client(clientName, (request, response) -> {});

    }

    /**
     * Obtains a {@code BlobServiceClient} on each invocation using the current client
     * settings. BlobServiceClient is thread safe and could be cached but the settings
     * can change, therefore the instance might be recreated from scratch.

     * @param clientName client name
     * @param statsCollector statistics collector
     * @return the {@code BlobServiceClient} instance and context
     */
    public Tuple<BlobServiceClient, Supplier<Context>> client(String clientName, BiConsumer<HttpRequest, HttpResponse> statsCollector) {
        final AzureStorageSettings azureStorageSettings = getStorageSettings(clientName);

        // New Azure storage clients are thread-safe and do not hold any state so could be cached, see please:
        // https://github.com/Azure/azure-storage-java/blob/master/V12%20Upgrade%20Story.md#v12-the-best-of-both-worlds
        ClientState state = clients.get(azureStorageSettings);

        if (state == null) {
            state = clients.computeIfAbsent(azureStorageSettings, key -> {
                try {
                    return buildClient(azureStorageSettings, statsCollector);
                } catch (InvalidKeyException | URISyntaxException | IllegalArgumentException e) {
                    throw new SettingsException("Invalid azure client settings with name [" + clientName + "]", e);
                }
            });
        }

        return new Tuple<>(state.getClient(), () -> buildOperationContext(azureStorageSettings));
    }

    Tuple<BlobServiceClient, Supplier<Context>> clientForPrimaryOnly(String clientName) {
        synchronized (clientLifecycleMutex) {
            if (closed) {
                throw new IllegalStateException("Azure storage service is closed");
            }
            final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
            if (azureStorageSettings == null) {
                throw new SettingsException("Unable to find client with name [" + clientName + "]");
            }
            PrimaryClientState primaryState = primaryClients.get(clientName);
            if (primaryState == null || primaryState.sourceSettings != azureStorageSettings) {
                if (primaryState != null) {
                    closeInternally(primaryState.clientState);
                }
                final AzureStorageSettings primarySettings = AzureStorageSettings.overrideLocationMode(
                    Collections.singletonMap(clientName, azureStorageSettings),
                    LocationMode.PRIMARY_ONLY
                ).get(clientName);
                try {
                    primaryState = new PrimaryClientState(
                        azureStorageSettings,
                        primarySettings,
                        buildClient(primarySettings, (request, response) -> {})
                    );
                    primaryClients.put(clientName, primaryState);
                } catch (InvalidKeyException | URISyntaxException | IllegalArgumentException e) {
                    primaryClients.remove(clientName);
                    throw new SettingsException("Invalid azure client settings with name [" + clientName + "]", e);
                }
            }
            final PrimaryClientState selectedState = primaryState;
            return new Tuple<>(selectedState.clientState.getClient(), () -> buildOperationContext(selectedState.primarySettings));
        }
    }

    private ClientState buildClient(AzureStorageSettings azureStorageSettings, BiConsumer<HttpRequest, HttpResponse> statsCollector)
        throws InvalidKeyException, URISyntaxException {
        final BlobServiceClientBuilder builder = createClientBuilder(azureStorageSettings);
        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(new NioThreadFactory());
        final NettyAsyncHttpClientBuilder clientBuilder = new NettyAsyncHttpClientBuilder().eventLoopGroup(eventLoopGroup);

        AccessController.doPrivilegedChecked(() -> {
            final ProxySettings proxySettings = azureStorageSettings.getProxySettings();
            if (proxySettings != ProxySettings.NO_PROXY_SETTINGS) {
                final ProxyOptions proxyOptions = new ProxyOptions(proxySettings.getType().toProxyType(), proxySettings.getAddress());
                if (proxySettings.isAuthenticated()) {
                    proxyOptions.setCredentials(proxySettings.getUsername(), proxySettings.getPassword());
                }
                clientBuilder.proxy(proxyOptions);
            }
        });

        final TimeValue connectTimeout = azureStorageSettings.getConnectTimeout();
        if (connectTimeout != null) {
            clientBuilder.connectTimeout(Duration.ofMillis(connectTimeout.millis()));
        }

        final TimeValue writeTimeout = azureStorageSettings.getWriteTimeout();
        if (writeTimeout != null) {
            clientBuilder.writeTimeout(Duration.ofMillis(writeTimeout.millis()));
        }

        final TimeValue readTimeout = azureStorageSettings.getReadTimeout();
        if (readTimeout != null) {
            clientBuilder.readTimeout(Duration.ofMillis(readTimeout.millis()));
        }

        final TimeValue responseTimeout = azureStorageSettings.getResponseTimeout();
        if (responseTimeout != null) {
            clientBuilder.responseTimeout(Duration.ofMillis(responseTimeout.millis()));
        }

        builder.httpClient(clientBuilder.build());

        // We define a default exponential retry policy
        return new ClientState(
            applyLocationMode(builder, azureStorageSettings).addPolicy(new HttpStatsPolicy(statsCollector)).buildClient(),
            eventLoopGroup
        );
    }

    /**
     * The location mode is not there in v12 APIs anymore but it is possible to mimic its semantics using
     * retry options and combination of primary / secondary endpoints. Refer to
     * <a href="https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/storage/azure-storage-blob/migrationGuides/V8_V12.md#miscellaneous">migration guide</a> for mode details:
     */
    private BlobServiceClientBuilder applyLocationMode(final BlobServiceClientBuilder builder, final AzureStorageSettings settings) {
        final StorageEndpoint endpoint = settings.getStorageEndpoint(logger);

        if (endpoint == null || endpoint.getPrimaryUri() == null) {
            throw new IllegalArgumentException("connectionString missing required settings to derive blob service primary endpoint.");
        }

        final LocationMode locationMode = settings.getLocationMode();
        if (locationMode == LocationMode.PRIMARY_ONLY) {
            builder.retryOptions(createRetryPolicy(settings, null));
        } else if (locationMode == LocationMode.SECONDARY_ONLY) {
            if (endpoint.getSecondaryUri() == null) {
                throw new IllegalArgumentException("connectionString missing required settings to derive blob service secondary endpoint.");
            }

            builder.endpoint(endpoint.getSecondaryUri()).retryOptions(createRetryPolicy(settings, null));
        } else if (locationMode == LocationMode.PRIMARY_THEN_SECONDARY) {
            builder.retryOptions(createRetryPolicy(settings, endpoint.getSecondaryUri()));
        } else if (locationMode == LocationMode.SECONDARY_THEN_PRIMARY) {
            if (endpoint.getSecondaryUri() == null) {
                throw new IllegalArgumentException("connectionString missing required settings to derive blob service secondary endpoint.");
            }

            builder.endpoint(endpoint.getSecondaryUri()).retryOptions(createRetryPolicy(settings, endpoint.getPrimaryUri()));
        } else {
            throw new IllegalArgumentException("Unsupported location mode: " + locationMode);
        }

        return builder;
    }

    private BlobServiceClientBuilder createClientBuilder(AzureStorageSettings settings) throws InvalidKeyException, URISyntaxException {
        return AccessController.doPrivileged(() -> settings.configure(new BlobServiceClientBuilder(), executor, logger));
    }

    /**
     * For the time being, create an empty context but the implementation could be extended.
     * @param azureStorageSettings azure seetings
     * @return context instance
     */
    private static Context buildOperationContext(AzureStorageSettings azureStorageSettings) {
        return Context.NONE;
    }

    // non-static, package private for testing
    RequestRetryOptions createRetryPolicy(final AzureStorageSettings azureStorageSettings, String secondaryHost) {
        // We define a default exponential retry policy{
        return new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureStorageSettings.getMaxRetries(),
            (Integer) null,
            null,
            null,
            secondaryHost
        );
    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     * @return the old settings
     */
    public Map<String, AzureStorageSettings> refreshAndClearCache(Map<String, AzureStorageSettings> clientsSettings) {
        synchronized (clientLifecycleMutex) {
            if (closed) {
                throw new IllegalStateException("Azure storage service is closed");
            }
            final Map<String, AzureStorageSettings> prevSettings = this.storageSettings;
            final Map<AzureStorageSettings, ClientState> prevClients = new HashMap<>(this.clients);
            final Map<String, PrimaryClientState> prevPrimaryClients = new HashMap<>(this.primaryClients);
            prevClients.values().forEach(this::closeInternally);
            prevPrimaryClients.values().forEach(state -> closeInternally(state.clientState));
            prevClients.clear();
            prevPrimaryClients.clear();

            this.storageSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
            this.clients.clear();
            this.primaryClients.clear();

            // clients are built lazily by {@link client(String)}
            return prevSettings;
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (clientLifecycleMutex) {
            if (closed) {
                return;
            }
            closed = true;
            this.clients.values().forEach(this::closeInternally);
            this.primaryClients.values().forEach(state -> closeInternally(state.clientState));
            this.clients.clear();
            this.primaryClients.clear();
        }
        this.executor.shutdown();
        try {
            if (this.executor.awaitTermination(30, TimeUnit.SECONDS) == false) {
                this.executor.shutdownNow();
                logger.warning("The executor was not shutdown gracefully with 30 seconds");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException(ex);
        }
    }

    public Duration getBlobRequestTimeout(String clientName) {
        final AzureStorageSettings azureStorageSettings = getStorageSettings(clientName);

        // Set timeout option if the user sets cloud.azure.storage.timeout or
        // cloud.azure.storage.xxx.timeout (it's negative by default)
        final long timeout = azureStorageSettings.getTimeout().getMillis();

        if (timeout > 0) {
            if (timeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
            }

            return Duration.ofMillis(timeout);
        }

        return null;
    }

    @Nullable
    Integer getReadBlockSize(String clientName) {
        final long readBlockSize = getStorageSettings(clientName).getReadBlockSize().getBytes();
        return readBlockSize < 0L ? null : Math.toIntExact(readBlockSize);
    }

    @Nullable
    ParallelTransferOptions getBlobRequestOptionsForWriteBlob(String clientName) {
        final AzureStorageSettings settings = getStorageSettings(clientName);
        final long writeBlockSize = settings.getWriteBlockSize().getBytes();
        final long maxSingleUploadSize = settings.getMaxSingleUploadSize().getBytes();
        final int writeConcurrency = settings.getWriteConcurrency();

        if (writeBlockSize < 0L && maxSingleUploadSize < 0L && writeConcurrency < 0) {
            return null;
        }

        final ParallelTransferOptions options = new ParallelTransferOptions();
        if (writeBlockSize >= 0L) {
            options.setBlockSizeLong(writeBlockSize);
        }
        if (maxSingleUploadSize >= 0L) {
            options.setMaxSingleUploadSizeLong(maxSingleUploadSize);
        }
        if (writeConcurrency >= 0) {
            options.setMaxConcurrency(writeConcurrency);
        }
        return options;
    }

    private AzureStorageSettings getStorageSettings(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        return azureStorageSettings;
    }

    private void closeInternally(ClientState state) {
        final Future<?> shutdownFuture = state.getEventLoopGroup().shutdownGracefully(0, 5, TimeUnit.SECONDS);
        shutdownFuture.awaitUninterruptibly();

        if (shutdownFuture.isSuccess() == false) {
            logger.warning("Error closing Netty Event Loop group", shutdownFuture.cause());
        }
    }

    /**
     * Implements HTTP pipeline policy to collect statistics on API calls. See :
     * <a href="https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/storage/azure-storage-blob/migrationGuides/V8_V12.md#miscellaneous">migration guide</a>
     */
    private static class HttpStatsPolicy implements HttpPipelinePolicy {
        private final BiConsumer<HttpRequest, HttpResponse> statsCollector;

        HttpStatsPolicy(final BiConsumer<HttpRequest, HttpResponse> statsCollector) {
            this.statsCollector = statsCollector;
        }

        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext httpPipelineCallContext, HttpPipelineNextPolicy httpPipelineNextPolicy) {
            final HttpRequest request = httpPipelineCallContext.getHttpRequest();
            return httpPipelineNextPolicy.process().doOnNext(response -> statsCollector.accept(request, response));
        }

        @Override
        public HttpPipelinePosition getPipelinePosition() {
            // This policy must be in a position to see each retry
            return HttpPipelinePosition.PER_RETRY;
        }
    }

    /**
     * Helper class to hold the state of the cached clients and associated event groups to support
     * graceful shutdown logic.
     */
    private static class ClientState {
        private final BlobServiceClient client;
        private final EventLoopGroup eventLoopGroup;

        ClientState(final BlobServiceClient client, final EventLoopGroup eventLoopGroup) {
            this.client = client;
            this.eventLoopGroup = eventLoopGroup;
        }

        public BlobServiceClient getClient() {
            return client;
        }

        public EventLoopGroup getEventLoopGroup() {
            return eventLoopGroup;
        }
    }

    private static final class PrimaryClientState {
        private final AzureStorageSettings sourceSettings;
        private final AzureStorageSettings primarySettings;
        private final ClientState clientState;

        private PrimaryClientState(AzureStorageSettings sourceSettings, AzureStorageSettings primarySettings, ClientState clientState) {
            this.sourceSettings = sourceSettings;
            this.primarySettings = primarySettings;
            this.clientState = clientState;
        }
    }

    int primaryClientCount() {
        synchronized (clientLifecycleMutex) {
            return primaryClients.size();
        }
    }

    boolean isPrimaryClientCurrent(String clientName, BlobServiceClient client) {
        synchronized (clientLifecycleMutex) {
            final PrimaryClientState state = primaryClients.get(clientName);
            return closed == false
                && state != null
                && state.sourceSettings == storageSettings.get(clientName)
                && state.clientState.getClient() == client;
        }
    }

    /**
     * The NIO thread factory which is aware of the SecurityManager
     */
    private static class NioThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        @SuppressWarnings("removal")
        NioThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "reactor-nio-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, () -> AccessController.doPrivileged(r), namePrefix + threadNumber.getAndIncrement(), 0);

            if (t.isDaemon()) {
                t.setDaemon(false);
            }

            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }

            return t;
        }
    }

}
