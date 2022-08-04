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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.Mono;

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
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.implementation.connectionstring.StorageEndpoint;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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

    static {
        // See please:
        // - https://github.com/Azure/azure-sdk-for-java/issues/24373
        // - https://github.com/Azure/azure-sdk-for-java/pull/25004
        // - https://github.com/Azure/azure-sdk-for-java/pull/24374
        Configuration.getGlobalConfiguration().put("AZURE_JACKSON_ADAPTER_USE_ACCESS_HELPER", "true");
    }

    public AzureStorageService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshAndClearCache(clientsSettings);
    }

    /**
     * Obtains a {@code BlobServiceClient} on each invocation using the current client
     * settings. BlobServiceClient is thread safe and and could be cached but the settings
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
     * settings. BlobServiceClient is thread safe and and could be cached but the settings
     * can change, therefore the instance might be recreated from scratch.

     * @param clientName client name
     * @param statsCollector statistics collector
     * @return the {@code BlobServiceClient} instance and context
     */
    public Tuple<BlobServiceClient, Supplier<Context>> client(String clientName, BiConsumer<HttpRequest, HttpResponse> statsCollector) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }

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

    private ClientState buildClient(AzureStorageSettings azureStorageSettings, BiConsumer<HttpRequest, HttpResponse> statsCollector)
        throws InvalidKeyException, URISyntaxException {
        final BlobServiceClientBuilder builder = createClientBuilder(azureStorageSettings);

        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(new NioThreadFactory());
        final NettyAsyncHttpClientBuilder clientBuilder = new NettyAsyncHttpClientBuilder().eventLoopGroup(eventLoopGroup);

        SocketAccess.doPrivilegedVoidException(() -> {
            final ProxySettings proxySettings = azureStorageSettings.getProxySettings();
            if (proxySettings != ProxySettings.NO_PROXY_SETTINGS) {
                if (proxySettings.isAuthenticated()) {
                    Authenticator.setDefault(new Authenticator() {
                        @Override
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(proxySettings.getUsername(), proxySettings.getPassword().toCharArray());
                        }
                    });
                }
                clientBuilder.proxy(new ProxyOptions(proxySettings.getType().toProxyType(), proxySettings.getAddress()));
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
     * retry options and combination of primary / secondary endpoints. Refer to migration guide for mode details:
     * https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/storage/azure-storage-blob/migrationGuides/V8_V12.md#miscellaneous
     */
    private BlobServiceClientBuilder applyLocationMode(final BlobServiceClientBuilder builder, final AzureStorageSettings settings) {
        final StorageConnectionString storageConnectionString = StorageConnectionString.create(settings.getConnectString(), logger);
        final StorageEndpoint endpoint = storageConnectionString.getBlobEndpoint();

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

    private static BlobServiceClientBuilder createClientBuilder(AzureStorageSettings settings) throws InvalidKeyException,
        URISyntaxException {
        return new BlobServiceClientBuilder().connectionString(settings.getConnectString());
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
        final Map<String, AzureStorageSettings> prevSettings = this.storageSettings;
        final Map<AzureStorageSettings, ClientState> prevClients = new HashMap<>(this.clients);
        prevClients.values().forEach(this::closeInternally);
        prevClients.clear();

        this.storageSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        this.clients.clear();

        // clients are built lazily by {@link client(String)}
        return prevSettings;
    }

    @Override
    public void close() {
        this.clients.values().forEach(this::closeInternally);
        this.clients.clear();
    }

    public Duration getBlobRequestTimeout(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }

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

    ParallelTransferOptions getBlobRequestOptionsForWriteBlob() {
        return null;
    }

    private void closeInternally(ClientState state) {
        final Future<?> shutdownFuture = state.getEventLoopGroup().shutdownGracefully(0, 5, TimeUnit.SECONDS);
        shutdownFuture.awaitUninterruptibly();

        if (shutdownFuture.isSuccess() == false) {
            logger.warning("Error closing Netty Event Loop group", shutdownFuture.cause());
        }
    }

    /**
     * Implements HTTP pipeline policy to collect statistics on API calls. See please:
     * https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/storage/azure-storage-blob/migrationGuides/V8_V12.md#miscellaneous
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

    /**
     * The NIO thread factory which is aware of the SecurityManager
     */
    private static class NioThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NioThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "reactor-nio-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);

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
