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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.network;

import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpInfo;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpStats;
import org.opensearch.http.NullDispatcher;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.startsWith;

public class NetworkModuleTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private SecureSettingsFactory secureSettingsFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(NetworkModuleTests.class.getName());
        secureSettingsFactory = new SecureSettingsFactory() {

            @Override
            public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) {
                return Optional.of(new SecureTransportSettingsProvider() {
                    @Override
                    public Optional<TransportExceptionHandler> buildServerTransportExceptionHandler(
                        Settings settings,
                        Transport transport
                    ) {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, Transport transport)
                        throws SSLException {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<SSLEngine> buildSecureClientTransportEngine(Settings settings, String hostname, int port)
                        throws SSLException {
                        return Optional.empty();
                    }
                });
            }

            @Override
            public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) {
                return Optional.of(new SecureHttpTransportSettingsProvider() {
                    @Override
                    public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport)
                        throws SSLException {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(
                        Settings settings,
                        HttpServerTransport transport
                    ) {
                        return Optional.empty();
                    }
                });
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}

        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }

        @Override
        public HttpInfo info() {
            return null;
        }

        @Override
        public HttpStats stats() {
            return null;
        }
    }

    public void testRegisterTransport() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom").build();
        Supplier<Transport> custom = () -> null; // content doesn't matter we check reference equality
        NetworkPlugin plugin = new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(
                Settings settings,
                ThreadPool threadPool,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedWriteableRegistry namedWriteableRegistry,
                NetworkService networkService,
                Tracer tracer
            ) {
                return Collections.singletonMap("custom", custom);
            }
        };
        NetworkModule module = newNetworkModule(settings, null, plugin);
        assertSame(custom, module.getTransportSupplier());
    }

    public void testRegisterHttpTransport() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local")
            .build();
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;

        NetworkModule module = newNetworkModule(settings, null, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
                Settings settings,
                ThreadPool threadPool,
                BigArrays bigArrays,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedXContentRegistry xContentRegistry,
                NetworkService networkService,
                HttpServerTransport.Dispatcher requestDispatcher,
                ClusterSettings clusterSettings,
                Tracer tracer
            ) {
                return Collections.singletonMap("custom", custom);
            }
        });
        assertSame(custom, module.getHttpServerTransportSupplier());

        settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        NetworkModule newModule = newNetworkModule(settings, null);
        expectThrows(IllegalStateException.class, () -> newModule.getHttpServerTransportSupplier());
    }

    public void testRegisterSecureTransport() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom-secure").build();
        Supplier<Transport> custom = () -> null; // content doesn't matter we check reference equality
        NetworkPlugin plugin = new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getSecureTransports(
                Settings settings,
                ThreadPool threadPool,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedWriteableRegistry namedWriteableRegistry,
                NetworkService networkService,
                SecureTransportSettingsProvider secureTransportSettingsProvider,
                Tracer tracer
            ) {
                return Collections.singletonMap("custom-secure", custom);
            }
        };
        NetworkModule module = newNetworkModule(settings, null, List.of(secureSettingsFactory), plugin);
        assertSame(custom, module.getTransportSupplier());
    }

    public void testRegisterSecureHttpTransport() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom-secure")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local")
            .build();
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;

        NetworkModule module = newNetworkModule(settings, null, List.of(secureSettingsFactory), new NetworkPlugin() {
            @Override
            public Map<String, Supplier<HttpServerTransport>> getSecureHttpTransports(
                Settings settings,
                ThreadPool threadPool,
                BigArrays bigArrays,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedXContentRegistry xContentRegistry,
                NetworkService networkService,
                HttpServerTransport.Dispatcher requestDispatcher,
                ClusterSettings clusterSettings,
                SecureHttpTransportSettingsProvider secureTransportSettingsProvider,
                Tracer tracer
            ) {
                return Collections.singletonMap("custom-secure", custom);
            }
        });
        assertSame(custom, module.getHttpServerTransportSupplier());
    }

    public void testOverrideDefault() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "local")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "default_custom")
            .build();
        Supplier<Transport> customTransport = () -> null;  // content doesn't matter we check reference equality
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;
        Supplier<HttpServerTransport> def = FakeHttpTransport::new;
        NetworkModule module = newNetworkModule(settings, null, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(
                Settings settings,
                ThreadPool threadPool,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedWriteableRegistry namedWriteableRegistry,
                NetworkService networkService,
                Tracer tracer
            ) {
                return Collections.singletonMap("default_custom", customTransport);
            }

            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
                Settings settings,
                ThreadPool threadPool,
                BigArrays bigArrays,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedXContentRegistry xContentRegistry,
                NetworkService networkService,
                HttpServerTransport.Dispatcher requestDispatcher,
                ClusterSettings clusterSettings,
                Tracer tracer
            ) {
                Map<String, Supplier<HttpServerTransport>> supplierMap = new HashMap<>();
                supplierMap.put("custom", custom);
                supplierMap.put("default_custom", def);
                return supplierMap;
            }
        });
        assertSame(custom, module.getHttpServerTransportSupplier());
        assertSame(customTransport, module.getTransportSupplier());
    }

    public void testDefaultKeys() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .build();
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;
        Supplier<HttpServerTransport> def = FakeHttpTransport::new;
        Supplier<Transport> customTransport = () -> null;
        NetworkModule module = newNetworkModule(settings, null, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(
                Settings settings,
                ThreadPool threadPool,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedWriteableRegistry namedWriteableRegistry,
                NetworkService networkService,
                Tracer tracer
            ) {
                return Collections.singletonMap("default_custom", customTransport);
            }

            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
                Settings settings,
                ThreadPool threadPool,
                BigArrays bigArrays,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedXContentRegistry xContentRegistry,
                NetworkService networkService,
                HttpServerTransport.Dispatcher requestDispatcher,
                ClusterSettings clusterSettings,
                Tracer tracer
            ) {
                Map<String, Supplier<HttpServerTransport>> supplierMap = new HashMap<>();
                supplierMap.put("custom", custom);
                supplierMap.put("default_custom", def);
                return supplierMap;
            }
        });

        assertSame(def, module.getHttpServerTransportSupplier());
        assertSame(customTransport, module.getTransportSupplier());
    }

    public void testRegisterInterceptor() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        AtomicInteger called = new AtomicInteger(0);

        TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };
        NetworkModule module = newNetworkModule(settings, null, new NetworkPlugin() {
            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                assertNotNull(threadContext);
                return Collections.singletonList(interceptor);
            }
        });

        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertEquals(0, called.get());
        transportInterceptor.interceptHandler("foo/bar/boom", null, true, null);
        assertEquals(1, called.get());
        transportInterceptor.interceptHandler("foo/baz/boom", null, false, null);
        assertEquals(2, called.get());
        assertTrue(transportInterceptor instanceof NetworkModule.CompositeTransportInterceptor);
        assertEquals(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.size(), 1);
        assertSame(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.get(0), interceptor);

        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> {
            newNetworkModule(settings, null, new NetworkPlugin() {
                @Override
                public List<TransportInterceptor> getTransportInterceptors(
                    NamedWriteableRegistry namedWriteableRegistry,
                    ThreadContext threadContext
                ) {
                    assertNotNull(threadContext);
                    return Collections.singletonList(null);
                }
            });
        });
        assertEquals("interceptor must not be null", nullPointerException.getMessage());
    }

    public void testRegisterCoreInterceptor() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        AtomicInteger called = new AtomicInteger(0);

        TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };

        List<TransportInterceptor> coreTransportInterceptors = new ArrayList<>();
        coreTransportInterceptors.add(interceptor);

        NetworkModule module = newNetworkModule(settings, coreTransportInterceptors);

        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertEquals(0, called.get());
        transportInterceptor.interceptHandler("foo/bar/boom", null, true, null);
        assertEquals(1, called.get());
        transportInterceptor.interceptHandler("foo/baz/boom", null, false, null);
        assertEquals(2, called.get());
        assertTrue(transportInterceptor instanceof NetworkModule.CompositeTransportInterceptor);
        assertEquals(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.size(), 1);
        assertSame(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.get(0), interceptor);
    }

    public void testInterceptorOrder() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        AtomicInteger called = new AtomicInteger(0);
        AtomicInteger called1 = new AtomicInteger(0);

        TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };

        TransportInterceptor interceptor1 = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called1.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };

        List<TransportInterceptor> coreTransportInterceptors = new ArrayList<>();
        coreTransportInterceptors.add(interceptor1);

        NetworkModule module = newNetworkModule(settings, coreTransportInterceptors, new NetworkPlugin() {
            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                assertNotNull(threadContext);
                return Collections.singletonList(interceptor);
            }
        });

        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertEquals(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.size(), 2);

        assertEquals(0, called.get());
        assertEquals(0, called1.get());
        transportInterceptor.interceptHandler("foo/bar/boom", null, true, null);
        assertEquals(1, called.get());
        assertEquals(1, called1.get());
        transportInterceptor.interceptHandler("foo/baz/boom", null, false, null);
        assertEquals(2, called.get());
        assertEquals(2, called1.get());
    }

    public void testInterceptorOrderException() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        AtomicInteger called = new AtomicInteger(0);
        AtomicInteger called1 = new AtomicInteger(0);

        TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };

        TransportInterceptor interceptor1 = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                called1.incrementAndGet();
                throw new RuntimeException("Handler Invoke Failed");
            }
        };

        List<TransportInterceptor> coreTransportInterceptors = new ArrayList<>();
        coreTransportInterceptors.add(interceptor1);

        NetworkModule module = newNetworkModule(settings, coreTransportInterceptors, new NetworkPlugin() {
            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                assertNotNull(threadContext);
                return Collections.singletonList(interceptor);
            }
        });

        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertEquals(((NetworkModule.CompositeTransportInterceptor) transportInterceptor).transportInterceptors.size(), 2);

        assertEquals(0, called.get());
        assertEquals(0, called1.get());
        try {
            transportInterceptor.interceptHandler("foo/bar/boom", null, true, null);
        } catch (Exception e) {
            assertEquals(1, called.get());
            assertEquals(1, called1.get());
        }

        coreTransportInterceptors = new ArrayList<>();
        coreTransportInterceptors.add(interceptor);
        module = newNetworkModule(settings, coreTransportInterceptors, new NetworkPlugin() {
            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                assertNotNull(threadContext);
                return Collections.singletonList(interceptor1);
            }
        });

        transportInterceptor = module.getTransportInterceptor();

        try {
            transportInterceptor.interceptHandler("foo/baz/boom", null, false, null);
        } catch (Exception e) {
            assertEquals(1, called.get());
            assertEquals(2, called1.get());
        }
    }

    private NetworkModule newNetworkModule(
        Settings settings,
        List<TransportInterceptor> coreTransportInterceptors,
        NetworkPlugin... plugins
    ) {
        return newNetworkModule(settings, coreTransportInterceptors, List.of(), plugins);
    }

    private NetworkModule newNetworkModule(
        Settings settings,
        List<TransportInterceptor> coreTransportInterceptors,
        List<SecureSettingsFactory> secureSettingsFactories,
        NetworkPlugin... plugins
    ) {
        return new NetworkModule(
            settings,
            Arrays.asList(plugins),
            threadPool,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            null,
            new NullDispatcher(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            NoopTracer.INSTANCE,
            coreTransportInterceptors,
            secureSettingsFactories
        );
    }

    public void testRegisterSecureTransportMultipleProviers() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom-secure").build();
        Supplier<Transport> custom = () -> null; // content doesn't matter we check reference equality
        NetworkPlugin plugin = new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getSecureTransports(
                Settings settings,
                ThreadPool threadPool,
                PageCacheRecycler pageCacheRecycler,
                CircuitBreakerService circuitBreakerService,
                NamedWriteableRegistry namedWriteableRegistry,
                NetworkService networkService,
                SecureTransportSettingsProvider secureTransportSettingsProvider,
                Tracer tracer
            ) {
                return Collections.singletonMap("custom-secure", custom);
            }
        };

        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> newNetworkModule(settings, null, List.of(secureSettingsFactory, secureSettingsFactory), plugin)
        );
        assertThat(ex.getMessage(), startsWith("there is more than one secure transport settings provider"));
    }
}
