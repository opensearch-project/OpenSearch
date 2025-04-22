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

package org.opensearch.discovery.ec2;

import software.amazon.awssdk.services.ec2.model.Instance;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.opensearch.Version;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.Streams;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.discovery.SeedHostsResolver;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "use a http server")
public class Ec2RetriesTests extends AbstractEc2MockAPITestCase {
    @Override
    protected MockTransportService createTransportService() {
        return new MockTransportService(
            Settings.EMPTY,
            new MockNioTransport(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                networkService,
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(Collections.emptyList()),
                new NoneCircuitBreakerService(),
                NoopTracer.INSTANCE
            ),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null,
            NoopTracer.INSTANCE
        );
    }

    public void testEC2DiscoveryRetriesOnRateLimiting() throws IOException {
        final String accessKey = "ec2_access";
        final List<String> hosts = Collections.singletonList("127.0.0.1:9300");
        final Map<String, Integer> failedRequests = new ConcurrentHashMap<>();
        // retry the same request 5 times at most
        final int maxRetries = randomIntBetween(1, 5);
        httpServer.createContext("/", exchange -> {
            if (exchange.getRequestMethod().equals("POST")) {
                final String request = Streams.readFully(exchange.getRequestBody()).utf8ToString();
                final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
                if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {
                    final String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    if (auth == null || auth.contains(accessKey) == false) {
                        throw new IllegalArgumentException("wrong access key: " + auth);
                    }
                    if (failedRequests.compute(
                        exchange.getRequestHeaders().getFirst("Amz-sdk-invocation-id"),
                        (requestId, count) -> (count == null ? 0 : count) + 1
                    ) < maxRetries) {
                        exchange.sendResponseHeaders(HttpStatus.SC_SERVICE_UNAVAILABLE, -1);
                        return;
                    }
                    // Simulate an EC2 DescribeInstancesResponse
                    byte[] responseBody = null;
                    for (NameValuePair parse : URLEncodedUtils.parse(request, UTF_8)) {
                        if ("Action".equals(parse.getName())) {
                            responseBody = generateDescribeInstancesResponse(
                                hosts.stream()
                                    .map(address -> Instance.builder().publicIpAddress(address).build())
                                    .collect(Collectors.toList())
                            );
                            break;
                        }
                    }
                    responseBody = responseBody == null ? new byte[0] : responseBody;
                    exchange.getResponseHeaders().set("Content-Type", "text/xml; charset=UTF-8");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, responseBody.length);
                    exchange.getResponseBody().write(responseBody);
                    exchange.getResponseBody().flush();
                    return;
                }
            }
            fail("did not send response");
        });
        try (Ec2DiscoveryPlugin plugin = new Ec2DiscoveryPlugin(buildSettings(accessKey))) {
            final SeedHostsProvider seedHostsProvider = plugin.getSeedHostProviders(transportService, networkService).get("ec2").get();
            final SeedHostsResolver resolver = new SeedHostsResolver("test", Settings.EMPTY, transportService, seedHostsProvider);
            resolver.start();
            final List<TransportAddress> addressList = seedHostsProvider.getSeedAddresses(null);
            assertThat(addressList, Matchers.hasSize(1));
            assertThat(addressList.get(0).toString(), is(hosts.get(0)));
            assertThat(failedRequests, aMapWithSize(1));
            assertThat(failedRequests.values().iterator().next(), is(maxRetries));
        }
    }
}
