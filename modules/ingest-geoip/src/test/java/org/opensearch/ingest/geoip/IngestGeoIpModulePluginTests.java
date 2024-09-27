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

package org.opensearch.ingest.geoip;

import com.maxmind.geoip2.model.AbstractResponse;

import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.TestEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GeoIpCache;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.StreamsUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class IngestGeoIpModulePluginTests extends OpenSearchTestCase {

    public void testCachesAndEvictsResults() {
        GeoIpCache cache = new GeoIpCache(1);
        AbstractResponse response1 = mock(AbstractResponse.class);
        AbstractResponse response2 = mock(AbstractResponse.class);

        // add a key
        AbstractResponse cachedResponse = cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class, ip -> response1);
        assertSame(cachedResponse, response1);
        assertSame(cachedResponse, cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class, ip -> response1));
        assertSame(cachedResponse, cache.get(InetAddresses.forString("127.0.0.1"), AbstractResponse.class));

        // evict old key by adding another value
        cachedResponse = cache.putIfAbsent(InetAddresses.forString("127.0.0.2"), AbstractResponse.class, ip -> response2);
        assertSame(cachedResponse, response2);
        assertSame(cachedResponse, cache.putIfAbsent(InetAddresses.forString("127.0.0.2"), AbstractResponse.class, ip -> response2));
        assertSame(cachedResponse, cache.get(InetAddresses.forString("127.0.0.2"), AbstractResponse.class));

        assertNotSame(response1, cache.get(InetAddresses.forString("127.0.0.1"), AbstractResponse.class));
    }

    public void testThrowsFunctionsException() {
        GeoIpCache cache = new GeoIpCache(1);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class, ip -> {
                throw new IllegalArgumentException("bad");
            })
        );
        assertEquals("bad", ex.getMessage());
    }

    public void testInvalidInit() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new GeoIpCache(-1));
        assertEquals("geoip max cache size must be 0 or greater", ex.getMessage());
    }

    public void testAllowList() throws IOException {
        runAllowListTest(List.of());
        runAllowListTest(List.of("geoip"));
    }

    public void testInvalidAllowList() throws IOException {
        List<String> invalidAllowList = List.of("set");
        Settings.Builder settingsBuilder = Settings.builder()
            .putList(IngestGeoIpModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), invalidAllowList);
        createDb(settingsBuilder);
        try (IngestGeoIpModulePlugin plugin = new IngestGeoIpModulePlugin()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> plugin.getProcessors(createParameters(settingsBuilder.build()))
            );
            assertEquals(
                "Processor(s) "
                    + invalidAllowList
                    + " were defined in ["
                    + IngestGeoIpModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey()
                    + "] but do not exist",
                e.getMessage()
            );
        }
    }

    public void testAllowListNotSpecified() throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.remove(IngestGeoIpModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey());
        createDb(settingsBuilder);
        try (IngestGeoIpModulePlugin plugin = new IngestGeoIpModulePlugin()) {
            final Set<String> expected = Set.of("geoip");
            assertEquals(expected, plugin.getProcessors(createParameters(settingsBuilder.build())).keySet());
        }
    }

    public void testSettingsRegistration() {
        final IngestGeoIpModulePlugin plugin = new IngestGeoIpModulePlugin();
        final List<Setting<?>> settings = plugin.getSettings();
        assertTrue(settings.contains(IngestGeoIpModulePlugin.PROCESSORS_ALLOWLIST_SETTING));
    }

    private void runAllowListTest(List<String> allowList) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        createDb(settingsBuilder);
        final Settings settings = settingsBuilder.putList(IngestGeoIpModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), allowList).build();
        try (IngestGeoIpModulePlugin plugin = new IngestGeoIpModulePlugin()) {
            assertEquals(Set.copyOf(allowList), plugin.getProcessors(createParameters(settings)).keySet());
        }
    }

    private void createDb(Settings.Builder settingsBuilder) throws IOException {
        Path configDir = createTempDir();
        Path userAgentConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(userAgentConfigDir);
        settingsBuilder.put("ingest.geoip.database_path", configDir).put("path.home", configDir);
        try {
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")),
                configDir.resolve("GeoLite2-City.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")),
                configDir.resolve("GeoLite2-Country.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-ASN.mmdb")),
                configDir.resolve("GeoLite2-ASN.mmdb")
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Processor.Parameters createParameters(Settings settings) {
        return new Processor.Parameters(
            TestEnvironment.newEnvironment(settings),
            null,
            null,
            null,
            () -> 0L,
            (a, b) -> null,
            null,
            null,
            $ -> {},
            null
        );
    }
}
