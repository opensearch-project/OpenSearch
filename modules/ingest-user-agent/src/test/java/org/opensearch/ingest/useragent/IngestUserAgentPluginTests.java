/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.useragent;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.TestEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public class IngestUserAgentPluginTests extends OpenSearchTestCase {
    private Settings.Builder settingsBuilder;

    @Before
    public void setup() throws IOException {
        Path configDir = createTempDir();
        Path userAgentConfigDir = configDir.resolve("ingest-user-agent");
        Files.createDirectories(userAgentConfigDir);
        settingsBuilder = Settings.builder().put("ingest-user-agent", configDir).put("path.home", configDir);

        // Copy file, leaving out the device parsers at the end
        String regexWithoutDevicesFilename = "regexes_without_devices.yml";
        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(UserAgentProcessor.class.getResourceAsStream("/regexes.yml"), StandardCharsets.UTF_8)
            );
            BufferedWriter writer = Files.newBufferedWriter(userAgentConfigDir.resolve(regexWithoutDevicesFilename));
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("device_parsers:")) {
                    break;
                }

                writer.write(line);
                writer.newLine();
            }
        }
    }

    public void testAllowList() throws IOException {
        runAllowListTest(List.of());
        runAllowListTest(List.of("user_agent"));
    }

    public void testInvalidAllowList() throws IOException {
        List<String> invalidAllowList = List.of("set");
        final Settings settings = settingsBuilder.putList(IngestUserAgentPlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), invalidAllowList)
            .build();
        try (IngestUserAgentPlugin plugin = new IngestUserAgentPlugin()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> plugin.getProcessors(createParameters(settings))
            );
            assertEquals(
                "Processor(s) "
                    + invalidAllowList
                    + " were defined in ["
                    + IngestUserAgentPlugin.PROCESSORS_ALLOWLIST_SETTING.getKey()
                    + "] but do not exist",
                e.getMessage()
            );
        }
    }

    public void testAllowListNotSpecified() throws IOException {
        settingsBuilder.remove(IngestUserAgentPlugin.PROCESSORS_ALLOWLIST_SETTING.getKey());
        try (IngestUserAgentPlugin plugin = new IngestUserAgentPlugin()) {
            final Set<String> expected = Set.of("user_agent");
            assertEquals(expected, plugin.getProcessors(createParameters(settingsBuilder.build())).keySet());
        }
    }

    public void testSettingsRegistration() {
        final IngestUserAgentPlugin plugin = new IngestUserAgentPlugin();
        final List<Setting<?>> settings = plugin.getSettings();
        assertTrue(settings.contains(IngestUserAgentPlugin.PROCESSORS_ALLOWLIST_SETTING));
    }

    private void runAllowListTest(List<String> allowList) throws IOException {
        final Settings settings = settingsBuilder.putList(IngestUserAgentPlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), allowList).build();
        try (IngestUserAgentPlugin plugin = new IngestUserAgentPlugin()) {
            assertEquals(Set.copyOf(allowList), plugin.getProcessors(createParameters(settings)).keySet());
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
