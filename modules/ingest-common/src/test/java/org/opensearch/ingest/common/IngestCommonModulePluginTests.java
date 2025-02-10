/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.TestEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class IngestCommonModulePluginTests extends OpenSearchTestCase {

    public void testAllowlist() throws IOException {
        runAllowlistTest(List.of());
        runAllowlistTest(List.of("date"));
        runAllowlistTest(List.of("set"));
        runAllowlistTest(List.of("copy", "date"));
        runAllowlistTest(List.of("date", "set", "copy"));
    }

    private void runAllowlistTest(List<String> allowlist) throws IOException {
        final Settings settings = Settings.builder()
            .putList(IngestCommonModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), allowlist)
            .build();
        try (IngestCommonModulePlugin plugin = new IngestCommonModulePlugin()) {
            assertEquals(Set.copyOf(allowlist), plugin.getProcessors(createParameters(settings)).keySet());
        }
    }

    public void testAllowlistNotSpecified() throws IOException {
        final Settings.Builder builder = Settings.builder();
        builder.remove(IngestCommonModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey());
        final Settings settings = builder.build();
        try (IngestCommonModulePlugin plugin = new IngestCommonModulePlugin()) {
            final Set<String> expected = Set.of(
                "append",
                "urldecode",
                "sort",
                "fail",
                "trim",
                "set",
                "fingerprint",
                "pipeline",
                "json",
                "join",
                "kv",
                "bytes",
                "date",
                "drop",
                "community_id",
                "lowercase",
                "convert",
                "copy",
                "gsub",
                "dot_expander",
                "rename",
                "remove_by_pattern",
                "html_strip",
                "remove",
                "csv",
                "grok",
                "date_index_name",
                "foreach",
                "script",
                "dissect",
                "uppercase",
                "split"
            );
            assertEquals(expected, plugin.getProcessors(createParameters(settings)).keySet());
        }
    }

    public void testAllowlistHasNonexistentProcessors() throws IOException {
        final Settings settings = Settings.builder()
            .putList(IngestCommonModulePlugin.PROCESSORS_ALLOWLIST_SETTING.getKey(), List.of("threeve"))
            .build();
        try (IngestCommonModulePlugin plugin = new IngestCommonModulePlugin()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> plugin.getProcessors(createParameters(settings))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("threeve"));
        }
    }

    private static Processor.Parameters createParameters(Settings settings) {
        return new Processor.Parameters(
            TestEnvironment.newEnvironment(Settings.builder().put(settings).put("path.home", "").build()),
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
