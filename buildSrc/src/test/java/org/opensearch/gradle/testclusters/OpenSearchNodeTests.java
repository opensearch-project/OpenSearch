/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.testclusters;

import org.opensearch.gradle.test.GradleUnitTestCase;

import java.nio.file.Path;
import java.util.Map;

public class OpenSearchNodeTests extends GradleUnitTestCase {

    public void testJvmOutputPathsAreAbsolute() {
        Path logPath = Path.of("build", "testclusters", "node-0", "logs");
        Path absoluteLogPath = logPath.toAbsolutePath();

        Map<String, String> expansions = OpenSearchNode.jvmOptionExpansions(logPath);

        assertEquals("-XX:HeapDumpPath=" + absoluteLogPath, expansions.get("-XX:HeapDumpPath=data"));
        assertEquals(absoluteLogPath.resolve("gc.log").toString(), expansions.get("logs/gc.log"));
        assertEquals("-XX:ErrorFile=" + absoluteLogPath.resolve("hs_err_pid%p.log"), expansions.get("-XX:ErrorFile=logs/hs_err_pid%p.log"));
    }
}
