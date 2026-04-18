/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base class for all DataFusion REST integration tests in the sandbox QA package.
 * <p>
 * Handles cluster-level concerns: preserving cluster/indices across test methods,
 * loading classpath resources, JSON escaping, and common assertion helpers.
 * <p>
 * Test data provisioning (e.g. ClickBench dataset) is handled separately by
 * {@link ClickBenchTestFixture} to keep cluster config orthogonal to test data.
 * Future test classes (LuceneScanRestIT, etc.) can extend this without needing
 * ClickBench data.
 */
public abstract class DataFusionRestTestCase extends OpenSearchRestTestCase {

    protected static final Logger logger = LogManager.getLogger(DataFusionRestTestCase.class);

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /**
     * Load a classpath resource as a UTF-8 string.
     * Fails with an assertion error if the resource does not exist.
     */
    protected String loadResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }

    /**
     * Escape backslashes and double quotes for safe embedding in JSON string values.
     */
    protected static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Assert that the response has HTTP 200 status and return the body as a parsed Map.
     * The {@code context} string is included in failure messages for easier debugging.
     */
    protected Map<String, Object> assertOkAndParse(Response response, String context) throws IOException {
        assertEquals(context + ": expected HTTP 200", 200, response.getStatusLine().getStatusCode());
        return entityAsMap(response);
    }
}
