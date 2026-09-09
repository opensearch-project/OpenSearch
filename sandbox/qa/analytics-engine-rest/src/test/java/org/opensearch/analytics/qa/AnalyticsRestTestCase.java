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
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base class for all analytics REST integration tests in the sandbox QA package.
 * <p>
 * Handles cluster-level concerns: preserving cluster/indices across test methods,
 * loading classpath resources, JSON escaping, and common assertion helpers.
 * <p>
 * Test data provisioning is handled separately by dataset-specific helpers
 * (e.g. {@link ClickBenchTestHelper}) to keep cluster config orthogonal to test data.
 */
public abstract class AnalyticsRestTestCase extends OpenSearchRestTestCase {

    protected static final Logger logger = LogManager.getLogger(AnalyticsRestTestCase.class);

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

    /**
     * Extract column names from a PPL response's {@code schema} field. The real opensearch-sql
     * plugin emits {@code "schema": [{"name": "...", "type": "..."}, ...]} (vs. the legacy
     * opensearch-sql shim's bare {@code "columns": [name, ...]}). Returns an empty list
     * if no schema is present.
     */
    @SuppressWarnings("unchecked")
    protected static List<String> extractColumnNames(Map<String, Object> response) {
        Object schema = response.get("schema");
        if (schema == null) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> entries = (List<Map<String, Object>>) schema;
        return entries.stream().map(e -> (String) e.get("name")).collect(Collectors.toList());
    }

    /**
     * Hook invoked before each test method via JUnit's {@code @Before}, and also before
     * each {@link #executePpl} call as a belt-and-braces guard. Subclasses with lazily-
     * provisioned datasets should override to call their {@code DatasetProvisioner.provision}
     * (gated on a static {@code dataProvisioned} flag so the work only happens once per
     * JVM). Default: no-op.
     *
     * <p>Routing through {@code @Before} means setup that doesn't go through
     * {@link #executePpl} (alias creation, raw {@code _search}, expect-failure paths) still
     * sees the dataset present.
     */
    protected void onBeforeQuery() throws IOException {}

    @Before
    public final void invokeOnBeforeQueryHook() throws IOException {
        onBeforeQuery();
    }

    /**
     * Execute a PPL query against the real opensearch-sql plugin at {@code /_plugins/_ppl},
     * asserting HTTP 200 and returning the parsed JSON body. The {@link #onBeforeQuery}
     * hook has already fired via {@code @Before}, so subclasses don't need to ensure data
     * provisioning here.
     */
    protected Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Execute a SQL query against the real opensearch-sql plugin at {@code /_plugins/_sql},
     * asserting HTTP 200 and returning the parsed JSON body. Same {@code @Before} provisioning
     * hook flow as {@link #executePpl(String)} — subclasses don't need to ensure datasets here.
     */
    protected Map<String, Object> executeSql(String sql) throws IOException {
        Request request = new Request("POST", "/_plugins/_sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson(sql) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "SQL: " + sql);
    }

    /**
     * Execute a PPL query against the {@code test-ppl-frontend} shim at {@code /_analytics/ppl}.
     * Use this for tests that exercise <em>engine-internal</em> behavior (e.g. perf-delegation
     * marker placement, explain output shape) where the opensearch-sql plugin's user-facing PPL
     * surface isn't on the hook. Tests that exercise a real user-typed PPL feature should keep
     * using {@link #executePpl(String)} so they validate the production path end-to-end.
     */
    protected Map<String, Object> executePplViaShim(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> parsed = assertOkAndParse(response, "PPL (shim): " + ppl);
        // Normalize: shim returns {columns, rows}; real SQL plugin returns {schema, datarows}.
        // Tests that share assertions across both helpers read 'datarows' — mirror it for shim.
        if (parsed.containsKey("rows") && parsed.containsKey("datarows") == false) {
            parsed.put("datarows", parsed.get("rows"));
        }
        return parsed;
    }
}
