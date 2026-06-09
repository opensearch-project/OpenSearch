/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * End-to-end verification that boot-time cleanup wipes leaked spill entries and that
 * DataFusion provisions a fresh {@code datafusion-XXXXXX/} on top. The
 * {@code integTestSpillCleanup} Gradle task pre-seeds a {@code datafusion-aB3kF7/}
 * subtree and a loose {@code stray.txt} into the spill directory before cluster boot;
 * this IT asserts:
 * <ol>
 *   <li>Cluster booted healthy (200 from the spill stats endpoint).</li>
 *   <li>Seeded entries are gone — cleanup ran.</li>
 *   <li>Spill directory still exists — cleanup did not rmdir the root.</li>
 * </ol>
 *
 * <p>Scope note: this IT does <strong>not</strong> reproduce the EACCES boot failure
 * that motivated the fix — the seeded spill directory lives under
 * {@code build/spill-cleanup-it}, whose parent is writable to the test JVM, so the
 * pre-fix {@code fs::remove_dir_all(spill_dir)} would also pass these assertions.
 * The EACCES regression is owned by the Rust unit test
 * {@code create_global_runtime_succeeds_when_jvm_does_not_own_spill_parent} in
 * {@code analytics-backend-datafusion/rust/src/api.rs}, which chmods the parent to
 * {@code 0o555}.
 *
 * <p>Default {@code integTest} excludes this class.
 */
public class SpillCleanupOnBootIT extends OpenSearchRestTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SPILL_ENDPOINT = "/_plugins/_analytics_backend_datafusion/stats/disk_spill";

    /** Names of the leaked entries pre-seeded by the Gradle task. */
    private static final String LEAKED_SUBDIR = "datafusion-aB3kF7";
    private static final String LEAKED_LOOSE_FILE = "stray.txt";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testLeakedEntriesWipedAndSpillRootPreserved() throws Exception {
        // Cluster booted: stats endpoint responds. Proves create_global_runtime did not fail.
        Response response = client().performRequest(new Request("GET", SPILL_ENDPOINT));
        assertEquals("spill stats endpoint must return 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        JsonNode nodes = root.get("nodes");
        assertNotNull("response must contain nodes section", nodes);
        assertTrue("expected at least one node", nodes.size() > 0);

        // Pull the spill directory out of the per-node response and assert all nodes agree on it.
        String reportedDir = null;
        var nodeIds = nodes.fieldNames();
        while (nodeIds.hasNext()) {
            String id = nodeIds.next();
            JsonNode spill = nodes.get(id).get("disk_spill");
            assertNotNull("node " + id + " missing spill section", spill);
            String dir = spill.get("directory").asText();
            assertFalse("directory must not be empty (node " + id + ")", dir.isEmpty());
            if (reportedDir == null) {
                reportedDir = dir;
            } else {
                assertEquals("all nodes in this IT share one spill directory", reportedDir, dir);
            }
        }
        assertNotNull("at least one node must have reported a spill directory", reportedDir);

        Path spillRoot = PathUtils.get(reportedDir);

        // Spill root preserved, seeded entries gone.
        assertTrue("spill directory must still exist after boot: " + spillRoot, Files.isDirectory(spillRoot));
        Path leakedSubdir = spillRoot.resolve(LEAKED_SUBDIR);
        Path leakedLooseFile = spillRoot.resolve(LEAKED_LOOSE_FILE);
        assertFalse("leaked datafusion-* subdir must be removed: " + leakedSubdir, Files.exists(leakedSubdir));
        assertFalse("leaked top-level file must be removed: " + leakedLooseFile, Files.exists(leakedLooseFile));

        // DataFusion's tempdir_in provisions at least one fresh datafusion-XXXXXX/ per node.
        // None should reuse the seeded leaked name — that would mean cleanup didn't run.
        try (Stream<Path> children = Files.list(spillRoot)) {
            List<Path> dfSubdirs = children.filter(Files::isDirectory)
                .filter(p -> p.getFileName().toString().startsWith("datafusion-"))
                .toList();
            assertFalse("expected at least one fresh datafusion-XXXXXX/ subdir; found none in " + spillRoot,
                dfSubdirs.isEmpty());
            for (Path dfDir : dfSubdirs) {
                assertNotEquals("fresh subdir must not collide with seeded leaked name: " + dfDir,
                    LEAKED_SUBDIR, dfDir.getFileName().toString());
            }
        }
    }

    private JsonNode parseResponse(Response response) throws IOException, ParseException {
        return MAPPER.readTree(EntityUtils.toString(response.getEntity()));
    }
}
