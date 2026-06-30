/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class HierarchicalRoutingProcessorTests extends OpenSearchTestCase {

    public void testBasicRouting() throws Exception {
        IngestDocument ingestDocument = createTestDocument("/company/department/team/file.txt");
        Processor processor = createProcessor("path_field", 2, "/", false, true);

        processor.execute(ingestDocument);

        String routingValue = ingestDocument.getFieldValue("_routing", String.class);
        assertThat(routingValue, notNullValue());

        // Test that it's deterministic - same input produces same output
        IngestDocument ingestDocument2 = createTestDocument("/company/department/team/file.txt");
        processor.execute(ingestDocument2);
        String secondRoutingValue = ingestDocument2.getFieldValue("_routing", String.class);
        assertThat(routingValue, equalTo(secondRoutingValue));
    }

    public void testDifferentAnchorDepths() throws Exception {
        String path = "/company/department/team/project/file.txt";
        IngestDocument doc1 = createTestDocument(path);
        IngestDocument doc2 = createTestDocument(path);
        IngestDocument doc3 = createTestDocument(path);

        Processor processor1 = createProcessor("path_field", 1, "/", false, true);
        Processor processor2 = createProcessor("path_field", 2, "/", false, true);
        Processor processor3 = createProcessor("path_field", 3, "/", false, true);

        processor1.execute(doc1);
        processor2.execute(doc2);
        processor3.execute(doc3);

        String routing1 = doc1.getFieldValue("_routing", String.class);
        String routing2 = doc2.getFieldValue("_routing", String.class);
        String routing3 = doc3.getFieldValue("_routing", String.class);

        // Different depths should produce different routing values for this path
        assertThat(routing1, notNullValue());
        assertThat(routing2, notNullValue());
        assertThat(routing3, notNullValue());

        // They should all be different since they use different anchor depths
        assertNotEquals(routing1, routing2);
        assertNotEquals(routing2, routing3);
        assertNotEquals(routing1, routing3);
    }

    public void testCustomSeparator() throws Exception {
        IngestDocument ingestDocument = createTestDocument("company\\department\\team\\file.txt");
        Processor processor = createProcessor("path_field", 2, "\\", false, true);

        processor.execute(ingestDocument);

        String routingValue = ingestDocument.getFieldValue("_routing", String.class);
        assertThat(routingValue, notNullValue());

        // Test deterministic behavior with same input
        IngestDocument ingestDocument2 = createTestDocument("company\\department\\team\\file.txt");
        processor.execute(ingestDocument2);
        String routingValue2 = ingestDocument2.getFieldValue("_routing", String.class);
        assertThat(routingValue, equalTo(routingValue2));
    }

    public void testPathNormalization() throws Exception {
        // Test various path formats that should normalize to the same result
        String[] paths = {
            "/company/department/team/",
            "//company//department//team//",
            "company/department/team",
            "/company/department/team",
            " /company/department/team/ " };

        String expectedRouting = null;

        for (String path : paths) {
            IngestDocument doc = createTestDocument(path);
            Processor processor = createProcessor("path_field", 2, "/", false, true);
            processor.execute(doc);

            String routing = doc.getFieldValue("_routing", String.class);
            assertThat("Path: " + path, routing, notNullValue());

            if (expectedRouting == null) {
                expectedRouting = routing;
            } else {
                assertThat("Path: " + path + " should produce same routing", routing, equalTo(expectedRouting));
            }
        }
    }

    public void testMissingField() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());

        // Test with ignore_missing = false (should throw exception)
        Processor processor = createProcessor("missing_field", 2, "/", false, true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("not present"));

        // Test with ignore_missing = true (should skip document)
        Processor processorIgnore = createProcessor("missing_field", 2, "/", true, true);
        IngestDocument result = processorIgnore.execute(ingestDocument);
        assertThat(result, equalTo(ingestDocument));
        assertFalse(result.hasField("_routing"));
    }

    public void testEmptyPath() throws Exception {
        IngestDocument ingestDocument = createTestDocument("");

        // Test with ignore_missing = false
        Processor processor = createProcessor("path_field", 2, "/", false, true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("null or empty"));

        // Test with ignore_missing = true
        Processor processorIgnore = createProcessor("path_field", 2, "/", true, true);
        IngestDocument result = processorIgnore.execute(ingestDocument);
        assertFalse(result.hasField("_routing"));
    }

    public void testNullPath() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("path_field", null);
        IngestDocument ingestDocument = new IngestDocument(document, new HashMap<>());

        Processor processor = createProcessor("path_field", 2, "/", false, true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("doesn't exist"));
    }

    public void testOverrideExistingRouting() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("path_field", "/company/department/team/file.txt");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_routing", "existing_routing");
        IngestDocument ingestDocument = new IngestDocument(document, metadata);

        // Test with override_existing = false - existing routing should be preserved
        Processor processorNoOverride = createProcessor("path_field", 2, "/", false, false);
        processorNoOverride.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("_routing", String.class), equalTo("9195230787246894787"));

        // Create new document for override test
        Map<String, Object> document2 = new HashMap<>();
        document2.put("path_field", "/company/department/team/file.txt");
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("_routing", "existing_routing");
        IngestDocument ingestDocument2 = new IngestDocument(document2, metadata2);

        // Test with override_existing = true (default) - routing should be overridden
        Processor processorOverride = createProcessor("path_field", 2, "/", false, true);
        processorOverride.execute(ingestDocument2);
        String newRouting = ingestDocument2.getFieldValue("_routing", String.class);
        assertThat(newRouting, notNullValue());
        assertNotEquals("New routing should be different from existing", newRouting, "existing_routing");
    }

    public void testShortPath() throws Exception {
        // Test path with fewer segments than anchor depth
        IngestDocument ingestDocument = createTestDocument("/company");
        Processor processor = createProcessor("path_field", 3, "/", false, true);

        processor.execute(ingestDocument);

        String routingValue = ingestDocument.getFieldValue("_routing", String.class);
        assertThat(routingValue, notNullValue());
    }

    public void testRootPath() throws Exception {
        // Test empty path after normalization
        IngestDocument ingestDocument = createTestDocument("///");
        Processor processor = createProcessor("path_field", 2, "/", false, true);

        processor.execute(ingestDocument);

        String routingValue = ingestDocument.getFieldValue("_routing", String.class);
        assertThat(routingValue, notNullValue());
    }

    public void testFactoryValidation() throws Exception {
        // Test invalid anchor depth
        Map<String, Object> config1 = createConfig("path_field", 0, "/", false, true);
        OpenSearchParseException exception = expectThrows(
            OpenSearchParseException.class,
            () -> new HierarchicalRoutingProcessor.Factory().create(null, "test", null, config1)
        );
        assertThat(exception.getMessage(), containsString("must be greater than 0"));

        // Test empty path separator
        Map<String, Object> config2 = createConfig("path_field", 2, "", false, true);
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> new HierarchicalRoutingProcessor.Factory().create(null, "test", null, config2)
        );
        assertThat(exception.getMessage(), containsString("cannot be null or empty"));

        // Test null path field
        Map<String, Object> config3 = createConfig(null, 2, "/", false, true);
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> new HierarchicalRoutingProcessor.Factory().create(null, "test", null, config3)
        );
        assertThat(exception.getMessage(), containsString("required property is missing"));
    }

    public void testRoutingConsistency() throws Exception {
        // Test that same input always produces same routing
        String path = "/company/department/team/project/file.txt";
        String expectedRouting = null;

        for (int i = 0; i < 10; i++) {
            IngestDocument doc = createTestDocument(path);
            Processor processor = createProcessor("path_field", 2, "/", false, true);
            processor.execute(doc);

            String routing = doc.getFieldValue("_routing", String.class);
            if (expectedRouting == null) {
                expectedRouting = routing;
            } else {
                assertThat("Iteration " + i, routing, equalTo(expectedRouting));
            }
        }
    }

    public void testDifferentPathsSameAnchor() throws Exception {
        // Test that different paths with same anchor produce same routing
        String[] paths = {
            "/company/department/team1/file1.txt",
            "/company/department/team2/file2.txt",
            "/company/department/project/subfolder/file3.txt" };

        String expectedRouting = null;

        for (String path : paths) {
            IngestDocument doc = createTestDocument(path);
            Processor processor = createProcessor("path_field", 2, "/", false, true);
            processor.execute(doc);

            String routing = doc.getFieldValue("_routing", String.class);
            assertThat("Path: " + path, routing, notNullValue());

            if (expectedRouting == null) {
                expectedRouting = routing;
            } else {
                assertThat(
                    "Path: " + path + " should produce same routing as other paths with same anchor",
                    routing,
                    equalTo(expectedRouting)
                );
            }
        }
    }

    // Helper methods
    private IngestDocument createTestDocument(String path) {
        Map<String, Object> document = new HashMap<>();
        document.put("path_field", path);
        return new IngestDocument(document, new HashMap<>());
    }

    private Processor createProcessor(String pathField, int anchorDepth, String separator, boolean ignoreMissing, boolean overrideExisting)
        throws Exception {
        Map<String, Object> config = createConfig(pathField, anchorDepth, separator, ignoreMissing, overrideExisting);
        return new HierarchicalRoutingProcessor.Factory().create(null, "test", "test processor", config);
    }

    private Map<String, Object> createConfig(
        String pathField,
        int anchorDepth,
        String separator,
        boolean ignoreMissing,
        boolean overrideExisting
    ) {
        Map<String, Object> config = new HashMap<>();
        if (pathField != null) {
            config.put("path_field", pathField);
        }
        config.put("anchor_depth", anchorDepth);
        config.put("path_separator", separator);
        config.put("ignore_missing", ignoreMissing);
        config.put("override_existing", overrideExisting);
        return config;
    }

}
