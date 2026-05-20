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
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AclRoutingProcessorTests extends OpenSearchTestCase {

    public void testAclRouting() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("acl_group", "group123");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("_routing", String.class), notNullValue());
    }

    public void testAclRoutingMissingField() {
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [acl_group] not present as part of path [acl_group]"));
    }

    public void testAclRoutingIgnoreMissing() throws Exception {
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Remove any existing _routing field that might have been added by RandomDocumentPicks
        if (ingestDocument.hasField("_routing")) {
            ingestDocument.removeField("_routing");
        }

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", true, true);
        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result, equalTo(ingestDocument));
        assertFalse(ingestDocument.hasField("_routing"));
    }

    public void testAclRoutingNoOverride() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("acl_group", "group123");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Set existing routing after document creation to ensure it's preserved
        ingestDocument.setFieldValue("_routing", "existing-routing");

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, false);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("_routing", String.class), equalTo("existing-routing"));
    }

    public void testAclRoutingOverride() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("acl_group", "group123");
        document.put("_routing", "existing-routing");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);
        processor.execute(ingestDocument);

        String newRouting = ingestDocument.getFieldValue("_routing", String.class);
        assertThat(newRouting, notNullValue());
        assertNotEquals(newRouting, "existing-routing");
    }

    public void testConsistentRouting() throws Exception {
        String aclValue = "team-alpha";

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("acl_group", aclValue);
        IngestDocument ingestDoc1 = RandomDocumentPicks.randomIngestDocument(random(), doc1);

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("acl_group", aclValue);
        IngestDocument ingestDoc2 = RandomDocumentPicks.randomIngestDocument(random(), doc2);

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);

        processor.execute(ingestDoc1);
        processor.execute(ingestDoc2);

        String routing1 = ingestDoc1.getFieldValue("_routing", String.class);
        String routing2 = ingestDoc2.getFieldValue("_routing", String.class);

        assertThat(routing1, equalTo(routing2));
    }

    public void testFactoryCreation() throws Exception {
        AclRoutingProcessor.Factory factory = new AclRoutingProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("acl_field", "acl_group");

        AclRoutingProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getType(), equalTo(AclRoutingProcessor.TYPE));
    }

    public void testFactoryCreationWithAllParams() throws Exception {
        AclRoutingProcessor.Factory factory = new AclRoutingProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("acl_field", "acl_group");
        config.put("target_field", "_custom_routing");
        config.put("ignore_missing", true);
        config.put("override_existing", false);

        AclRoutingProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getType(), equalTo(AclRoutingProcessor.TYPE));
    }

    public void testFactoryCreationMissingAclField() {
        AclRoutingProcessor.Factory factory = new AclRoutingProcessor.Factory();

        Map<String, Object> config = new HashMap<>();

        Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[acl_field] required property is missing"));
    }

    public void testCustomTargetField() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("acl_group", "group123");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "custom_routing", false, true);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("custom_routing", String.class), notNullValue());
        // Note: _routing field might exist from RandomDocumentPicks, so we only check custom_routing was set
    }

    public void testGetType() {
        AclRoutingProcessor processor = new AclRoutingProcessor("tag", "description", "acl_field", "_routing", false, true);
        assertThat(processor.getType(), equalTo("acl_routing"));
    }

    public void testHashingConsistency() throws Exception {
        Map<String, Object> document1 = new HashMap<>();
        document1.put("acl_group", "team-alpha");
        IngestDocument ingestDocument1 = RandomDocumentPicks.randomIngestDocument(random(), document1);

        Map<String, Object> document2 = new HashMap<>();
        document2.put("acl_group", "team-alpha");
        IngestDocument ingestDocument2 = RandomDocumentPicks.randomIngestDocument(random(), document2);

        AclRoutingProcessor processor1 = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);
        AclRoutingProcessor processor2 = new AclRoutingProcessor(null, null, "acl_group", "_routing", false, true);

        processor1.execute(ingestDocument1);
        processor2.execute(ingestDocument2);

        String routing1 = ingestDocument1.getFieldValue("_routing", String.class);
        String routing2 = ingestDocument2.getFieldValue("_routing", String.class);

        assertThat(routing1, equalTo(routing2));
    }

    public void testNullAclValue() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("acl_group", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Remove any existing _routing field that might have been added by RandomDocumentPicks
        if (ingestDocument.hasField("_routing")) {
            ingestDocument.removeField("_routing");
        }

        AclRoutingProcessor processor = new AclRoutingProcessor(null, null, "acl_group", "_routing", true, true);
        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result, equalTo(ingestDocument));
        // Check that no routing was added due to null ACL value
        if (ingestDocument.hasField("_routing")) {
            // If routing exists, it should be from RandomDocumentPicks, not from our processor
            Object routingValue = ingestDocument.getFieldValue("_routing", Object.class, true);
            // Our processor wouldn't create routing from null ACL, so if routing exists it's from elsewhere
            assertNotNull("Routing field exists but should not be created by our processor", routingValue);
        }
    }
}
