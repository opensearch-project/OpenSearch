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

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.XContentTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RolloverRequestTests extends OpenSearchTestCase {
    private NamedWriteableRegistry writeableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        writeableRegistry = new NamedWriteableRegistry(new IndicesModule(Collections.emptyList()).getNamedWriteables());
    }

    public void testConditionsParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
            .field("max_age", "10d")
            .field("max_docs", 100)
            .field("max_size", "45gb")
            .endObject()
            .endObject();
        request.fromXContent(createParser(builder));
        Map<String, Condition<?>> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(3));
        MaxAgeCondition maxAgeCondition = (MaxAgeCondition) conditions.get(MaxAgeCondition.NAME);
        assertThat(maxAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
        MaxDocsCondition maxDocsCondition = (MaxDocsCondition) conditions.get(MaxDocsCondition.NAME);
        assertThat(maxDocsCondition.value, equalTo(100L));
        MaxSizeCondition maxSizeCondition = (MaxSizeCondition) conditions.get(MaxSizeCondition.NAME);
        assertThat(maxSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(45)));
    }

    public void testParsingWithIndexSettings() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
            .field("max_age", "10d")
            .field("max_docs", 100)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "string")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .endObject()
            .startObject("settings")
            .field("number_of_shards", 10)
            .endObject()
            .startObject("aliases")
            .startObject("alias1")
            .endObject()
            .endObject()
            .endObject();
        request.fromXContent(createParser(builder));
        Map<String, Condition<?>> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(2));
        assertThat(request.getCreateIndexRequest().mappings(), containsString("not_analyzed"));
        assertThat(request.getCreateIndexRequest().aliases().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().settings().getAsInt("number_of_shards", 0), equalTo(10));
    }

    public void testTypelessMappingParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        request.fromXContent(createParser(builder));

        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        String mapping = createIndexRequest.mappings();
        assertNotNull(mapping);

        Map<String, Object> parsedMapping = XContentHelper.convertToMap(new BytesArray(mapping), false, MediaTypeRegistry.JSON).v2();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) parsedMapping.get(MapperService.SINGLE_MAPPING_NAME);
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    public void testCopyMappingsParsingPreservesPresence() throws Exception {
        final RolloverRequest omitted = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        assertNull(omitted.getCopyMappings());
        assertFalse(omitted.isCopyMappings());

        final RolloverRequest enabled = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        enabled.fromXContent(createParser(XContentFactory.jsonBuilder().startObject().field("copy_mappings", true).endObject()));
        assertEquals(Boolean.TRUE, enabled.getCopyMappings());
        assertTrue(enabled.isCopyMappings());

        final RolloverRequest disabled = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        disabled.fromXContent(createParser(XContentFactory.jsonBuilder().startObject().field("copy_mappings", false).endObject()));
        assertEquals(Boolean.FALSE, disabled.getCopyMappings());
        assertFalse(disabled.isCopyMappings());
    }

    public void testSerialize() throws Exception {
        RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
        originalRequest.addMaxIndexDocsCondition(randomNonNegativeLong());
        originalRequest.addMaxIndexAgeCondition(TimeValue.timeValueNanos(randomNonNegativeLong()));
        originalRequest.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong()));
        originalRequest.copyMappings(true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), writeableRegistry)) {
                RolloverRequest cloneRequest = new RolloverRequest(in);
                assertThat(cloneRequest.getNewIndexName(), equalTo(originalRequest.getNewIndexName()));
                assertThat(cloneRequest.getRolloverTarget(), equalTo(originalRequest.getRolloverTarget()));
                assertEquals(Boolean.TRUE, cloneRequest.getCopyMappings());
                assertTrue(cloneRequest.isCopyMappings());
                for (Map.Entry<String, Condition<?>> entry : cloneRequest.getConditions().entrySet()) {
                    Condition<?> condition = originalRequest.getConditions().get(entry.getKey());
                    // here we compare the string representation as there is some information loss when serializing
                    // and de-serializing MaxAgeCondition
                    assertEquals(condition.toString(), entry.getValue().toString());
                }
            }
        }
    }

    public void testCopyMappingsSerializationPreservesOmittedAndExplicitFalse() throws Exception {
        for (Boolean copyMappings : new Boolean[] { null, Boolean.FALSE }) {
            RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
            if (copyMappings != null) {
                originalRequest.copyMappings(copyMappings);
            }

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                originalRequest.writeTo(out);
                try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writeableRegistry)) {
                    RolloverRequest cloneRequest = new RolloverRequest(in);
                    assertEquals(copyMappings, cloneRequest.getCopyMappings());
                }
            }
        }
    }

    public void testCopyMappingsSerializationBwc() throws Exception {
        for (Version version : new Version[] {
            Version.V_2_19_0,
            Version.V_3_4_0,
            Version.V_3_7_0,
            Version.V_3_8_0,
            Version.V_3_9_0,
            Version.V_3_10_0 }) {
            RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
            originalRequest.addMaxIndexDocsCondition(42);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                originalRequest.writeTo(out);
                final LegacyRolloverRequest legacyRequest;
                try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writeableRegistry)) {
                    in.setVersion(version);
                    legacyRequest = new LegacyRolloverRequest(in);
                    assertEquals("Legacy readers must consume the complete request", 0, in.available());
                    assertEquals(originalRequest.getRolloverTarget(), legacyRequest.rolloverTarget);
                    assertEquals(originalRequest.getNewIndexName(), legacyRequest.newIndexName);
                    assertEquals("_na_", legacyRequest.createIndexRequest.index());
                }
                try (BytesStreamOutput forwarded = new BytesStreamOutput()) {
                    forwarded.setVersion(version);
                    legacyRequest.writeTo(forwarded);
                    final RolloverRequest received;
                    try (StreamInput in = new NamedWriteableAwareStreamInput(forwarded.bytes().streamInput(), writeableRegistry)) {
                        in.setVersion(version);
                        received = new RolloverRequest(in);
                        assertNull(received.getCopyMappings());
                        assertEquals(0, in.available());
                    }
                    try (BytesStreamOutput current = new BytesStreamOutput()) {
                        received.writeTo(current);
                        try (StreamInput in = new NamedWriteableAwareStreamInput(current.bytes().streamInput(), writeableRegistry)) {
                            final RolloverRequest clone = new RolloverRequest(in);
                            assertNull(clone.getCopyMappings());
                            assertEquals(originalRequest.getRolloverTarget(), clone.getRolloverTarget());
                            assertEquals(42L, clone.getConditions().get(MaxDocsCondition.NAME).value);
                            assertEquals(0, in.available());
                        }
                    }
                }
            }
        }
    }

    public void testExplicitCopyMappingsRejectedByOlderTransportVersions() throws Exception {
        for (Version version : new Version[] { Version.V_2_19_0, Version.V_3_7_0, Version.V_3_9_0, Version.V_3_10_0 }) {
            for (boolean copyMappings : new boolean[] { false, true }) {
                RolloverRequest request = new RolloverRequest("alias-index", "new-index-name");
                request.copyMappings(copyMappings);
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    out.setVersion(version);
                    final IllegalStateException exception = expectThrows(IllegalStateException.class, () -> request.writeTo(out));
                    assertEquals("copy_mappings requires nodes on or after version [3.11.0]", exception.getMessage());
                    assertEquals(0, out.size());
                }
            }
        }
    }

    /**
     * The rollover transport layout before 3.11, without a copy-mappings field.
     */
    private static class LegacyRolloverRequest extends AcknowledgedRequest<LegacyRolloverRequest> {
        private final String rolloverTarget;
        private final String newIndexName;
        private final boolean dryRun;
        private final Map<String, Condition<?>> conditions = new HashMap<>();
        private final CreateIndexRequest createIndexRequest;

        LegacyRolloverRequest(StreamInput in) throws IOException {
            super(in);
            rolloverTarget = in.readString();
            newIndexName = in.readOptionalString();
            dryRun = in.readBoolean();
            final int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                Condition<?> condition = in.readNamedWriteable(Condition.class);
                conditions.put(condition.name, condition);
            }
            createIndexRequest = new CreateIndexRequest(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return createIndexRequest.validate();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rolloverTarget);
            out.writeOptionalString(newIndexName);
            out.writeBoolean(dryRun);
            out.writeVInt(conditions.size());
            for (Condition<?> condition : conditions.values()) {
                out.writeNamedWriteable(condition);
            }
            createIndexRequest.writeTo(out);
        }
    }

    public void testUnknownFields() throws IOException {
        final RolloverRequest request = new RolloverRequest();
        XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType);
        builder.startObject();
        {
            builder.startObject("conditions");
            builder.field("max_age", "10d");
            builder.endObject();
        }
        builder.endObject();
        BytesReference mutated = XContentTestUtils.insertRandomFields(xContentType, BytesReference.bytes(builder), null, random());
        expectThrows(XContentParseException.class, () -> request.fromXContent(createParser(xContentType.xContent(), mutated)));
    }

    public void testSameConditionCanOnlyBeAddedOnce() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        Consumer<RolloverRequest> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(rolloverRequest);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(rolloverRequest));
    }

    public void testValidation() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        assertNotNull(rolloverRequest.getCreateIndexRequest());
        ActionRequestValidationException validationException = rolloverRequest.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("rollover target is missing", validationException.validationErrors().get(0));
    }

    private static List<Consumer<RolloverRequest>> conditionsGenerator = new ArrayList<>();
    static {
        conditionsGenerator.add((request) -> request.addMaxIndexDocsCondition(randomNonNegativeLong()));
        conditionsGenerator.add((request) -> request.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionsGenerator.add((request) -> request.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong())));
    }

}
