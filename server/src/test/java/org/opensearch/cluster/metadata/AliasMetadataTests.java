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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.metadata.AliasMetadata.Builder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.index.model.AliasMetadataModel;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class AliasMetadataTests extends AbstractXContentTestCase<AliasMetadata> {

    public void testSerialization() throws IOException {
        final AliasMetadata before = AliasMetadata.builder("alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .routing("routing")
            .searchRouting("trim,tw , ltw , lw")
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .isHidden(randomBoolean() ? null : randomBoolean())
            .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final AliasMetadata after = new AliasMetadata(in);

        assertThat(after, equalTo(before));
    }

    public void testModelDeserialization() throws IOException {
        final AliasMetadata aliasMetadata = AliasMetadata.builder("test-alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        // Serialize using AliasMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        aliasMetadata.writeTo(out);

        // Deserialize using AliasMetadataModel
        final StreamInput in = out.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in);

        // Verify all fields match
        assertThat(model.alias(), equalTo(aliasMetadata.alias()));
        assertThat(model.indexRouting(), equalTo(aliasMetadata.indexRouting()));
        assertThat(model.searchRouting(), equalTo(aliasMetadata.searchRouting()));
        assertThat(model.searchRoutingValues(), equalTo(aliasMetadata.searchRoutingValues()));
        assertThat(model.writeIndex(), equalTo(aliasMetadata.writeIndex()));
        assertThat(model.isHidden(), equalTo(aliasMetadata.isHidden()));
        assertThat(model.filteringRequired(), equalTo(aliasMetadata.filteringRequired()));

        // Verify filter content matches
        if (aliasMetadata.filter() != null) {
            assertNotNull(model.filter());
            assertArrayEquals(aliasMetadata.filter().compressed(), model.filter().compressedBytes());
        } else {
            assertNull(model.filter());
        }
    }

    public void testModelDeserializationWithNullValues() throws IOException {
        final AliasMetadata aliasMetadata = AliasMetadata.builder("minimal-alias").build();

        // Serialize using AliasMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        aliasMetadata.writeTo(out);

        // Deserialize using AliasMetadataModel
        final StreamInput in = out.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in);

        // Verify all fields match (including nulls)
        assertThat(model.alias(), equalTo(aliasMetadata.alias()));
        assertNull(model.filter());
        assertNull(model.indexRouting());
        assertNull(model.searchRouting());
        assertTrue(model.searchRoutingValues().isEmpty());
        assertNull(model.writeIndex());
        assertNull(model.isHidden());
        assertFalse(model.filteringRequired());
    }

    public void testModelToMetadataSerialization() throws IOException {
        // Create an AliasMetadata, serialize it
        final AliasMetadata original = AliasMetadata.builder("test-alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as AliasMetadataModel
        final StreamInput in1 = out1.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in1);

        // Serialize the model
        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        // Deserialize as AliasMetadata
        final StreamInput in2 = out2.bytes().streamInput();
        final AliasMetadata restored = new AliasMetadata(in2);

        // Verify round-trip preserves data
        assertThat(restored.alias(), equalTo(original.alias()));
        assertThat(restored.indexRouting(), equalTo(original.indexRouting()));
        assertThat(restored.searchRouting(), equalTo(original.searchRouting()));
        assertThat(restored.searchRoutingValues(), equalTo(original.searchRoutingValues()));
        assertThat(restored.writeIndex(), equalTo(original.writeIndex()));
        assertThat(restored.isHidden(), equalTo(original.isHidden()));
        assertThat(restored.filteringRequired(), equalTo(original.filteringRequired()));

        // Verify filter content matches
        if (original.filter() != null) {
            assertNotNull(restored.filter());
            assertArrayEquals(restored.filter().compressed(), original.filter().compressed());
        } else {
            assertNull(restored.filter());
        }
    }

    public void testModelToMetadataSerializationWithNullValues() throws IOException {
        // Create a minimal AliasMetadata with null optional fields
        final AliasMetadata original = AliasMetadata.builder("minimal-alias").build();

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as AliasMetadataModel
        final StreamInput in1 = out1.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in1);

        // Serialize the model
        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        // Deserialize as AliasMetadata
        final StreamInput in2 = out2.bytes().streamInput();
        final AliasMetadata restored = new AliasMetadata(in2);

        // Verify round-trip preserves data (including nulls)
        assertThat(restored.alias(), equalTo(original.alias()));
        assertNull(restored.filter());
        assertNull(restored.indexRouting());
        assertNull(restored.searchRouting());
        assertTrue(restored.searchRoutingValues().isEmpty());
        assertNull(restored.writeIndex());
        assertNull(restored.isHidden());
        assertFalse(restored.filteringRequired());
    }

    public void testModelAccessor() throws IOException {
        final AliasMetadata aliasMetadata = AliasMetadata.builder("test-alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        // Verify model() accessor returns the underlying model
        AliasMetadataModel model = aliasMetadata.model();
        assertNotNull(model);
        assertThat(model.alias(), equalTo("test-alias"));
        assertThat(model.indexRouting(), equalTo("indexRouting"));
        assertThat(model.searchRouting(), equalTo("routing1,routing2"));
        assertThat(model.writeIndex(), equalTo(true));
        assertThat(model.isHidden(), equalTo(false));
        assertTrue(model.filteringRequired());
    }

    public void testConstructorFromModel() throws IOException {
        // Create a model directly
        final AliasMetadata original = AliasMetadata.builder("model-alias")
            .filter("{ \"term\": \"bar\"}")
            .indexRouting("idx-routing")
            .searchRouting("search1,search2")
            .writeIndex(false)
            .isHidden(true)
            .build();

        // Get the model and create new AliasMetadata from it
        AliasMetadataModel model = original.model();
        AliasMetadata fromModel = new AliasMetadata.Builder(model).build();

        // Verify all fields match
        assertThat(fromModel.alias(), equalTo(original.alias()));
        assertThat(fromModel.indexRouting(), equalTo(original.indexRouting()));
        assertThat(fromModel.searchRouting(), equalTo(original.searchRouting()));
        assertThat(fromModel.searchRoutingValues(), equalTo(original.searchRoutingValues()));
        assertThat(fromModel.writeIndex(), equalTo(original.writeIndex()));
        assertThat(fromModel.isHidden(), equalTo(original.isHidden()));
        assertThat(fromModel.filteringRequired(), equalTo(original.filteringRequired()));

        // Verify equals
        assertThat(fromModel, equalTo(original));
    }

    public void testModelRoundTrip() throws IOException {
        final AliasMetadata original = AliasMetadata.builder("roundtrip-alias")
            .filter("{ \"match\": { \"field\": \"value\" }}")
            .indexRouting("idx")
            .searchRouting("s1,s2,s3")
            .writeIndex(true)
            .isHidden(false)
            .build();

        // Serialize AliasMetadata
        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as AliasMetadataModel
        final StreamInput in1 = out1.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in1);

        // Create new AliasMetadata from model
        AliasMetadata fromModel = new AliasMetadata.Builder(model).build();

        // Serialize the new AliasMetadata
        final BytesStreamOutput out2 = new BytesStreamOutput();
        fromModel.writeTo(out2);

        // Deserialize back to AliasMetadata
        final StreamInput in2 = out2.bytes().streamInput();
        final AliasMetadata restored = new AliasMetadata(in2);

        // Verify round-trip preserves all data
        assertThat(restored, equalTo(original));
    }

    @Override
    protected void assertEqualInstances(AliasMetadata expectedInstance, AliasMetadata newInstance) {
        assertNotSame(newInstance, expectedInstance);
        if (expectedInstance.writeIndex() == null) {
            expectedInstance = AliasMetadata.builder(expectedInstance.alias())
                .filter(expectedInstance.filter())
                .indexRouting(expectedInstance.indexRouting())
                .searchRouting(expectedInstance.searchRouting())
                .writeIndex(randomBoolean() ? null : randomBoolean())
                .isHidden(randomBoolean() ? null : randomBoolean())
                .build();
        }
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    @Override
    protected AliasMetadata createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("") // do not add elements at the top-level as any element at this level is parsed as a new alias
            || p.contains(".filter"); // do not insert random data into AliasMetadata#filter
    }

    @Override
    protected AliasMetadata doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken());
        AliasMetadata aliasMetadata = AliasMetadata.Builder.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return aliasMetadata;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    private static AliasMetadata createTestItem() {
        Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        builder.writeIndex(randomBoolean());

        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        return builder.build();
    }

}
