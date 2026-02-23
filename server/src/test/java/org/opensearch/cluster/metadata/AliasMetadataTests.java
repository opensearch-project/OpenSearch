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

    // --- Serialization tests ---

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

    // --- Model interop tests ---

    public void testModelDeserialization() throws IOException {
        final AliasMetadata aliasMetadata = AliasMetadata.builder("test-alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        final BytesStreamOutput out = new BytesStreamOutput();
        aliasMetadata.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in);

        assertThat(model.alias(), equalTo(aliasMetadata.alias()));
        assertThat(model.indexRouting(), equalTo(aliasMetadata.indexRouting()));
        assertThat(model.searchRouting(), equalTo(aliasMetadata.searchRouting()));
        assertThat(model.searchRoutingValues(), equalTo(aliasMetadata.searchRoutingValues()));
        assertThat(model.writeIndex(), equalTo(aliasMetadata.writeIndex()));
        assertThat(model.isHidden(), equalTo(aliasMetadata.isHidden()));
        assertThat(model.filteringRequired(), equalTo(aliasMetadata.filteringRequired()));

        if (aliasMetadata.filter() != null) {
            assertNotNull(model.filter());
            assertArrayEquals(aliasMetadata.filter().compressed(), model.filter().compressedBytes());
        } else {
            assertNull(model.filter());
        }
    }

    public void testModelDeserializationWithNullValues() throws IOException {
        final AliasMetadata aliasMetadata = AliasMetadata.builder("minimal-alias").build();

        final BytesStreamOutput out = new BytesStreamOutput();
        aliasMetadata.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in);

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
        final AliasMetadata original = AliasMetadata.builder("test-alias")
            .filter("{ \"term\": \"foo\"}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        final StreamInput in1 = out1.bytes().streamInput();
        final AliasMetadataModel model = new AliasMetadataModel(in1);

        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        final StreamInput in2 = out2.bytes().streamInput();
        final AliasMetadata restored = new AliasMetadata(in2);

        assertThat(restored.alias(), equalTo(original.alias()));
        assertThat(restored.indexRouting(), equalTo(original.indexRouting()));
        assertThat(restored.searchRouting(), equalTo(original.searchRouting()));
        assertThat(restored.writeIndex(), equalTo(original.writeIndex()));
        assertThat(restored.isHidden(), equalTo(original.isHidden()));
    }

    public void testConstructorFromModel() throws IOException {
        final AliasMetadata original = AliasMetadata.builder("model-alias")
            .filter("{ \"term\": \"bar\"}")
            .indexRouting("idx-routing")
            .searchRouting("search1,search2")
            .writeIndex(false)
            .isHidden(true)
            .build();

        AliasMetadata fromModel = new AliasMetadata.Builder(original.model()).build();

        assertThat(fromModel, equalTo(original));
    }

    public void testModelAccessor() {
        final AliasMetadata am = AliasMetadata.builder("test-alias")
            .filter("{\"term\":{\"year\":2016}}")
            .indexRouting("indexRouting")
            .searchRouting("routing1,routing2")
            .writeIndex(true)
            .isHidden(false)
            .build();

        AliasMetadataModel model = am.model();
        assertNotNull(model);
        assertThat(model.alias(), equalTo("test-alias"));
        assertThat(model.indexRouting(), equalTo("indexRouting"));
        assertTrue(model.filteringRequired());
    }

    // --- Getter alias tests ---

    public void testGetterAliases() {
        AliasMetadata am = AliasMetadata.builder("a").filter("{\"term\":{\"x\":1}}").indexRouting("ir").searchRouting("sr").build();

        assertEquals(am.alias(), am.getAlias());
        assertEquals(am.filter(), am.getFilter());
        assertEquals(am.indexRouting(), am.getIndexRouting());
        assertEquals(am.searchRouting(), am.getSearchRouting());
    }

    // --- newAliasMetadata (rename) test ---

    public void testNewAliasMetadata() {
        AliasMetadata original = AliasMetadata.builder("orig")
            .filter("{\"term\":{\"x\":1}}")
            .indexRouting("ir")
            .searchRouting("sr")
            .writeIndex(true)
            .isHidden(false)
            .build();

        AliasMetadata renamed = AliasMetadata.newAliasMetadata(original, "renamed");

        assertEquals("renamed", renamed.alias());
        assertEquals(original.indexRouting(), renamed.indexRouting());
        assertEquals(original.searchRouting(), renamed.searchRouting());
        assertEquals(original.writeIndex(), renamed.writeIndex());
        assertEquals(original.isHidden(), renamed.isHidden());
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        AliasMetadata m1 = AliasMetadata.builder("a").indexRouting("r").build();
        AliasMetadata m2 = AliasMetadata.builder("a").indexRouting("r").build();
        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
    }

    public void testNotEqualsNull() {
        assertNotEquals(AliasMetadata.builder("a").build(), null);
    }

    public void testNotEqualsDifferentType() {
        assertNotEquals(AliasMetadata.builder("a").build(), "a string");
    }

    public void testNotEqualsDifferentAlias() {
        assertNotEquals(AliasMetadata.builder("a").build(), AliasMetadata.builder("b").build());
    }

    // --- toString test ---

    public void testToString() {
        AliasMetadata am = AliasMetadata.builder("test-alias").indexRouting("ir").build();
        String str = am.toString();
        assertTrue(str.contains("test-alias"));
    }

    // --- AbstractXContentTestCase overrides ---

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
        return p -> p.equals("") || p.contains(".filter");
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
        if (randomBoolean()) builder.routing(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.filter("{\"term\":{\"year\":2016}}");
        builder.writeIndex(randomBoolean());
        if (randomBoolean()) builder.isHidden(randomBoolean());
        return builder.build();
    }
}
