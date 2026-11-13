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

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata.Builder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.test.AbstractXContentTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
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

    public void testScoringModeIsPartOfIdentity() {
        AliasMetadata with = AliasMetadata.builder("alias").filter("{ \"term\": \"foo\"}").scoringMode("filtered_stats").build();
        AliasMetadata without = AliasMetadata.builder("alias").filter("{ \"term\": \"foo\"}").build();
        assertNotEquals(with, without);
        assertNotEquals(with.hashCode(), without.hashCode());
        assertEquals(with, AliasMetadata.newAliasMetadata(with, "alias"));
    }

    public void testScoringModeWithoutAFilterIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AliasMetadata.builder("alias").scoringMode("no_stats").build()
        );
        assertThat(e.getMessage(), containsString("[scoring_mode] requires a [filter] on alias [alias]"));
    }

    public void testTemplateAliasesValidateTheMode() throws IOException {
        // Composable and component templates parse their aliases through fromXContent rather than the alias API.
        String misspelled = "{\"alias\":{\"filter\":{\"term\":{\"f\":\"v\"}},\"scoring_mode\":\"no_stat\"}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, misspelled)) {
            parser.nextToken(); // the enclosing object
            parser.nextToken(); // the alias name, where fromXContent starts
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AliasMetadata.Builder.fromXContent(parser));
            assertThat(e.getMessage(), containsString("unknown alias scoring mode [no_stat]"));
        }
        String unfiltered = "{\"alias\":{\"scoring_mode\":\"no_stats\"}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, unfiltered)) {
            parser.nextToken(); // the enclosing object
            parser.nextToken(); // the alias name, where fromXContent starts
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AliasMetadata.Builder.fromXContent(parser));
            assertThat(e.getMessage(), containsString("[scoring_mode] requires a [filter]"));
        }
    }

    public void testScoringModeRoundTripsOnCurrentVersion() throws IOException {
        final AliasMetadata before = AliasMetadata.builder("alias").filter("{ \"term\": \"foo\"}").scoringMode("filtered_stats").build();
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);
        assertThat(new AliasMetadata(out.bytes().streamInput()).scoringMode(), equalTo("filtered_stats"));
    }

    public void testScoringModeIsNotSentToANodeThatPredatesIt() throws IOException {
        final AliasMetadata before = AliasMetadata.builder("alias").filter("{ \"term\": \"foo\"}").scoringMode("filtered_stats").build();
        final Version older = VersionUtils.getPreviousVersion(AliasFilter.SCORING_MODE_VERSION);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(older);
        before.writeTo(out);
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(older);
        final AliasMetadata after = new AliasMetadata(in);
        // The stream must be fully consumed, or the next field in cluster state would be read from the wrong offset.
        assertThat(in.available(), equalTo(0));
        assertNull(after.scoringMode());
        assertThat(after.alias(), equalTo("alias"));
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
                .scoringMode(expectedInstance.scoringMode())
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
        final boolean filtered = randomBoolean();
        if (filtered) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        builder.writeIndex(randomBoolean());

        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        if (filtered && randomBoolean()) {
            builder.scoringMode(randomFrom("unfiltered_stats", "no_stats", "filtered_stats"));
        }
        return builder.build();
    }

}
