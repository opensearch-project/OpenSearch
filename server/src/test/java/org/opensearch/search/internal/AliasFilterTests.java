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

package org.opensearch.search.internal;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BaseQueryRewriteContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class AliasFilterTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry NAMED_WRITEABLES = new NamedWriteableRegistry(
        new SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList()).getNamedWriteables()
    );

    public void testEqualsAndHashCode() {
        final QueryBuilder filter = QueryBuilders.termQuery("field", "value");
        final String[] aliases = new String[] { "alias_0", "alias_1" };
        final AliasFilter aliasFilter = new AliasFilter(filter, aliases);
        final EqualsHashCodeTestUtils.CopyFunction<AliasFilter> aliasFilterCopyFunction = x -> {
            assertThat(x.getQueryBuilder(), instanceOf(TermQueryBuilder.class));
            final BytesStreamOutput out = new BytesStreamOutput();
            x.getQueryBuilder().writeTo(out);
            final QueryBuilder otherFilter = new TermQueryBuilder(out.bytes().streamInput());
            final String[] otherAliases = Arrays.copyOf(x.getAliases(), x.getAliases().length);
            return new AliasFilter(otherFilter, otherAliases);
        };

        final EqualsHashCodeTestUtils.MutateFunction<AliasFilter> aliasFilterMutationFunction = x -> {
            assertThat(x.getQueryBuilder(), instanceOf(TermQueryBuilder.class));
            final BytesStreamOutput out = new BytesStreamOutput();
            x.getQueryBuilder().writeTo(out);
            final QueryBuilder otherFilter = new TermQueryBuilder(out.bytes().streamInput());
            assertThat(x.getAliases().length, greaterThan(0));
            final String[] otherAliases = Arrays.copyOf(x.getAliases(), x.getAliases().length - 1);
            return new AliasFilter(otherFilter, otherAliases);
        };

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(aliasFilter, aliasFilterCopyFunction, aliasFilterMutationFunction);
    }

    /** The scoring mode is a new optional field &mdash; the historical two-arg
     *  constructor (and {@link AliasFilter#EMPTY}) must default it to
     *  {@link AliasFilter.ScoringMode#UNFILTERED_STATS} so no existing caller
     *  changes behavior. */
    public void testDefaultScoringModeIsUnfilteredStats() {
        final AliasFilter defaulted = new AliasFilter(QueryBuilders.termQuery("f", "v"), "a0");
        assertThat(defaulted.getScoringMode(), equalTo(AliasFilter.ScoringMode.UNFILTERED_STATS));
        assertThat(AliasFilter.EMPTY.getScoringMode(), equalTo(AliasFilter.ScoringMode.UNFILTERED_STATS));
    }

    /** A round trip at the current wire version preserves every field, including the scoring mode. */
    public void testSerializationRoundTrip() throws Exception {
        final AliasFilter original = new AliasFilter(
            QueryBuilders.termQuery("field", "value"),
            AliasFilter.ScoringMode.NO_STATS,
            "alias_0",
            "alias_1"
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), NAMED_WRITEABLES)) {
                final AliasFilter roundTripped = new AliasFilter(in);
                assertEquals(original, roundTripped);
                assertThat(roundTripped.getScoringMode(), equalTo(AliasFilter.ScoringMode.NO_STATS));
                assertArrayEquals(original.getAliases(), roundTripped.getAliases());
            }
        }
    }

    /** Against a node that predates the field, it is not on the wire and the receiver defaults to UNFILTERED_STATS. */
    public void testBackwardCompatibilityBeforeScoringModeVersion() throws Exception {
        // The newest release without the field, which is what a mixed-version cluster during upgrade contains.
        final Version legacy = Version.V_3_9_0;
        assertTrue("test premise: legacy predates the gate", legacy.before(AliasFilter.SCORING_MODE_VERSION));

        final AliasFilter original = new AliasFilter(
            QueryBuilders.termQuery("field", "value"),
            AliasFilter.ScoringMode.NO_STATS,   // sender records NO_STATS
            "alias_0"
        );
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(legacy);
        original.writeTo(out);

        try (StreamInput raw = out.bytes().streamInput(); StreamInput in = new NamedWriteableAwareStreamInput(raw, NAMED_WRITEABLES)) {
            in.setVersion(legacy);
            final AliasFilter roundTripped = new AliasFilter(in);
            // Old wire format carries no scoring mode -> receiver defaults to UNFILTERED_STATS.
            assertThat(roundTripped.getScoringMode(), equalTo(AliasFilter.ScoringMode.UNFILTERED_STATS));
            assertArrayEquals(original.getAliases(), roundTripped.getAliases());
        }
    }

    /** The scoring mode participates in equality &mdash; two otherwise-identical
     *  filters that differ only in scoring mode are not equal. */
    public void testScoringModeParticipatesInEquality() {
        final QueryBuilder filter = QueryBuilders.termQuery("f", "v");
        final AliasFilter post = new AliasFilter(filter, AliasFilter.ScoringMode.UNFILTERED_STATS, "a");
        final AliasFilter pre = new AliasFilter(filter, AliasFilter.ScoringMode.NO_STATS, "a");
        assertNotEquals(post, pre);
        assertNotEquals(post.hashCode(), pre.hashCode());
    }

    public void testScoringModeFromString() {
        for (AliasFilter.ScoringMode e : AliasFilter.ScoringMode.values()) {
            assertSame(e, AliasFilter.ScoringMode.fromString(e.value()));
        }
        assertSame(
            "an alias without a mode keeps the historical behaviour",
            AliasFilter.ScoringMode.UNFILTERED_STATS,
            AliasFilter.ScoringMode.fromString(null)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AliasFilter.ScoringMode.fromString("filter_stats"));
        assertThat(e.getMessage(), containsString("unknown alias scoring mode [filter_stats]"));
    }

    public void testScoringModeRoundTrip() throws Exception {
        for (AliasFilter.ScoringMode e : AliasFilter.ScoringMode.values()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                e.writeTo(out);
                assertSame(e, AliasFilter.ScoringMode.readFrom(out.bytes().streamInput()));
            }
        }
    }

    public void testScoringModeIsWrittenByName() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            AliasFilter.ScoringMode.NO_STATS.writeTo(out);
            assertEquals(
                "a mode is sent by name, so adding one later needs no wire version",
                "no_stats",
                out.bytes().streamInput().readString()
            );
        }
    }

    public void testScoringModeIsPartOfIdentityAndSurvivesRewrite() throws Exception {
        AliasFilter post = new AliasFilter(QueryBuilders.termQuery("f", "v"), AliasFilter.ScoringMode.UNFILTERED_STATS, "a");
        AliasFilter stats = new AliasFilter(QueryBuilders.termQuery("f", "v"), AliasFilter.ScoringMode.FILTERED_STATS, "a");
        assertNotEquals(post, stats);
        assertNotEquals(post.hashCode(), stats.hashCode());
        assertThat(stats.toString(), containsString("scoringMode=FILTERED_STATS"));
        assertSame(
            AliasFilter.ScoringMode.UNFILTERED_STATS,
            new AliasFilter(QueryBuilders.termQuery("f", "v"), (AliasFilter.ScoringMode) null, "a").getScoringMode()
        );

        // A rewrite that changes the query must keep the mode, or a rewritten alias would score over the whole shard.
        AliasFilter rewritable = new AliasFilter(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("f", "v")),
            AliasFilter.ScoringMode.FILTERED_STATS,
            "a"
        );
        AliasFilter rewritten = rewritable.rewrite(new BaseQueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> 0L));
        assertSame(AliasFilter.ScoringMode.FILTERED_STATS, rewritten.getScoringMode());
    }

    public void testScoringModeNeedsEveryNodeToReadIt() {
        final Version older = VersionUtils.getPreviousVersion(AliasFilter.SCORING_MODE_VERSION);
        AliasFilter.ensureScoringModeSupported("a", null, older);
        AliasFilter.ensureScoringModeSupported("a", "no_stats", AliasFilter.SCORING_MODE_VERSION);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AliasFilter.ensureScoringModeSupported("a", "no_stats", older)
        );
        assertEquals(
            "[scoring_mode] on alias [a] requires all nodes on version ["
                + AliasFilter.SCORING_MODE_VERSION
                + "] or later, but a node is on ["
                + older
                + "]",
            e.getMessage()
        );
    }

    public void testEveryAliasOfATemplateIsChecked() {
        final Version older = VersionUtils.getPreviousVersion(AliasFilter.SCORING_MODE_VERSION);
        Map<String, AliasMetadata> aliases = new LinkedHashMap<>();
        aliases.put("plain", AliasMetadata.builder("plain").filter("{\"match_all\":{}}").build());
        aliases.put("scoped", AliasMetadata.builder("scoped").filter("{\"match_all\":{}}").scoringMode("no_stats").build());
        AliasFilter.ensureScoringModesSupported(aliases, AliasFilter.SCORING_MODE_VERSION);
        AliasFilter.ensureScoringModesSupported(new Template(Settings.EMPTY, null, null), older);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AliasFilter.ensureScoringModesSupported(new Template(Settings.EMPTY, null, aliases), older)
        );
        assertThat(e.getMessage(), containsString("on alias [scoped]"));
    }
}
