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
import org.opensearch.common.io.stream.BytesStreamOutput;
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

import java.util.Arrays;
import java.util.Collections;

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

    /** Enforcement is a new optional field &mdash; the historical two-arg
     *  constructor (and {@link AliasFilter#EMPTY}) must default it to
     *  {@link AliasFilter.Enforcement#POST_FILTER} so no existing caller
     *  changes behavior. */
    public void testDefaultEnforcementIsPostFilter() {
        final AliasFilter defaulted = new AliasFilter(QueryBuilders.termQuery("f", "v"), "a0");
        assertThat(defaulted.getEnforcement(), equalTo(AliasFilter.Enforcement.POST_FILTER));
        assertThat(AliasFilter.EMPTY.getEnforcement(), equalTo(AliasFilter.Enforcement.POST_FILTER));
    }

    /** A round trip at the current wire version preserves every field, including the enforcement mode. */
    public void testSerializationRoundTrip() throws Exception {
        final AliasFilter original = new AliasFilter(
            QueryBuilders.termQuery("field", "value"),
            AliasFilter.Enforcement.PRE_FILTER,
            "alias_0",
            "alias_1"
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), NAMED_WRITEABLES)) {
                final AliasFilter roundTripped = new AliasFilter(in);
                assertEquals(original, roundTripped);
                assertThat(roundTripped.getEnforcement(), equalTo(AliasFilter.Enforcement.PRE_FILTER));
                assertArrayEquals(original.getAliases(), roundTripped.getAliases());
            }
        }
    }

    /** Against a node that predates the field, it is not on the wire and the receiver defaults to POST_FILTER. */
    public void testBackwardCompatibilityBeforeEnforcementVersion() throws Exception {
        // The newest release without the field, which is what a mixed-version cluster during upgrade contains.
        final Version legacy = Version.V_3_9_0;
        assertTrue("test premise: legacy predates the gate", legacy.before(AliasFilter.ENFORCEMENT_VERSION));

        final AliasFilter original = new AliasFilter(
            QueryBuilders.termQuery("field", "value"),
            AliasFilter.Enforcement.PRE_FILTER,   // sender records PRE_FILTER
            "alias_0"
        );
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(legacy);
        original.writeTo(out);

        try (StreamInput raw = out.bytes().streamInput(); StreamInput in = new NamedWriteableAwareStreamInput(raw, NAMED_WRITEABLES)) {
            in.setVersion(legacy);
            final AliasFilter roundTripped = new AliasFilter(in);
            // Old wire format carries no enforcement -> receiver defaults to POST_FILTER.
            assertThat(roundTripped.getEnforcement(), equalTo(AliasFilter.Enforcement.POST_FILTER));
            assertArrayEquals(original.getAliases(), roundTripped.getAliases());
        }
    }

    /** Enforcement participates in equality &mdash; two otherwise-identical
     *  filters that differ only in enforcement are not equal. */
    public void testEnforcementParticipatesInEquality() {
        final QueryBuilder filter = QueryBuilders.termQuery("f", "v");
        final AliasFilter post = new AliasFilter(filter, AliasFilter.Enforcement.POST_FILTER, "a");
        final AliasFilter pre = new AliasFilter(filter, AliasFilter.Enforcement.PRE_FILTER, "a");
        assertNotEquals(post, pre);
        assertNotEquals(post.hashCode(), pre.hashCode());
    }

    public void testEnforcementFromString() {
        for (AliasFilter.Enforcement e : AliasFilter.Enforcement.values()) {
            assertSame(e, AliasFilter.Enforcement.fromString(e.value()));
        }
        assertSame(
            "an alias without a mode keeps the historical behaviour",
            AliasFilter.Enforcement.POST_FILTER,
            AliasFilter.Enforcement.fromString(null)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AliasFilter.Enforcement.fromString("filter_stats"));
        assertThat(e.getMessage(), containsString("unknown alias enforcement [filter_stats]"));
    }

    public void testEnforcementRoundTrip() throws Exception {
        for (AliasFilter.Enforcement e : AliasFilter.Enforcement.values()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                e.writeTo(out);
                assertSame(e, AliasFilter.Enforcement.readFrom(out.bytes().streamInput()));
            }
        }
    }

    public void testEnforcementIsPartOfIdentityAndSurvivesRewrite() throws Exception {
        AliasFilter post = new AliasFilter(QueryBuilders.termQuery("f", "v"), AliasFilter.Enforcement.POST_FILTER, "a");
        AliasFilter stats = new AliasFilter(QueryBuilders.termQuery("f", "v"), AliasFilter.Enforcement.FILTERED_STATS, "a");
        assertNotEquals(post, stats);
        assertNotEquals(post.hashCode(), stats.hashCode());
        assertThat(stats.toString(), containsString("enforcement=FILTERED_STATS"));
        assertSame(
            AliasFilter.Enforcement.POST_FILTER,
            new AliasFilter(QueryBuilders.termQuery("f", "v"), (AliasFilter.Enforcement) null, "a").getEnforcement()
        );

        // A rewrite that changes the query must keep the mode, or a rewritten alias would score over the whole shard.
        AliasFilter rewritable = new AliasFilter(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("f", "v")),
            AliasFilter.Enforcement.FILTERED_STATS,
            "a"
        );
        AliasFilter rewritten = rewritable.rewrite(new BaseQueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> 0L));
        assertSame(AliasFilter.Enforcement.FILTERED_STATS, rewritten.getEnforcement());
    }

}
