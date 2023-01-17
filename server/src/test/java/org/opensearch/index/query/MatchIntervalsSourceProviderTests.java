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

package org.opensearch.index.query;

import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.opensearch.index.query.IntervalsSourceProvider.Match;

public class MatchIntervalsSourceProviderTests extends AbstractSerializingTestCase<Match> {

    @Override
    protected Match createTestInstance() {
        return IntervalQueryBuilderTests.createRandomMatch(0, randomBoolean());
    }

    @Override
    protected Match mutateInstance(Match instance) throws IOException {
        String query = instance.getQuery();
        int maxGaps = instance.getMaxGaps();
        IntervalMode mode = instance.getMode();
        String analyzer = instance.getAnalyzer();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        String useField = instance.getUseField();
        switch (between(0, 5)) {
            case 0:
                query = randomAlphaOfLength(query.length() + 3);
                break;
            case 1:
                maxGaps++;
                break;
            case 2:
                if (mode == IntervalMode.ORDERED) {
                    mode = randomBoolean() ? IntervalMode.UNORDERED : IntervalMode.UNORDERED_NO_OVERLAP;
                } else if (mode == IntervalMode.UNORDERED) {
                    mode = randomBoolean() ? IntervalMode.ORDERED : IntervalMode.UNORDERED_NO_OVERLAP;
                } else {
                    mode = randomBoolean() ? IntervalMode.UNORDERED : IntervalMode.ORDERED;
                }
                break;
            case 3:
                analyzer = analyzer == null ? randomAlphaOfLength(5) : null;
                break;
            case 4:
                filter = filter == null
                    ? IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean())
                    : FilterIntervalsSourceProviderTests.mutateFilter(filter);
                break;
            case 5:
                useField = useField == null ? randomAlphaOfLength(5) : (useField + "foo");
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Match(query, maxGaps, mode, analyzer, filter, useField);
    }

    @Override
    protected Writeable.Reader<Match> instanceReader() {
        return Match::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Match doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Match Match = (Match) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return Match;
    }
}
