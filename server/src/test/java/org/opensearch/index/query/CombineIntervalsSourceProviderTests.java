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
import java.util.List;

import static org.opensearch.index.query.IntervalsSourceProvider.Combine;

public class CombineIntervalsSourceProviderTests extends AbstractSerializingTestCase<Combine> {

    @Override
    protected Combine createTestInstance() {
        return IntervalQueryBuilderTests.createRandomCombine(0, randomBoolean());
    }

    @Override
    protected Combine mutateInstance(Combine instance) throws IOException {
        List<IntervalsSourceProvider> subSources = instance.getSubSources();
        IntervalMode mode = instance.getMode();
        int maxGaps = instance.getMaxGaps();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        switch (between(0, 3)) {
            case 0:
                subSources = subSources == null
                    ? IntervalQueryBuilderTests.createRandomSourceList(0, randomBoolean(), randomInt(5) + 1)
                    : null;
                break;
            case 1:
                if (mode == IntervalMode.ORDERED) {
                    mode = randomBoolean() ? IntervalMode.UNORDERED : IntervalMode.UNORDERED_NO_OVERLAP;
                } else if (mode == IntervalMode.UNORDERED) {
                    mode = randomBoolean() ? IntervalMode.ORDERED : IntervalMode.UNORDERED_NO_OVERLAP;
                } else {
                    mode = randomBoolean() ? IntervalMode.UNORDERED : IntervalMode.ORDERED;
                }
                break;
            case 2:
                maxGaps++;
                break;
            case 3:
                filter = filter == null
                    ? IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean())
                    : FilterIntervalsSourceProviderTests.mutateFilter(filter);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Combine(subSources, mode, maxGaps, filter);
    }

    @Override
    protected Writeable.Reader<Combine> instanceReader() {
        return Combine::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Combine doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Combine combine = (Combine) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return combine;
    }
}
