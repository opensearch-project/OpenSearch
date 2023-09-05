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

package org.opensearch.client.indices.rollover;

import org.opensearch.action.admin.indices.rollover.Condition;
import org.opensearch.action.admin.indices.rollover.MaxAgeCondition;
import org.opensearch.action.admin.indices.rollover.MaxDocsCondition;
import org.opensearch.action.admin.indices.rollover.MaxSizeCondition;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.opensearch.test.AbstractXContentTestCase.xContentTester;

public class RolloverResponseTests extends OpenSearchTestCase {

    private static final List<Supplier<Condition<?>>> conditionSuppliers = new ArrayList<>();
    static {
        conditionSuppliers.add(() -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
    }

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            RolloverResponseTests::createTestInstance,
            RolloverResponseTests::toXContent,
            RolloverResponse::fromXContent
        ).supportsUnknownFields(true).randomFieldsExcludeFilter(getRandomFieldsExcludeFilter()).test();
    }

    private static RolloverResponse createTestInstance() {
        final String oldIndex = randomAlphaOfLength(8);
        final String newIndex = randomAlphaOfLength(8);
        final boolean dryRun = randomBoolean();
        final boolean rolledOver = randomBoolean();
        final boolean acknowledged = randomBoolean();
        final boolean shardsAcknowledged = acknowledged && randomBoolean();

        Map<String, Boolean> results = new HashMap<>();
        int numResults = randomIntBetween(0, 3);
        List<Supplier<Condition<?>>> conditions = randomSubsetOf(numResults, conditionSuppliers);
        conditions.forEach(condition -> results.put(condition.get().name(), randomBoolean()));

        return new RolloverResponse(oldIndex, newIndex, results, dryRun, rolledOver, acknowledged, shardsAcknowledged);
    }

    private Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("conditions");
    }

    private static void toXContent(RolloverResponse response, XContentBuilder builder) throws IOException {
        org.opensearch.action.admin.indices.rollover.RolloverResponse serverResponse =
            new org.opensearch.action.admin.indices.rollover.RolloverResponse(
                response.getOldIndex(),
                response.getNewIndex(),
                response.getConditionStatus(),
                response.isDryRun(),
                response.isRolledOver(),
                response.isAcknowledged(),
                response.isShardsAcknowledged()
            );
        serverResponse.toXContent(builder, null);
    }
}
