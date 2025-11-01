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

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ConditionTests extends OpenSearchTestCase {

    public void testMaxAge() {
        final MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(1));

        long indexCreatedMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(61).getMillis();
        Condition.Result evaluate = maxAgeCondition.evaluate(
            new Condition.Stats.Builder().numDocs(0).indexCreated(indexCreatedMatch).indexSize(randomByteSize()).build()
        );
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(true));

        long indexCreatedNotMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(59).getMillis();
        evaluate = maxAgeCondition.evaluate(
            new Condition.Stats.Builder().numDocs(0).indexCreated(indexCreatedNotMatch).indexSize(randomByteSize()).build()
        );
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxDocs() {
        final MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);

        long maxDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = maxDocsCondition.evaluate(
            new Condition.Stats.Builder().numDocs(maxDocsMatch).indexCreated(0).indexSize(randomByteSize()).build()
        );
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(true));

        long maxDocsNotMatch = randomIntBetween(0, 99);
        evaluate = maxDocsCondition.evaluate(
            new Condition.Stats.Builder().numDocs(0).indexCreated(maxDocsNotMatch).indexSize(randomByteSize()).build()
        );
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxSize() {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 20), ByteSizeUnit.MB));

        Condition.Result result = maxSizeCondition.evaluate(
            new Condition.Stats.Builder().numDocs(randomNonNegativeLong())
                .indexCreated(randomNonNegativeLong())
                .indexSize(new ByteSizeValue(0, ByteSizeUnit.MB))
                .build()
        );
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(
            new Condition.Stats.Builder().numDocs(randomNonNegativeLong())
                .indexCreated(randomNonNegativeLong())
                .indexSize(new ByteSizeValue(randomIntBetween(0, 9), ByteSizeUnit.MB))
                .build()
        );
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(
            new Condition.Stats.Builder().numDocs(randomNonNegativeLong())
                .indexCreated(randomNonNegativeLong())
                .indexSize(new ByteSizeValue(randomIntBetween(20, 1000), ByteSizeUnit.MB))
                .build()
        );
        assertThat(result.matched, equalTo(true));
    }

    public void testEqualsAndHashCode() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxDocsCondition,
            condition -> new MaxDocsCondition(condition.value),
            condition -> new MaxDocsCondition(randomLong())
        );

        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxSizeCondition,
            condition -> new MaxSizeCondition(condition.value),
            condition -> new MaxSizeCondition(randomByteSize())
        );

        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(new TimeValue(randomNonNegativeLong()));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxAgeCondition,
            condition -> new MaxAgeCondition(condition.value),
            condition -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong()))
        );
    }

    private static ByteSizeValue randomByteSize() {
        return new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES);
    }
}
