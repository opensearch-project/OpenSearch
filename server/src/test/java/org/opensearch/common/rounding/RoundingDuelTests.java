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

package org.opensearch.common.rounding;

import org.opensearch.LegacyESVersion;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.joda.time.DateTimeZone;

import java.time.ZoneOffset;

import static org.hamcrest.Matchers.is;

public class RoundingDuelTests extends OpenSearchTestCase {

    // dont include nano/micro seconds as rounding would become zero then and throw an exception
    private static final String[] ALLOWED_TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m"};

    public void testSerialization() throws Exception {
        org.opensearch.common.Rounding.DateTimeUnit randomDateTimeUnit =
            randomFrom(org.opensearch.common.Rounding.DateTimeUnit.values());
        org.opensearch.common.Rounding rounding;
        boolean oldNextRoundingValueWorks;
        if (randomBoolean()) {
            rounding = org.opensearch.common.Rounding.builder(randomDateTimeUnit).timeZone(ZoneOffset.UTC).build();
            oldNextRoundingValueWorks = true;
        } else {
            rounding = org.opensearch.common.Rounding.builder(timeValue()).timeZone(ZoneOffset.UTC).build();
            oldNextRoundingValueWorks = false;
        }
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0));
        rounding.writeTo(output);

        Rounding roundingJoda = Rounding.Streams.read(output.bytes().streamInput());
        org.opensearch.common.Rounding roundingJavaTime =
            org.opensearch.common.Rounding.read(output.bytes().streamInput());

        int randomInt = randomIntBetween(1, 1_000_000_000);
        assertThat(roundingJoda.round(randomInt), is(roundingJavaTime.round(randomInt)));
        if (oldNextRoundingValueWorks) {
            assertThat(roundingJoda.nextRoundingValue(randomInt), is(roundingJavaTime.nextRoundingValue(randomInt)));
        }
    }

    public void testDuellingImplementations() {
        org.opensearch.common.Rounding.DateTimeUnit randomDateTimeUnit =
            randomFrom(org.opensearch.common.Rounding.DateTimeUnit.values());
        org.opensearch.common.Rounding.Prepared rounding;
        Rounding roundingJoda;

        if (randomBoolean()) {
            rounding = org.opensearch.common.Rounding.builder(randomDateTimeUnit).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
            DateTimeUnit dateTimeUnit = DateTimeUnit.resolve(randomDateTimeUnit.getId());
            roundingJoda = Rounding.builder(dateTimeUnit).timeZone(DateTimeZone.UTC).build();
        } else {
            TimeValue interval = timeValue();
            rounding = org.opensearch.common.Rounding.builder(interval).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
            roundingJoda = Rounding.builder(interval).timeZone(DateTimeZone.UTC).build();
        }

        long roundValue = randomLong();
        assertThat(roundingJoda.round(roundValue), is(rounding.round(roundValue)));
    }

    static TimeValue timeValue() {
        return TimeValue.parseTimeValue(randomIntBetween(1, 1000) + randomFrom(ALLOWED_TIME_SUFFIXES), "settingName");
    }
}
