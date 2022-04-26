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

package org.opensearch.bootstrap;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.Runtime.Version;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class JavaVersionTests extends OpenSearchTestCase {
    public void testParse() {
        Version javaVersionEarlyAccess = Version.parse("14.0.1-ea");
        List<Integer> version14 = javaVersionEarlyAccess.version();
        assertThat(version14.size(), is(3));
        assertThat(version14.get(0), is(14));
        assertThat(version14.get(1), is(0));
        assertThat(version14.get(2), is(1));

        Version javaVersionOtherPrePart = Version.parse("13.2.4-somethingElseHere");
        List<Integer> version13 = javaVersionOtherPrePart.version();
        assertThat(version13.size(), is(3));
        assertThat(version13.get(0), is(13));
        assertThat(version13.get(1), is(2));
        assertThat(version13.get(2), is(4));

        Version javaVersionNumericPrePart = Version.parse("13.2.4-something124443");
        List<Integer> version11 = javaVersionNumericPrePart.version();
        assertThat(version11.size(), is(3));
        assertThat(version11.get(0), is(13));
        assertThat(version11.get(1), is(2));
        assertThat(version11.get(2), is(4));
    }

    public void testParseInvalidVersions() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Version.parse("11.2+something-else"));
        assertThat(e.getMessage(), equalTo("Invalid version string: '11.2+something-else'"));
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> Version.parse("11.0."));
        assertThat(e1.getMessage(), equalTo("Invalid version string: '11.0.'"));
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> Version.parse("11.a.3"));
        assertThat(e2.getMessage(), equalTo("Invalid version string: '11.a.3'"));
    }

    public void testToString() {
        Version javaVersion9 = Version.parse("9");
        assertThat(javaVersion9.toString(), is("9"));
        Version javaVersion11 = Version.parse("11.0.1-something09random");
        assertThat(javaVersion11.toString(), is("11.0.1-something09random"));
        Version javaVersion12 = Version.parse("12.2-2019");
        assertThat(javaVersion12.toString(), is("12.2-2019"));
        Version javaVersion13ea = Version.parse("13.1-ea");
        assertThat(javaVersion13ea.toString(), is("13.1-ea"));
    }

    public void testCompare() {
        Version thirteen = Version.parse("13");
        Version thirteenPointTwoPointOne = Version.parse("13.2.1");
        Version thirteenPointTwoPointOneTwoThousand = Version.parse("13.2.1-2000");
        Version thirteenPointTwoPointOneThreeThousand = Version.parse("13.2.1-3000");
        Version thirteenPointTwoPointOneA = Version.parse("13.2.1-aaa");
        Version thirteenPointTwoPointOneB = Version.parse("13.2.1-bbb");
        Version fourteen = Version.parse("14");
        Version fourteenPointTwoPointOne = Version.parse("14.2.1");
        Version fourteenPointTwoPointOneEarlyAccess = Version.parse("14.2.1-ea");

        assertTrue(thirteen.compareTo(thirteenPointTwoPointOne) < 0);
        assertTrue(thirteen.compareTo(fourteen) < 0);
        assertTrue(thirteenPointTwoPointOneThreeThousand.compareTo(thirteenPointTwoPointOneTwoThousand) > 0);
        assertTrue(thirteenPointTwoPointOneThreeThousand.compareTo(thirteenPointTwoPointOneThreeThousand) == 0);
        assertTrue(thirteenPointTwoPointOneA.compareTo(thirteenPointTwoPointOneA) == 0);
        assertTrue(thirteenPointTwoPointOneA.compareTo(thirteenPointTwoPointOneB) < 0);
        assertTrue(thirteenPointTwoPointOneA.compareTo(thirteenPointTwoPointOneThreeThousand) > 0);
        assertTrue(fourteenPointTwoPointOneEarlyAccess.compareTo(fourteenPointTwoPointOne) < 0);
        assertTrue(fourteenPointTwoPointOneEarlyAccess.compareTo(fourteen) > 0);

    }

    public void testValidVersions() {
        String[] versions = new String[] { "12-ea", "13.0.2.3-ea", "14-something", "11.0.2-21002", "11.0.14.1+1", "17.0.2+8" };
        for (String version : versions) {
            assertNotNull(Version.parse(version));
        }
    }

    public void testInvalidVersions() {
        String[] versions = new String[] { "", "1.7.0_80", "1.7.", "11.2+something-else" };
        for (String version : versions) {
            assertThrows(IllegalArgumentException.class, () -> Version.parse(version));
        }
    }
}
