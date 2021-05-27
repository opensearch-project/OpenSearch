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

package org.opensearch.gradle;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class BwcOpenSearchVersionsTests extends GradleUnitTestCase {

    private static final Map<String, List<String>> sampleVersions = new HashMap<>();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    static {
        // unreleased major and two unreleased minors ( minor in feature freeze )
        sampleVersions.put("1.0.0", asList("7_0_0", "7_0_1", "7_1_0", "7_1_1", "7_2_0", "7_3_0", "1.0.0"));
        sampleVersions.put(
            "7.0.0-alpha1",
            asList(
                "6_0_0_alpha1",
                "6_0_0_alpha2",
                "6_0_0_beta1",
                "6_0_0_beta2",
                "6_0_0_rc1",
                "6_0_0_rc2",
                "6_0_0",
                "6_0_1",
                "6_1_0",
                "6_1_1",
                "6_1_2",
                "6_1_3",
                "6_1_4",
                "6_2_0",
                "6_2_1",
                "6_2_2",
                "6_2_3",
                "6_2_4",
                "6_3_0",
                "6_3_1",
                "6_3_2",
                "6_4_0",
                "6_4_1",
                "6_4_2",
                "6_5_0",
                "7_0_0_alpha1"
            )
        );
        sampleVersions.put(
            "6.5.0",
            asList(
                "5_0_0_alpha1",
                "5_0_0_alpha2",
                "5_0_0_alpha3",
                "5_0_0_alpha4",
                "5_0_0_alpha5",
                "5_0_0_beta1",
                "5_0_0_rc1",
                "5_0_0",
                "5_0_1",
                "5_0_2",
                "5_1_1",
                "5_1_2",
                "5_2_0",
                "5_2_1",
                "5_2_2",
                "5_3_0",
                "5_3_1",
                "5_3_2",
                "5_3_3",
                "5_4_0",
                "5_4_1",
                "5_4_2",
                "5_4_3",
                "5_5_0",
                "5_5_1",
                "5_5_2",
                "5_5_3",
                "5_6_0",
                "5_6_1",
                "5_6_2",
                "5_6_3",
                "5_6_4",
                "5_6_5",
                "5_6_6",
                "5_6_7",
                "5_6_1",
                "5_6_9",
                "5_6_10",
                "5_6_11",
                "5_6_12",
                "5_6_13",
                "6_0_0_alpha1",
                "6_0_0_alpha2",
                "6_0_0_beta1",
                "6_0_0_beta2",
                "6_0_0_rc1",
                "6_0_0_rc2",
                "6_0_0",
                "6_0_1",
                "6_1_0",
                "6_1_1",
                "6_1_2",
                "6_1_3",
                "6_1_4",
                "6_2_0",
                "6_2_1",
                "6_2_2",
                "6_2_3",
                "6_2_4",
                "6_3_0",
                "6_3_1",
                "6_3_2",
                "6_4_0",
                "6_4_1",
                "6_4_2",
                "6_5_0"
            )
        );
        sampleVersions.put(
            "6.6.0",
            asList(
                "5_0_0_alpha1",
                "5_0_0_alpha2",
                "5_0_0_alpha3",
                "5_0_0_alpha4",
                "5_0_0_alpha5",
                "5_0_0_beta1",
                "5_0_0_rc1",
                "5_0_0",
                "5_0_1",
                "5_0_2",
                "5_1_1",
                "5_1_2",
                "5_2_0",
                "5_2_1",
                "5_2_2",
                "5_3_0",
                "5_3_1",
                "5_3_2",
                "5_3_3",
                "5_4_0",
                "5_4_1",
                "5_4_2",
                "5_4_3",
                "5_5_0",
                "5_5_1",
                "5_5_2",
                "5_5_3",
                "5_6_0",
                "5_6_1",
                "5_6_2",
                "5_6_3",
                "5_6_4",
                "5_6_5",
                "5_6_6",
                "5_6_7",
                "5_6_1",
                "5_6_9",
                "5_6_10",
                "5_6_11",
                "5_6_12",
                "5_6_13",
                "6_0_0_alpha1",
                "6_0_0_alpha2",
                "6_0_0_beta1",
                "6_0_0_beta2",
                "6_0_0_rc1",
                "6_0_0_rc2",
                "6_0_0",
                "6_0_1",
                "6_1_0",
                "6_1_1",
                "6_1_2",
                "6_1_3",
                "6_1_4",
                "6_2_0",
                "6_2_1",
                "6_2_2",
                "6_2_3",
                "6_2_4",
                "6_3_0",
                "6_3_1",
                "6_3_2",
                "6_4_0",
                "6_4_1",
                "6_4_2",
                "6_5_0",
                "6_6_0"
            )
        );
        sampleVersions.put(
            "6.4.2",
            asList(
                "5_0_0_alpha1",
                "5_0_0_alpha2",
                "5_0_0_alpha3",
                "5_0_0_alpha4",
                "5_0_0_alpha5",
                "5_0_0_beta1",
                "5_0_0_rc1",
                "5_0_0",
                "5_0_1",
                "5_0_2",
                "5_1_1",
                "5_1_2",
                "5_2_0",
                "5_2_1",
                "5_2_2",
                "5_3_0",
                "5_3_1",
                "5_3_2",
                "5_3_3",
                "5_4_0",
                "5_4_1",
                "5_4_2",
                "5_4_3",
                "5_5_0",
                "5_5_1",
                "5_5_2",
                "5_5_3",
                "5_6_0",
                "5_6_1",
                "5_6_2",
                "5_6_3",
                "5_6_4",
                "5_6_5",
                "5_6_6",
                "5_6_7",
                "5_6_1",
                "5_6_9",
                "5_6_10",
                "5_6_11",
                "5_6_12",
                "5_6_13",
                "6_0_0_alpha1",
                "6_0_0_alpha2",
                "6_0_0_beta1",
                "6_0_0_beta2",
                "6_0_0_rc1",
                "6_0_0_rc2",
                "6_0_0",
                "6_0_1",
                "6_1_0",
                "6_1_1",
                "6_1_2",
                "6_1_3",
                "6_1_4",
                "6_2_0",
                "6_2_1",
                "6_2_2",
                "6_2_3",
                "6_2_4",
                "6_3_0",
                "6_3_1",
                "6_3_2",
                "6_4_0",
                "6_4_1",
                "6_4_2"
            )
        );
        sampleVersions.put("7.1.0", asList("7_1_0", "7_0_0", "6_7_0", "6_6_1", "6_6_0"));
    }

    public void testWireCompatible() {
        //assertVersionsEquals(asList("7.0.0"), getVersionCollection("1.0.0").getWireCompatible());
        //assertVersionsEquals(asList("7.3.0"), getVersionCollection("1.0.0").getWireCompatible());
    }

    public void testWireCompatibleUnreleased() {
    }

    public void testIndexCompatible() {
    }

    public void testIndexCompatibleUnreleased() {
    }

    public void testGetUnreleased() {
        assertVersionsEquals(asList("1.0.0"), getVersionCollection("1.0.0").getUnreleased());
    }

    private String formatVersionToLine(final String version) {
        return " public static final Version V_" + version.replaceAll("\\.", "_") + " ";
    }

    private void assertVersionsEquals(List<String> expected, List<Version> actual) {
        assertEquals(expected.stream().map(Version::fromString).collect(Collectors.toList()), actual);
    }

    private BwcVersions getVersionCollection(String versionString) {
        List<String> versionMap = sampleVersions.get(versionString);
        assertNotNull(versionMap);
        Version version = Version.fromString(versionString);
        assertNotNull(version);
        return new BwcVersions(
            versionMap.stream().map(this::formatVersionToLine).collect(Collectors.toList()),
            version
        );
    }
}
