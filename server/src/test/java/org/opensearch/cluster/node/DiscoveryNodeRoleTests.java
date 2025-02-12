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

package org.opensearch.cluster.node;

import org.opensearch.common.settings.Setting;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class DiscoveryNodeRoleTests extends OpenSearchTestCase {

    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNames() {
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DiscoveryNode.setAdditionalRoles(new HashSet<>(Arrays.asList(new DiscoveryNodeRole("foo", "f") {

                @Override
                public Setting<Boolean> legacySetting() {
                    return null;
                }

            }, new DiscoveryNodeRole("foo", "f") {

                @Override
                public Setting<Boolean> legacySetting() {
                    return null;
                }

            })))
        );
        assertThat(e, hasToString(containsString("Duplicate key")));
    }

    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNameAbbreviations() {
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DiscoveryNode.setAdditionalRoles(new HashSet<>(Arrays.asList(new DiscoveryNodeRole("foo_1", "f") {

                @Override
                public Setting<Boolean> legacySetting() {
                    return null;
                }

            }, new DiscoveryNodeRole("foo_2", "f") {

                @Override
                public Setting<Boolean> legacySetting() {
                    return null;
                }

            })))
        );
        assertThat(e, hasToString(containsString("Duplicate key")));
    }

    public void testDiscoveryNodeRoleEqualsHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new DiscoveryNodeRole.UnknownRole(randomAlphaOfLength(10), randomAlphaOfLength(1), randomBoolean()),
            r -> new DiscoveryNodeRole.UnknownRole(r.roleName(), r.roleNameAbbreviation(), r.canContainData()),
            r -> {
                final int value = randomIntBetween(0, 2);
                switch (value) {
                    case 0:
                        return new DiscoveryNodeRole.UnknownRole(
                            randomAlphaOfLength(21 - r.roleName().length()),
                            r.roleNameAbbreviation(),
                            r.canContainData()
                        );
                    case 1:
                        return new DiscoveryNodeRole.UnknownRole(
                            r.roleName(),
                            randomAlphaOfLength(3 - r.roleNameAbbreviation().length()),
                            r.canContainData()
                        );
                    case 2:
                        return new DiscoveryNodeRole.UnknownRole(r.roleName(), r.roleNameAbbreviation(), r.canContainData() == false);
                    default:
                        throw new AssertionError("unexpected value [" + value + "] not between 0 and 2");
                }
            }
        );

    }

    public void testUnknownRoleIsDistinctFromKnownOrDynamicRoles() {
        for (DiscoveryNodeRole buildInRole : DiscoveryNodeRole.BUILT_IN_ROLES) {
            final DiscoveryNodeRole.UnknownRole unknownDataRole = new DiscoveryNodeRole.UnknownRole(
                buildInRole.roleName(),
                buildInRole.roleNameAbbreviation(),
                buildInRole.canContainData()
            );
            assertNotEquals(buildInRole, unknownDataRole);
            assertNotEquals(buildInRole.toString(), unknownDataRole.toString());
            final DiscoveryNodeRole.DynamicRole dynamicRole = new DiscoveryNodeRole.DynamicRole(
                buildInRole.roleName(),
                buildInRole.roleNameAbbreviation(),
                buildInRole.canContainData()
            );
            assertNotEquals(buildInRole, dynamicRole);
            assertNotEquals(buildInRole.toString(), dynamicRole.toString());
            assertNotEquals(unknownDataRole, dynamicRole);
            assertNotEquals(unknownDataRole.toString(), dynamicRole.toString());
        }
    }

    /**
     * Validate the method can identify the role as cluster-manager.
     * Remove along with MASTER_ROLE.
     */
    public void testIsClusterManager() {
        assertTrue(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.isClusterManager());
        assertTrue(DiscoveryNodeRole.MASTER_ROLE.isClusterManager());
        assertFalse(randomFrom(DiscoveryNodeRole.DATA_ROLE.isClusterManager(), DiscoveryNodeRole.INGEST_ROLE.isClusterManager()));
    }

    public void testRoleNameIsCaseInsensitive() {
        String roleName = "TestRole";
        String roleNameAbbreviation = "T";
        DiscoveryNodeRole unknownRole = new DiscoveryNodeRole.UnknownRole(roleName, roleNameAbbreviation, false);
        assertEquals(roleName.toLowerCase(Locale.ROOT), unknownRole.roleName());
        assertEquals(roleNameAbbreviation.toLowerCase(Locale.ROOT), unknownRole.roleNameAbbreviation());
        DiscoveryNodeRole dynamicRole = new DiscoveryNodeRole.DynamicRole(roleName, roleNameAbbreviation, false);
        assertEquals(roleName.toLowerCase(Locale.ROOT), dynamicRole.roleName());
        assertEquals(roleNameAbbreviation.toLowerCase(Locale.ROOT), dynamicRole.roleNameAbbreviation());
    }
}
