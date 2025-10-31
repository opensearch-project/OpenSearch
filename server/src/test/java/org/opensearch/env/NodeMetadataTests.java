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

package org.opensearch.env;

import org.opensearch.Version;
import org.opensearch.common.collect.Tuple;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.nio.file.Path;

import static org.opensearch.Version.MASK;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class NodeMetadataTests extends OpenSearchTestCase {
    private Version randomVersion() {
        // VersionUtils.randomVersion() only returns known versions, which are necessarily no later than Version.CURRENT; however we want
        // also to consider our behaviour with all versions, so occasionally pick up a truly random version.
        return rarely() ? Version.fromId(randomInt()) : VersionUtils.randomVersion(random());
    }

    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new NodeMetadata(randomAlphaOfLength(10), randomVersion()), nodeMetadata -> {
            final long generation = NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, tempDir);
            final Tuple<NodeMetadata, Long> nodeMetadataLongTuple = NodeMetadata.FORMAT.loadLatestStateWithGeneration(
                logger,
                xContentRegistry(),
                tempDir
            );
            assertThat(nodeMetadataLongTuple.v2(), equalTo(generation));
            return nodeMetadataLongTuple.v1();
        }, nodeMetadata -> {
            if (randomBoolean()) {
                return new NodeMetadata(randomAlphaOfLength(21 - nodeMetadata.nodeId().length()), nodeMetadata.nodeVersion());
            } else {
                return new NodeMetadata(nodeMetadata.nodeId(), randomValueOtherThan(nodeMetadata.nodeVersion(), this::randomVersion));
            }
        });
    }

    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(
            nodeId,
            randomValueOtherThanMany(
                v -> v.after(Version.CURRENT) || v.before(Version.CURRENT.minimumIndexCompatibilityVersion()),
                this::randomVersion
            )
        ).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId, Version.V_EMPTY).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testDoesNotUpgradeFutureVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooNewVersion()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node from version ["), endsWith("] to version [" + Version.CURRENT + "]"))
        );
    }

    public void testDoesNotUpgradeAncientVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooOldVersion()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(startsWith("cannot upgrade a node from version ["), endsWith("] directly to version [" + Version.CURRENT + "]"))
        );
    }

    public static Version tooNewVersion() {
        return Version.fromId(between(Version.CURRENT.id + 1, 234217727));
    }

    public static Version tooOldVersion() {
        return Version.fromId(between(MASK, Version.CURRENT.minimumIndexCompatibilityVersion().id - 1));
    }
}
