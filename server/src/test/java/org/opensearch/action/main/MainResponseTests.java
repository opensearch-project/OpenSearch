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

package org.opensearch.action.main;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Date;

import static org.opensearch.action.main.MainResponse.TAGLINE;

public class MainResponseTests extends AbstractSerializingTestCase<MainResponse> {

    @Override
    protected MainResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Build build = new Build(
            Build.Type.UNKNOWN,
            randomAlphaOfLength(8),
            date,
            randomBoolean(),
            version.toString(),
            randomAlphaOfLength(10)
        );
        return new MainResponse(nodeName, version, clusterName, clusterUuid, build);
    }

    @Override
    protected Writeable.Reader<MainResponse> instanceReader() {
        return MainResponse::new;
    }

    @Override
    protected MainResponse doParseInstance(XContentParser parser) {
        return MainResponse.fromXContent(parser);
    }

    public void testToXContent() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build current = Build.CURRENT;
        Build build = new Build(
            current.type(),
            current.hash(),
            current.date(),
            current.isSnapshot(),
            current.getQualifiedVersion(),
            current.getDistribution()
        );
        Version version = Version.CURRENT;
        MainResponse response = new MainResponse("nodeName", version, new ClusterName("clusterName"), clusterUUID, build);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{"
                + "\"name\":\"nodeName\","
                + "\"cluster_name\":\"clusterName\","
                + "\"cluster_uuid\":\""
                + clusterUUID
                + "\","
                + "\"version\":{"
                + "\"distribution\":\""
                + build.getDistribution()
                + "\","
                + "\"number\":\""
                + build.getQualifiedVersion()
                + "\","
                + "\"build_type\":\""
                + current.type().displayName()
                + "\","
                + "\"build_hash\":\""
                + current.hash()
                + "\","
                + "\"build_date\":\""
                + current.date()
                + "\","
                + "\"build_snapshot\":"
                + current.isSnapshot()
                + ","
                + "\"lucene_version\":\""
                + version.luceneVersion.toString()
                + "\","
                + "\"minimum_wire_compatibility_version\":\""
                + version.minimumCompatibilityVersion().toString()
                + "\","
                + "\"minimum_index_compatibility_version\":\""
                + version.minimumIndexCompatibilityVersion().toString()
                + "\"},"
                + "\"tagline\":\""
                + TAGLINE
                + "\""
                + "}",
            Strings.toString(builder)
        );
    }

    @Override
    protected MainResponse mutateInstance(MainResponse mutateInstance) {
        String clusterUuid = mutateInstance.getClusterUuid();
        Build build = mutateInstance.getBuild();
        Version version = mutateInstance.getVersion();
        String nodeName = mutateInstance.getNodeName();
        ClusterName clusterName = mutateInstance.getClusterName();
        switch (randomIntBetween(0, 4)) {
            case 0:
                clusterUuid = clusterUuid + randomAlphaOfLength(5);
                break;
            case 1:
                nodeName = nodeName + randomAlphaOfLength(5);
                break;
            case 2:
                // toggle the snapshot flag of the original Build parameter
                build = new Build(
                    Build.Type.UNKNOWN,
                    build.hash(),
                    build.date(),
                    !build.isSnapshot(),
                    build.getQualifiedVersion(),
                    build.getDistribution()
                );
                break;
            case 3:
                version = randomValueOtherThan(version, () -> VersionUtils.randomVersion(random()));
                break;
            case 4:
                clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
                break;
        }
        return new MainResponse(nodeName, version, clusterName, clusterUuid, build);
    }
}
