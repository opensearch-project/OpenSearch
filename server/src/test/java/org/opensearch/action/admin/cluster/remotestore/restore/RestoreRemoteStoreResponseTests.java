/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RestoreRemoteStoreResponseTests extends AbstractXContentTestCase<RestoreRemoteStoreResponse> {

    @Override
    protected RestoreRemoteStoreResponse createTestInstance() {
        if (randomBoolean()) {
            String name = "remote_store";
            List<String> indices = new ArrayList<>();
            indices.add("test0");
            indices.add("test1");
            int totalShards = randomIntBetween(1, 1000);
            int successfulShards = randomIntBetween(0, totalShards);
            return new RestoreRemoteStoreResponse(new RestoreInfo(name, indices, totalShards, successfulShards));
        } else {
            return new RestoreRemoteStoreResponse((RestoreInfo) null);
        }
    }

    @Override
    protected RestoreRemoteStoreResponse doParseInstance(XContentParser parser) throws IOException {
        return RestoreRemoteStoreResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
