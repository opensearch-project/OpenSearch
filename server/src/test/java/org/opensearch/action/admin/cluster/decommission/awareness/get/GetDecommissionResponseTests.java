/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class GetDecommissionResponseTests extends AbstractXContentTestCase<GetDecommissionResponse> {
    @Override
    protected GetDecommissionResponse createTestInstance() {
        DecommissionStatus status = DecommissionStatus.fromValue((byte) randomIntBetween(0, 5));
        String attributeName = randomAlphaOfLength(10);
        String attributeValue = randomAlphaOfLength(10);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        return new GetDecommissionResponse(decommissionAttribute, status);
    }

    @Override
    protected GetDecommissionResponse doParseInstance(XContentParser parser) throws IOException {
        return GetDecommissionResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
