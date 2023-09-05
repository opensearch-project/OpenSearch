/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collections;

public class RestoreRemoteStoreRequestTests extends AbstractWireSerializingTestCase<RestoreRemoteStoreRequest> {
    private RestoreRemoteStoreRequest randomState(RestoreRemoteStoreRequest instance) {
        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            instance.indices(indices);
        }

        instance.waitForCompletion(randomBoolean());
        instance.restoreAllShards(randomBoolean());

        if (randomBoolean()) {
            instance.masterNodeTimeout(randomTimeValue());
        }

        return instance;
    }

    @Override
    protected RestoreRemoteStoreRequest createTestInstance() {
        return randomState(new RestoreRemoteStoreRequest());
    }

    @Override
    protected Writeable.Reader<RestoreRemoteStoreRequest> instanceReader() {
        return RestoreRemoteStoreRequest::new;
    }

    @Override
    protected RestoreRemoteStoreRequest mutateInstance(RestoreRemoteStoreRequest instance) throws IOException {
        RestoreRemoteStoreRequest copy = copyInstance(instance);
        // ensure that at least one property is different
        List<String> indices = new ArrayList<>(List.of(instance.indices()));
        indices.add("copied");
        copy.indices(indices);
        return randomState(copy);
    }

    public void testSource() throws IOException {
        RestoreRemoteStoreRequest original = createTestInstance();
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();

        RestoreRemoteStoreRequest processed = new RestoreRemoteStoreRequest();
        processed.masterNodeTimeout(original.masterNodeTimeout());
        processed.waitForCompletion(original.waitForCompletion());
        processed.restoreAllShards(original.restoreAllShards());
        processed.source(map);

        assertEquals(original, processed);
    }
}
