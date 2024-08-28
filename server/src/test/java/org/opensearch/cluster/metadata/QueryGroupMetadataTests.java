/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.Diff;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.ResourceType;
import org.opensearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.cluster.metadata.QueryGroupTests.createRandomQueryGroup;

public class QueryGroupMetadataTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

    public void testToXContent() throws IOException {
        long updatedAt = 1720047207;
        QueryGroupMetadata queryGroupMetadata = new QueryGroupMetadata(
            Map.of(
                "ajakgakg983r92_4242",
                new QueryGroup(
                    "test",
                    "ajakgakg983r92_4242",
                    QueryGroup.ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    updatedAt
                )
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        queryGroupMetadata.toXContent(builder, null);
        builder.endObject();
        assertEquals(
            "{\"ajakgakg983r92_4242\":{\"_id\":\"ajakgakg983r92_4242\",\"name\":\"test\",\"resiliency_mode\":\"enforced\",\"updated_at\":1720047207,\"resource_limits\":{\"memory\":0.5}}}",
            builder.toString()
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(QueryGroupMetadata.class, QueryGroupMetadata.TYPE, QueryGroupMetadata::new)
            )
        );
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        final QueryGroup queryGroup = createRandomQueryGroup("asdfakgjwrir23r25");
        final QueryGroupMetadata queryGroupMetadata = new QueryGroupMetadata(Map.of(queryGroup.get_id(), queryGroup));
        return queryGroupMetadata;
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return QueryGroupMetadata::readDiffFrom;
    }

    @Override
    protected Metadata.Custom doParseInstance(XContentParser parser) throws IOException {
        return QueryGroupMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return QueryGroupMetadata::new;
    }

    @Override
    protected QueryGroupMetadata createTestInstance() {
        return new QueryGroupMetadata(getRandomQueryGroups());
    }

    private Map<String, QueryGroup> getRandomQueryGroups() {
        QueryGroup qg1 = createRandomQueryGroup("1243gsgsdgs");
        QueryGroup qg2 = createRandomQueryGroup("lkajga8080");
        return Map.of(qg1.get_id(), qg1, qg2.get_id(), qg2);
    }
}
