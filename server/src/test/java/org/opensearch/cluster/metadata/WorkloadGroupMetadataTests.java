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
import org.opensearch.test.AbstractDiffableSerializationTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.cluster.metadata.WorkloadGroupTests.createRandomWorkloadGroup;

public class WorkloadGroupMetadataTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

    public void testToXContent() throws IOException {
        long updatedAt = 1720047207;
        WorkloadGroupMetadata workloadGroupMetadata = new WorkloadGroupMetadata(
            Map.of(
                "ajakgakg983r92_4242",
                new WorkloadGroup(
                    "test",
                    "ajakgakg983r92_4242",
                    new MutableWorkloadGroupFragment(
                        MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                        Map.of(ResourceType.MEMORY, 0.5)
                    ),
                    updatedAt
                )
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        workloadGroupMetadata.toXContent(builder, null);
        builder.endObject();
        assertEquals(
            "{\"ajakgakg983r92_4242\":{\"_id\":\"ajakgakg983r92_4242\",\"name\":\"test\",\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},\"updated_at\":1720047207}}",
            builder.toString()
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(WorkloadGroupMetadata.class, WorkloadGroupMetadata.TYPE, WorkloadGroupMetadata::new)
            )
        );
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        final WorkloadGroup workloadGroup = createRandomWorkloadGroup("asdfakgjwrir23r25");
        return new WorkloadGroupMetadata(Map.of(workloadGroup.get_id(), workloadGroup));
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return WorkloadGroupMetadata::readDiffFrom;
    }

    @Override
    protected Metadata.Custom doParseInstance(XContentParser parser) throws IOException {
        return WorkloadGroupMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return WorkloadGroupMetadata::new;
    }

    @Override
    protected WorkloadGroupMetadata createTestInstance() {
        return new WorkloadGroupMetadata(getRandomWorkloadGroups());
    }

    private Map<String, WorkloadGroup> getRandomWorkloadGroups() {
        WorkloadGroup qg1 = createRandomWorkloadGroup("1243gsgsdgs");
        WorkloadGroup qg2 = createRandomWorkloadGroup("lkajga8080");
        return Map.of(qg1.get_id(), qg1, qg2.get_id(), qg2);
    }
}
