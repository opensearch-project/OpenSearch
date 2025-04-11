/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkloadGroupTests extends AbstractSerializingTestCase<WorkloadGroup> {

    private static final List<ResiliencyMode> allowedModes = List.of(ResiliencyMode.SOFT, ResiliencyMode.ENFORCED, ResiliencyMode.MONITOR);

    static WorkloadGroup createRandomWorkloadGroup(String _id) {
        String name = randomAlphaOfLength(10);
        Map<ResourceType, Double> resourceLimit = new HashMap<>();
        resourceLimit.put(ResourceType.MEMORY, randomDoubleBetween(0.0, 0.80, false));
        return new WorkloadGroup(name, _id, new MutableWorkloadGroupFragment(randomMode(), resourceLimit), Instant.now().getMillis());
    }

    private static ResiliencyMode randomMode() {
        return allowedModes.get(randomIntBetween(0, allowedModes.size() - 1));
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     *
     * @param parser
     */
    @Override
    protected WorkloadGroup doParseInstance(XContentParser parser) throws IOException {
        return WorkloadGroup.fromXContent(parser);
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    @Override
    protected Writeable.Reader<WorkloadGroup> instanceReader() {
        return WorkloadGroup::new;
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected WorkloadGroup createTestInstance() {
        return createRandomWorkloadGroup("1232sfraeradf_");
    }

    public void testNullName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                null,
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullId() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "Dummy",
                null,
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullResourceLimits() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("analytics", "_id", new MutableWorkloadGroupFragment(randomMode(), null), Instant.now().getMillis())
        );
    }

    public void testEmptyResourceLimits() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testIllegalWorkloadGroupMode() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(null, Map.of(ResourceType.MEMORY, 0.4)),
                Instant.now().getMillis()
            )
        );
    }

    public void testWorkloadGroupInitiation() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, 0.4))
        );
        assertNotNull(workloadGroup.getName());
        assertNotNull(workloadGroup.get_id());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertTrue(workloadGroup.getUpdatedAtInMillis() != 0);
    }

    public void testIllegalWorkloadGroupName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("a".repeat(51), "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("", "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );

    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(1.1, 1.8, false))),
                Instant.now().getMillis()
            )
        );
    }

    public void testValidWorkloadGroup() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            "_id",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            Instant.ofEpochMilli(1717187289).getMillis()
        );

        assertNotNull(workloadGroup.getName());
        assertEquals("analytics", workloadGroup.getName());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertEquals(1717187289, workloadGroup.getUpdatedAtInMillis());
    }

    public void testToXContent() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.30, ResourceType.MEMORY, 0.40)),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\"_id\":\""
                + workloadGroupId
                + "\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"cpu\":0.3,\"memory\":0.4},\"updated_at\":"
                + currentTimeInMillis
                + "}",
            builder.toString()
        );
    }
}
