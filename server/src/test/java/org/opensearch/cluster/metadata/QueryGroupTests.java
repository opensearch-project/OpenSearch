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
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.ChangeableQueryGroup;
import org.opensearch.wlm.ChangeableQueryGroup.ResiliencyMode;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryGroupTests extends AbstractSerializingTestCase<QueryGroup> {

    private static final List<ResiliencyMode> allowedModes = List.of(ResiliencyMode.SOFT, ResiliencyMode.ENFORCED, ResiliencyMode.MONITOR);

    static QueryGroup createRandomQueryGroup(String _id) {
        String name = randomAlphaOfLength(10);
        Map<ResourceType, Double> resourceLimit = new HashMap<>();
        resourceLimit.put(ResourceType.MEMORY, randomDoubleBetween(0.0, 0.80, false));
        return new QueryGroup(name, _id, new ChangeableQueryGroup(randomMode(), resourceLimit), Instant.now().getMillis());
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
    protected QueryGroup doParseInstance(XContentParser parser) throws IOException {
        return QueryGroup.fromXContent(parser);
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    @Override
    protected Writeable.Reader<QueryGroup> instanceReader() {
        return QueryGroup::new;
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected QueryGroup createTestInstance() {
        return createRandomQueryGroup("1232sfraeradf_");
    }

    public void testNullName() {
        assertThrows(
            NullPointerException.class,
            () -> new QueryGroup(null, "_id", new ChangeableQueryGroup(randomMode(), Collections.emptyMap()), Instant.now().getMillis())
        );
    }

    public void testNullId() {
        assertThrows(
            NullPointerException.class,
            () -> new QueryGroup("Dummy", null, new ChangeableQueryGroup(randomMode(), Collections.emptyMap()), Instant.now().getMillis())
        );
    }

    public void testNullResourceLimits() {
        assertThrows(
            NullPointerException.class,
            () -> new QueryGroup("analytics", "_id", new ChangeableQueryGroup(randomMode(), null), Instant.now().getMillis())
        );
    }

    public void testEmptyResourceLimits() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new QueryGroup(
                "analytics",
                "_id",
                new ChangeableQueryGroup(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testIllegalQueryGroupMode() {
        assertThrows(
            NullPointerException.class,
            () -> new QueryGroup(
                "analytics",
                "_id",
                new ChangeableQueryGroup(null, Map.of(ResourceType.MEMORY, 0.4)),
                Instant.now().getMillis()
            )
        );
    }

    public void testQueryGroupInitiation() {
        QueryGroup queryGroup = new QueryGroup("analytics", new ChangeableQueryGroup(randomMode(), Map.of(ResourceType.MEMORY, 0.4)));
        assertNotNull(queryGroup.getName());
        assertNotNull(queryGroup.get_id());
        assertNotNull(queryGroup.getResourceLimits());
        assertFalse(queryGroup.getResourceLimits().isEmpty());
        assertEquals(1, queryGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(queryGroup.getResiliencyMode()));
        assertTrue(queryGroup.getUpdatedAtInMillis() != 0);
    }

    public void testIllegalQueryGroupName() {
        assertThrows(
            NullPointerException.class,
            () -> new QueryGroup("a".repeat(51), "_id", new ChangeableQueryGroup(), Instant.now().getMillis())
        );
        assertThrows(NullPointerException.class, () -> new QueryGroup("", "_id", new ChangeableQueryGroup(), Instant.now().getMillis()));

    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new QueryGroup(
                "analytics",
                "_id",
                new ChangeableQueryGroup(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(1.1, 1.8, false))),
                Instant.now().getMillis()
            )
        );
    }

    public void testValidQueryGroup() {
        QueryGroup queryGroup = new QueryGroup(
            "analytics",
            "_id",
            new ChangeableQueryGroup(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            Instant.ofEpochMilli(1717187289).getMillis()
        );

        assertNotNull(queryGroup.getName());
        assertEquals("analytics", queryGroup.getName());
        assertNotNull(queryGroup.getResourceLimits());
        assertFalse(queryGroup.getResourceLimits().isEmpty());
        assertEquals(1, queryGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(queryGroup.getResiliencyMode()));
        assertEquals(1717187289, queryGroup.getUpdatedAtInMillis());
    }

    public void testToXContent() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String queryGroupId = UUIDs.randomBase64UUID();
        QueryGroup queryGroup = new QueryGroup(
            "TestQueryGroup",
            queryGroupId,
            new ChangeableQueryGroup(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.30, ResourceType.MEMORY, 0.40)),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        queryGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\"_id\":\""
                + queryGroupId
                + "\",\"name\":\"TestQueryGroup\",\"resiliency_mode\":\"enforced\",\"updated_at\":"
                + currentTimeInMillis
                + ",\"resource_limits\":{\"cpu\":0.3,\"memory\":0.4}}",
            builder.toString()
        );
    }
}
