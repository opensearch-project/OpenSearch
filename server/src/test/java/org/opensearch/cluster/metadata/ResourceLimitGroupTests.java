/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceLimitGroupTests extends AbstractSerializingTestCase<ResourceLimitGroup> {

    private static final List<ResourceLimitGroup.ResourceLimitGroupMode> allowedModes = List.of(
        ResourceLimitGroup.ResourceLimitGroupMode.SOFT,
        ResourceLimitGroup.ResourceLimitGroupMode.ENFORCED,
        ResourceLimitGroup.ResourceLimitGroupMode.MONITOR
    );

    static ResourceLimitGroup createRandomResourceLimitGroup() {
        String name = randomAlphaOfLength(10);
        Map<String, Object> resourceLimit = new HashMap<>();
        resourceLimit.put("jvm", randomDoubleBetween(0.0, 0.80, false));
        return new ResourceLimitGroup(name, "random", randomMode(), resourceLimit, Instant.now().getMillis());
    }

    private static ResourceLimitGroup.ResourceLimitGroupMode randomMode() {
        return allowedModes.get(randomIntBetween(0, allowedModes.size() - 1));
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     *
     * @param parser
     */
    @Override
    protected ResourceLimitGroup doParseInstance(XContentParser parser) throws IOException {
        return ResourceLimitGroup.fromXContent(parser);
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    @Override
    protected Writeable.Reader<ResourceLimitGroup> instanceReader() {
        return ResourceLimitGroup::new;
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected ResourceLimitGroup createTestInstance() {
        return createRandomResourceLimitGroup();
    }

    public void testNullName() {
        assertThrows(
            NullPointerException.class,
            () -> new ResourceLimitGroup(null, "_id", randomMode(), Collections.emptyMap(), Instant.now().getMillis())
        );
    }

    public void testNullId() {
        assertThrows(
            NullPointerException.class,
            () -> new ResourceLimitGroup("Dummy", null, randomMode(), Collections.emptyMap(), Instant.now().getMillis())
        );
    }

    public void testNullResourceLimits() {
        assertThrows(
            NullPointerException.class,
            () -> new ResourceLimitGroup("analytics", "_id", randomMode(), null, Instant.now().getMillis())
        );
    }

    public void testEmptyResourceLimits() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("analytics", "_id", randomMode(), Collections.emptyMap(), Instant.now().getMillis())
        );
    }

    public void testIllegalResourceLimitGroupMode() {
        assertThrows(
            NullPointerException.class,
            () -> new ResourceLimitGroup("analytics", "_id", null, Map.of("jvm", (Object) 0.4), Instant.now().getMillis())
        );
    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceNameIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup(
                "analytics",
                "_id",
                randomMode(),
                Map.of("RequestRate", (Object) randomDoubleBetween(0.01, 0.8, false)),
                Instant.now().getMillis()
            )
        );
    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup(
                "analytics",
                "_id",
                randomMode(),
                Map.of("RequestRate", (Object) randomDoubleBetween(1.1, 1.8, false)),
                Instant.now().getMillis()
            )
        );
    }

    public void testValidResourceLimitGroup() {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(
            "analytics",
            "_id",
            randomMode(),
            Map.of("jvm", randomDoubleBetween(0.01, 0.8, false)),
            Instant.ofEpochMilli(1717187289).getMillis()
        );

        assertNotNull(resourceLimitGroup.getName());
        assertEquals("analytics", resourceLimitGroup.getName());
        assertNotNull(resourceLimitGroup.getResourceLimits());
        assertFalse(resourceLimitGroup.getResourceLimits().isEmpty());
        assertEquals(1, resourceLimitGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(resourceLimitGroup.getMode()));
        assertEquals(1717187289, resourceLimitGroup.getUpdatedAtInMillis());
    }
}
