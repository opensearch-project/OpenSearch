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

import java.io.IOException;
import java.util.List;

public class ResourceLimitGroupTests extends AbstractSerializingTestCase<ResourceLimitGroup> {


    static ResourceLimitGroup createRandomResourceLimitGroup() {
        String name = randomAlphaOfLength(10);
        ResourceLimitGroup.ResourceLimit resourceLimit =  new ResourceLimitGroup.ResourceLimit("jvm",
            randomDoubleBetween(0.0, 0.80, false));
        return new ResourceLimitGroup(name, List.of(resourceLimit));
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
        assertThrows(NullPointerException.class,
            () -> new ResourceLimitGroup(null, List.of()));
    }

    public void testNullResourceLimits() {
        assertThrows(NullPointerException.class,
            () -> new ResourceLimitGroup("analytics", null));
    }

    public void testEmptyResourceLimits() {
        assertThrows(IllegalArgumentException.class,
            () -> new ResourceLimitGroup("analytics", List.of()));
    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceNameIsGiven() {
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimitGroup(
            "analytics",
            List.of(
                new ResourceLimitGroup.ResourceLimit("RequestRate", randomDoubleBetween(0.01, 0.8, false))
            )
        ));
    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimitGroup(
            "analytics",
            List.of(
                new ResourceLimitGroup.ResourceLimit("RequestRate", randomDoubleBetween(1.1, 1.8, false))
            )
        ));
    }

    public void testValidResourceLimitGroup() {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(
            "analytics",
            List.of(
                new ResourceLimitGroup.ResourceLimit("jvm", randomDoubleBetween(0.01, 0.8, false))
            )
        );

        assertNotNull(resourceLimitGroup.getName());
        assertEquals("analytics", resourceLimitGroup.getName());
        assertNotNull(resourceLimitGroup.getResourceLimits());
        assertFalse(resourceLimitGroup.getResourceLimits().isEmpty());
        assertEquals(1, resourceLimitGroup.getResourceLimits().size());
    }
}
