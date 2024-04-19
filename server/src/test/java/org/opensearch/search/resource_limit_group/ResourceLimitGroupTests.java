/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResourceLimitGroupTests extends OpenSearchTestCase {

    public static final String JVM = "jvm";
    public static final String NAME_ONE = "resource_limit_group_one";
    public static final String MONITOR = "monitor";
    public static final ResourceLimitGroup resourceLimitGroupOne = new ResourceLimitGroup(
        NAME_ONE,
        List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)),
        MONITOR
    );

    public static void compareResourceLimits(
        List<ResourceLimitGroup.ResourceLimit> limitsOne,
        List<ResourceLimitGroup.ResourceLimit> limitsTwo
    ) {
        assertEquals(limitsOne.size(), limitsTwo.size());
        Map<String, Double> resourceLimitMapOne = limitsOne.stream()
            .collect(Collectors.toMap(ResourceLimitGroup.ResourceLimit::getResourceName, ResourceLimitGroup.ResourceLimit::getValue));
        Map<String, Double> resourceLimitMapTwo = limitsTwo.stream()
            .collect(Collectors.toMap(ResourceLimitGroup.ResourceLimit::getResourceName, ResourceLimitGroup.ResourceLimit::getValue));
        for (String resourceName : resourceLimitMapOne.keySet()) {
            assertTrue(resourceLimitMapTwo.containsKey(resourceName));
            assertEquals(resourceLimitMapOne.get(resourceName), resourceLimitMapTwo.get(resourceName));
        }
    }

    public static void compareResourceLimitGroups(List<ResourceLimitGroup> listOne, List<ResourceLimitGroup> listTwo) {
        assertEquals(listOne.size(), listTwo.size());
        for (ResourceLimitGroup groupOne : listOne) {
            String groupOneName = groupOne.getName();
            List<ResourceLimitGroup> groupTwoList = listTwo.stream()
                .filter(sb -> sb.getName().equals(groupOneName))
                .collect(Collectors.toList());
            assertEquals(1, groupTwoList.size());
            ResourceLimitGroup groupTwo = groupTwoList.get(0);
            assertEquals(groupOne.getName(), groupTwo.getName());
            compareResourceLimits(groupOne.getResourceLimits(), groupTwo.getResourceLimits());
            assertEquals(groupOne.getEnforcement(), groupTwo.getEnforcement());
        }
    }

    public void testSerializationResourceLimitGroup() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        resourceLimitGroupOne.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        ResourceLimitGroup otherGroup = new ResourceLimitGroup(streamInput);
        compareResourceLimitGroups(List.of(resourceLimitGroupOne), List.of(otherGroup));
    }

    public void testInvalidName() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("-test", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("_test", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup(":test", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("te*st", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("test?", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup("Test", List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), MONITOR)
        );
    }

    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimitGroup.ResourceLimit(JVM, 0.3)), "random")
        );
    }

    public void testInvalidResourceLimit() {
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimit(JVM, -3.0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimit(JVM, 12.0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimit(JVM, 0.345));
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimit("cpu", 0.3));
    }
}
