/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.tasks.TaskResultsService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.tasks.TaskResultsService.TASK_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SystemIndicesTests extends OpenSearchTestCase {

    public void testBasicOverlappingPatterns() {
        SystemIndexDescriptor broadPattern = new SystemIndexDescriptor(".a*c*", "test");
        SystemIndexDescriptor notOverlapping = new SystemIndexDescriptor(".bbbddd*", "test");
        SystemIndexDescriptor overlapping1 = new SystemIndexDescriptor(".ac*", "test");
        SystemIndexDescriptor overlapping2 = new SystemIndexDescriptor(".aaaabbbccc", "test");
        SystemIndexDescriptor overlapping3 = new SystemIndexDescriptor(".aaabb*cccddd*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String broadPatternSource = "AAA" + randomAlphaOfLength(5);
        String otherSource = "ZZZ" + randomAlphaOfLength(6);
        Map<String, Collection<SystemIndexDescriptor>> descriptors = new HashMap<>();
        descriptors.put(broadPatternSource, singletonList(broadPattern));
        descriptors.put(otherSource, Arrays.asList(notOverlapping, overlapping1, overlapping2, overlapping3));

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> SystemIndexRegistry.checkForOverlappingPatterns(descriptors)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "a system index descriptor ["
                    + broadPattern
                    + "] from ["
                    + broadPatternSource
                    + "] overlaps with other system index descriptors:"
            )
        );
        String fromPluginString = " from [" + otherSource + "]";
        assertThat(exception.getMessage(), containsString(overlapping1.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping2.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping3.toString() + fromPluginString));
        assertThat(exception.getMessage(), not(containsString(notOverlapping.toString())));

        IllegalStateException constructorException = expectThrows(IllegalStateException.class, () -> new SystemIndices(descriptors));
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testComplexOverlappingPatterns() {
        // These patterns are slightly more complex to detect because pattern1 does not match pattern2 and vice versa
        SystemIndexDescriptor pattern1 = new SystemIndexDescriptor(".a*c", "test");
        SystemIndexDescriptor pattern2 = new SystemIndexDescriptor(".ab*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String source1 = "AAA" + randomAlphaOfLength(5);
        String source2 = "ZZZ" + randomAlphaOfLength(6);
        Map<String, Collection<SystemIndexDescriptor>> descriptors = new HashMap<>();
        descriptors.put(source1, singletonList(pattern1));
        descriptors.put(source2, singletonList(pattern2));

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> SystemIndexRegistry.checkForOverlappingPatterns(descriptors)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "a system index descriptor [" + pattern1 + "] from [" + source1 + "] overlaps with other system index descriptors:"
            )
        );
        assertThat(exception.getMessage(), containsString(pattern2.toString() + " from [" + source2 + "]"));

        IllegalStateException constructorException = expectThrows(IllegalStateException.class, () -> new SystemIndices(descriptors));
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testBuiltInSystemIndices() {
        SystemIndices systemIndices = new SystemIndices(emptyMap());
        assertTrue(systemIndices.isSystemIndex(".tasks"));
        assertTrue(systemIndices.isSystemIndex(".tasks1"));
        assertTrue(systemIndices.isSystemIndex(".tasks-old"));
    }

    public void testPluginCannotOverrideBuiltInSystemIndex() {
        Map<String, Collection<SystemIndexDescriptor>> pluginMap = singletonMap(
            TaskResultsService.class.getName(),
            singletonList(new SystemIndexDescriptor(TASK_INDEX, "Task Result Index"))
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SystemIndices(pluginMap));
        assertThat(e.getMessage(), containsString("plugin or module attempted to define the same source"));
    }

    public void testSystemIndexMatching() {
        SystemIndexPlugin plugin1 = new SystemIndexPlugin1();
        SystemIndexPlugin plugin2 = new SystemIndexPlugin2();
        SystemIndexPlugin plugin3 = new SystemIndexPatternPlugin();
        SystemIndices pluginSystemIndices = new SystemIndices(
            Map.of(
                SystemIndexPlugin1.class.getCanonicalName(),
                plugin1.getSystemIndexDescriptors(Settings.EMPTY),
                SystemIndexPlugin2.class.getCanonicalName(),
                plugin2.getSystemIndexDescriptors(Settings.EMPTY),
                SystemIndexPatternPlugin.class.getCanonicalName(),
                plugin3.getSystemIndexDescriptors(Settings.EMPTY)
            )
        );

        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index1", ".system-index2")),
            equalTo(Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1, SystemIndexPlugin2.SYSTEM_INDEX_2))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index1")),
            equalTo(Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index2")),
            equalTo(Set.of(SystemIndexPlugin2.SYSTEM_INDEX_2))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index-pattern1")),
            equalTo(Set.of(".system-index-pattern1"))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index-pattern-sub*")),
            equalTo(Set.of(".system-index-pattern-sub*"))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index-pattern1", ".system-index-pattern2")),
            equalTo(Set.of(".system-index-pattern1", ".system-index-pattern2"))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index1", ".system-index-pattern1")),
            equalTo(Set.of(".system-index1", ".system-index-pattern1"))
        );
        assertThat(
            SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".system-index1", ".system-index-pattern1", ".not-system")),
            equalTo(Set.of(".system-index1", ".system-index-pattern1"))
        );
        assertThat(SystemIndexRegistry.matchesSystemIndexPattern(Set.of(".not-system")), equalTo(Collections.emptySet()));
    }

    public void testRegisteredSystemIndexMatching() {
        SystemIndexPlugin plugin1 = new SystemIndexPlugin1();
        SystemIndexPlugin plugin2 = new SystemIndexPlugin2();
        SystemIndices pluginSystemIndices = new SystemIndices(
            Map.of(
                SystemIndexPlugin1.class.getCanonicalName(),
                plugin1.getSystemIndexDescriptors(Settings.EMPTY),
                SystemIndexPlugin2.class.getCanonicalName(),
                plugin2.getSystemIndexDescriptors(Settings.EMPTY)
            )
        );
        Set<String> systemIndices = SystemIndexRegistry.matchesSystemIndexPattern(
            Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1, SystemIndexPlugin2.SYSTEM_INDEX_2)
        );
        assertEquals(2, systemIndices.size());
        assertTrue(systemIndices.containsAll(Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1, SystemIndexPlugin2.SYSTEM_INDEX_2)));
    }

    public void testRegisteredSystemIndexMatchingForPlugin() {
        SystemIndexPlugin plugin1 = new SystemIndexPlugin1();
        SystemIndexPlugin plugin2 = new SystemIndexPlugin2();
        SystemIndices pluginSystemIndices = new SystemIndices(
            Map.of(
                SystemIndexPlugin1.class.getCanonicalName(),
                plugin1.getSystemIndexDescriptors(Settings.EMPTY),
                SystemIndexPlugin2.class.getCanonicalName(),
                plugin2.getSystemIndexDescriptors(Settings.EMPTY)
            )
        );
        Set<String> systemIndicesForPlugin1 = SystemIndexRegistry.matchesPluginSystemIndexPattern(
            SystemIndexPlugin1.class.getCanonicalName(),
            Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1, SystemIndexPlugin2.SYSTEM_INDEX_2, "other-index")
        );
        assertEquals(1, systemIndicesForPlugin1.size());
        assertTrue(systemIndicesForPlugin1.contains(SystemIndexPlugin1.SYSTEM_INDEX_1));

        Set<String> systemIndicesForPlugin2 = SystemIndexRegistry.matchesPluginSystemIndexPattern(
            SystemIndexPlugin2.class.getCanonicalName(),
            Set.of(SystemIndexPlugin1.SYSTEM_INDEX_1, SystemIndexPlugin2.SYSTEM_INDEX_2, "other-index")
        );
        assertEquals(1, systemIndicesForPlugin2.size());
        assertTrue(systemIndicesForPlugin2.contains(SystemIndexPlugin2.SYSTEM_INDEX_2));

        Set<String> noMatchingSystemIndices = SystemIndexRegistry.matchesPluginSystemIndexPattern(
            SystemIndexPlugin2.class.getCanonicalName(),
            Set.of("other-index")
        );
        assertEquals(0, noMatchingSystemIndices.size());
    }

    static final class SystemIndexPlugin1 extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_INDEX_1 = ".system-index1";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            final SystemIndexDescriptor systemIndexDescriptor = new SystemIndexDescriptor(SYSTEM_INDEX_1, "System index 1");
            return Collections.singletonList(systemIndexDescriptor);
        }
    }

    static final class SystemIndexPlugin2 extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_INDEX_2 = ".system-index2";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            final SystemIndexDescriptor systemIndexDescriptor = new SystemIndexDescriptor(SYSTEM_INDEX_2, "System index 2");
            return Collections.singletonList(systemIndexDescriptor);
        }
    }

    static final class SystemIndexPatternPlugin extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_INDEX_PATTERN = ".system-index-pattern*";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            final SystemIndexDescriptor systemIndexDescriptor = new SystemIndexDescriptor(SYSTEM_INDEX_PATTERN, "System index pattern");
            return Collections.singletonList(systemIndexDescriptor);
        }
    }
}
