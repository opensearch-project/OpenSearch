/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;

/**
 * Base class for running the tests with parameterization with static settings: the cluster will be pre-created with the settings at startup, the method
 * {@link #hasSameParametersAs(ParameterizedOpenSearchIntegTestCase)} is being used by the test scaffolding to detect when the test suite is instantiated with
 * the new parameters and the test cluster has to be recreated.
 * <p>
 * Here is the simple illustration on of the execution flow per parameters combination:
 * <ul>
 *   <li><b>suite scope</b>: create cluster -&gt; for each test method { run test method } -&gt; shutdown cluster</li>
 *   <li><b>test scope</b>: for each test method { create cluster -&gt; run test method -&gt; shutdown cluster }</li>
 * </ul>
 */
public abstract class ParameterizedStaticSettingsOpenSearchIntegTestCase extends ParameterizedOpenSearchIntegTestCase {

    public static final List<Object[]> replicationSettings = Arrays.asList(
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build() },
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build() }
    );

    public ParameterizedStaticSettingsOpenSearchIntegTestCase(Settings nodeSettings) {
        super(nodeSettings);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(settings).build();
    }

    @Override
    boolean hasSameParametersAs(ParameterizedOpenSearchIntegTestCase obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final ParameterizedStaticSettingsOpenSearchIntegTestCase other = (ParameterizedStaticSettingsOpenSearchIntegTestCase) obj;
        return Objects.equals(settings, other.settings);
    }
}
