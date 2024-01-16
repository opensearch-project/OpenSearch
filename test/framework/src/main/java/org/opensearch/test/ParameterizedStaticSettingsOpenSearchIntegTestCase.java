/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.opensearch.common.settings.Settings;

import java.util.Objects;

/**
 * Base class for running the tests with parameterization wit static settings: the cluster will be pre-created with the settings at startup, the method
 * {@link #hasSameParametersAs(ParameterizedOpenSearchIntegTestCase)} is being used by the test scaffolding to detect when the test suite is instantiated with
 * the new parameters and the test cluster has to be recreated.
 */
public abstract class ParameterizedStaticSettingsOpenSearchIntegTestCase extends ParameterizedOpenSearchIntegTestCase {
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
