/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.utils.StandardValidator;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.index.mapper.GeoShapeFieldMapper;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

/**
 * This is the base class for all the Geo related integration tests. Use this class to add the features and settings
 * for the test cluster on which integration tests are running.
 */
public abstract class GeoModulePluginIntegTestCase extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    protected static final double GEOHASH_TOLERANCE = 1E-5D;

    protected static final WellKnownText WKT = new WellKnownText(true, new StandardValidator(true));

    public GeoModulePluginIntegTestCase(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    /**
     * Returns a collection of plugins that should be loaded on each node for doing the integration tests. As this
     * geo plugin is not getting packaged in a zip, we need to load it before the tests run.
     *
     * @return List of {@link Plugin}
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(GeoModulePlugin.class);
    }

    /**
     * This was added as a backdoor to Mock the implementation of {@link GeoShapeFieldMapper} which was coming from
     * {@link GeoModulePlugin}. Mock implementation is {@link TestGeoShapeFieldMapperPlugin}. Now we are using the
     * {@link GeoModulePlugin} in our integration tests we need to override this functionality to avoid multiple mapper
     * error.
     *
     * @return boolean
     */
    @Override
    protected boolean addMockGeoShapeFieldMapper() {
        return false;
    }
}
