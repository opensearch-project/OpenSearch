/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources.testplugins;

import org.opensearch.accesscontrol.resources.ResourceService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ResourcePlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// Sample test resource plugin
public class TestResourcePlugin extends Plugin implements ResourcePlugin {

    public static final String SAMPLE_TEST_INDEX = ".sample_test_resource";

    @Override
    public String getResourceType() {
        return "";
    }

    @Override
    public String getResourceIndex() {
        return SAMPLE_TEST_INDEX;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final List<Class<? extends LifecycleComponent>> services = new ArrayList<>(1);
        services.add(GuiceHolder.class);
        return services;
    }

    public static class GuiceHolder implements LifecycleComponent {

        private static ResourceService resourceService;

        @Inject
        public GuiceHolder(final ResourceService resourceService) {
            GuiceHolder.resourceService = resourceService;
        }

        public static ResourceService getResourceService() {
            return resourceService;
        }

        @Override
        public void close() {}

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

    }

    public static class TestResource {
        public String id;
        public String name;

        public TestResource() {}

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
