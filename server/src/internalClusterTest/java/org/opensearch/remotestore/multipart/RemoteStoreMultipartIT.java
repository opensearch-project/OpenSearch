/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreIT;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteStoreMultipartIT extends RemoteStoreIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected void putRepository(Path path) {
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME)
                .setType(MockFsRepositoryPlugin.TYPE)
                .setSettings(Settings.builder().put("location", path))
        );
    }
}
