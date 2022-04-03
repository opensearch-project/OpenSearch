/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.settings.Settings;

/**
 * Index Settings Tests for NIO FileSystem as index store type.
 */
public class SmbNIOFsTests extends AbstractAzureFsTestCase {
    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put("index.store.type", "smb_nio_fs").build();
    }
}
