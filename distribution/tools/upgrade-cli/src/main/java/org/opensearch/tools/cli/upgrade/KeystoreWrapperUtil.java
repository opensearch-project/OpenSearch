/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.common.settings.KeyStoreWrapper;

/**
 * Utility that has package level access to the {@link KeyStoreWrapper} for
 * saving a setting.
 */
public final class KeystoreWrapperUtil {
    /**
     * No public constructor. Contains only static functions.
     */
    private KeystoreWrapperUtil() {}

    /**
     * Save a secure setting using the wrapper.
     *
     * @param keystore an instance of {@link KeyStoreWrapper}
     * @param setting  setting to save
     * @param bytes    value of the setting in bytes
     */
    public static void saveSetting(KeyStoreWrapper keystore, String setting, byte[] bytes) {
        keystore.setFile(setting, bytes);
    }
}
