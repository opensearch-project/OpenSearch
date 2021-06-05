/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

public class KeystoreWrapperUtil {
    public static void saveSetting(KeyStoreWrapper keystore, String setting, byte[] bytes) {
        keystore.setFile(setting, bytes);
    }
}
