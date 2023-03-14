/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.opensearch.common.settings.Setting;

public class RemoteStoreSettings {

    public static Setting<Boolean> REMOTE_STORE_MULTIPART_PARALLEL_UPLOAD_SETTING = Setting.boolSetting(
        "remote_store.multipart_parallel_upload.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
}
