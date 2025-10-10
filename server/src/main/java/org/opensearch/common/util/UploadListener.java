/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.index.engine.exec.FileMetadata;

/**
 * A tracker class that is fed to FileUploader.
 *
 * @opensearch.internal
 */
public interface UploadListener {

    void beforeUpload(FileMetadata fileMetadata);

    void onSuccess(FileMetadata fileMetadata);

    void onFailure(FileMetadata fileMetadata);
}
