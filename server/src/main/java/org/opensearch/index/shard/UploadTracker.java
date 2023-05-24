/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

/**
 * A tracker class that is fed to FileUploader.
 *
 * @opensearch.internal
 */
public interface UploadTracker {

    void beforeUpload(String file);

    void onSuccess(String file);

    void onFailure(String file);
}
