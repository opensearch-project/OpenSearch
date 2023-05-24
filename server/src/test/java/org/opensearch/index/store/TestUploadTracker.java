/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.index.shard.UploadTracker;

import java.util.concurrent.ConcurrentHashMap;

public class TestUploadTracker implements UploadTracker {

    private final ConcurrentHashMap<String, UploadStatus> uploadStatusMap = new ConcurrentHashMap<>();

    enum UploadStatus {
        BEFORE_UPLOAD,
        UPLOAD_SUCCESS,
        UPLOAD_FAILURE
    }

    @Override
    public void beforeUpload(String file) {
        uploadStatusMap.put(file, UploadStatus.BEFORE_UPLOAD);
    }

    @Override
    public void onSuccess(String file) {
        uploadStatusMap.put(file, UploadStatus.UPLOAD_SUCCESS);
    }

    @Override
    public void onFailure(String file) {
        uploadStatusMap.put(file, UploadStatus.UPLOAD_FAILURE);
    }

    public UploadStatus getUploadStatus(String file) {
        return uploadStatusMap.get(file);
    }
}
