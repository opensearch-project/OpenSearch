/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.util.HashMap;
import java.util.Map;

/**
 * stores all the files added to the filetracker
 */
public class FileTrackerImp implements FileTracker {
    private final Map<String, FileTrackingInfo> fileTrackingInfoMap;

    public FileTrackerImp() {
        // at node restart, the file tracking map is initialized as empty for now
        fileTrackingInfoMap = new HashMap<>();
    }

    public FileTrackerImp(Map<String, FileTrackingInfo> fileTracker) {
        this.fileTrackingInfoMap = fileTracker;
    }

    @Override
    public boolean isPresent(String file) {
        return fileTrackingInfoMap.containsKey(file);
    }

    @Override
    public void updateState(String file, FileTrackingInfo.FileState fileState) {
        FileTrackingInfo fileTrackingInfo = fileTrackingInfoMap.get(file);
        if (fileTrackingInfo != null) {
            fileTrackingInfoMap.put(
                file,
                new FileTrackingInfo(
                    file,
                    fileState,
                    fileTrackingInfo.getFileType(),
                    fileTrackingInfo.getFilePath(),
                    fileTrackingInfo.getUploadedSegmentMetadata()
                )
            );
        }
    }

    @Override
    public void updateFileType(String file, FileTrackingInfo.FileType fileType) {
        FileTrackingInfo fileTrackingInfo = fileTrackingInfoMap.get(file);
        if (fileTrackingInfo != null) {
            fileTrackingInfoMap.put(
                file,
                new FileTrackingInfo(
                    file,
                    fileTrackingInfo.getFileState(),
                    fileType,
                    fileTrackingInfo.getFilePath(),
                    fileTrackingInfo.getUploadedSegmentMetadata()
                )
            );
        }
    }

    public Map<String, FileTrackingInfo> getFileTrackingInfoMap() {
        return fileTrackingInfoMap;
    }
}
