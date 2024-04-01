/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.index.store.remote.utils.filetracker.FileState;
import org.opensearch.index.store.remote.utils.filetracker.FileTrackingInfo;
import org.opensearch.index.store.remote.utils.filetracker.FileType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CompositeDirectoryRemoteStoreFileTrackerAdapter implements RemoteStoreFileTrackerAdapter {

    private FileCache fileCache;
    private Map<String, FileTrackingInfo> fileTracker;
    private RemoteSegmentStoreDirectory remoteDirectory;

    public CompositeDirectoryRemoteStoreFileTrackerAdapter(FileCache fileCache) {
        this.fileCache = fileCache;
        remoteDirectory = null;
        this.fileTracker = new HashMap<>();
    }

    public void setRemoteDirectory(Directory remoteDirectory) {
        this.remoteDirectory = (RemoteSegmentStoreDirectory) remoteDirectory;
    }

    public String getUploadedFileName(String name) {
        return remoteDirectory.getExistingRemoteFilename(name);
    }

    public long getFileLength(String name) {
        try {
            return remoteDirectory.fileLength(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws IOException {
        return new TransferManager(remoteDirectory.getDataDirectoryBlobContainer(), fileCache).fetchBlob(blobFetchRequest);
    }

    public void trackFile(String name, FileState fileState, FileType fileType) {
        if (!fileTracker.containsKey(name)) {
            fileTracker.put(name, new FileTrackingInfo(fileState, fileType));
        }
    }

    public void updateFileType(String name, FileType fileType) {
        FileTrackingInfo fileTrackingInfo = fileTracker.get(name);
        if (fileTrackingInfo != null) {
            fileTracker.put(name, new FileTrackingInfo(fileTrackingInfo.getFileState(), fileType));
        }
    }

    public void updateFileState(String name, FileState fileState) {
        FileTrackingInfo fileTrackingInfo = fileTracker.get(name);
        if (fileTrackingInfo != null) {
            fileTracker.put(name, new FileTrackingInfo(fileState, fileTrackingInfo.getFileType()));
        }
    }

    public void removeFileFromTracker(String name) {
        fileTracker.remove(name);
    }

    public FileState getFileState(String name) {
        if (!fileTracker.containsKey(name)) {
            return null;
        }
        return fileTracker.get(name).getFileState();
    }

    public FileType getFileType(String name) {
        if (!fileTracker.containsKey(name)) {
            return null;
        }
        return fileTracker.get(name).getFileType();
    }

    public boolean isFilePresent(String name) {
        return fileTracker.containsKey(name);
    }

    public String[] getRemoteFiles() throws IOException {
        String[] remoteFiles;
        try {
            remoteFiles = remoteDirectory.listAll();
        } catch (Exception e) {
            remoteFiles = new String[0];
        }
        return remoteFiles;
    }

    public String logFileTracker() {
        String result = "";
        for (Map.Entry<String, FileTrackingInfo> entry : fileTracker.entrySet()) {
            result += entry.getKey()
                + " : "
                + entry.getValue().getFileType().name()
                + " , "
                + entry.getValue().getFileState().name()
                + "\n";
        }
        return result;
    }
}
