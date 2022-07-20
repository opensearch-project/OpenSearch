/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.common.UUIDs;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @opensearch.internal
 */
public final class RemoteDirectoryWrapper extends FilterDirectory {
    public static final String SEPARATOR = "__";
    private final RemoteDirectory remoteDirectory;
    private final long primaryTerm;
    private final Map<String, String> tempSegmentNameMapping;
    private final Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    public RemoteDirectoryWrapper(RemoteDirectory remoteDirectory, long primaryTerm) throws IOException {
        super(remoteDirectory);
        this.primaryTerm = primaryTerm;
        this.remoteDirectory = remoteDirectory;
        // Read latest mapping file and populate this map with list of files uploaded
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(readLatestMappingFile(remoteDirectory));
        this.tempSegmentNameMapping = new ConcurrentHashMap<>();
    }

    private Map<String, UploadedSegmentMetadata> readLatestMappingFile(RemoteDirectory remoteDirectory) throws IOException {
        Collection<String> mappingFiles = remoteDirectory.listFilesByPrefix("mapping_file");
        Optional<String> latestMappingFile = mappingFiles.stream().max(new RemoteFilenameComparator());
        if(latestMappingFile.isPresent()) {
            String latestMappingFilename = latestMappingFile.get();
            IndexInput indexInput = remoteDirectory.openInput(latestMappingFilename, IOContext.DEFAULT);
            Map<String, String> segmentMapping = indexInput.readMapOfStrings();
            Map<String, UploadedSegmentMetadata> segmentMetadataMapping = segmentMapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                String[] values = entry.getValue().split(SEPARATOR);
                return new UploadedSegmentMetadata(values[0], values[1], values[2]);
            }));
            String latestMappingFileLocalName = getLocalFilename(latestMappingFilename);
            segmentMetadataMapping.put(latestMappingFileLocalName, new UploadedSegmentMetadata(latestMappingFileLocalName, latestMappingFilename, "0"));
            return segmentMetadataMapping;
        } else {
            return new HashMap<>();
        }
    }

    static class UploadedSegmentMetadata {
        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return originalFilename + SEPARATOR + uploadedFilename + SEPARATOR + checksum;
        }
    }

    static class RemoteFilenameComparator implements Comparator<String> {

        @Override
        public int compare(String first, String second) {
            String[] firstTokens = first.split(SEPARATOR);
            String[] secondTokens = second.split(SEPARATOR);
            long firstPrimaryTerm = Long.parseLong(firstTokens[2]);
            long secondPrimaryTerm = Long.parseLong(secondTokens[2]);
            if(firstPrimaryTerm != secondPrimaryTerm) {
                return (int) (firstPrimaryTerm - secondPrimaryTerm);
            } else {
                int firstGeneration = Integer.parseInt(firstTokens[1], Character.MAX_RADIX);
                int secondGeneration = Integer.parseInt(secondTokens[1], Character.MAX_RADIX);
                if(firstGeneration != secondGeneration) {
                    return firstGeneration - secondGeneration;
                } else {
                    return firstTokens[3].compareTo(secondTokens[3]);
                }
            }
        }
    }

    @Override
    public String[] listAll() throws IOException {
        return listAll(false);
    }

    public String[] listAll(boolean includePendingDeletions) throws IOException {
        if(includePendingDeletions) {
            return super.listAll();
        } else {
            return segmentsUploadedToRemoteStore.keySet().toArray(new String[0]);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        remoteDirectory.deleteFile(getExistingRemoteFilename(name));
        segmentsUploadedToRemoteStore.remove(name);
        tempSegmentNameMapping.remove(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return remoteDirectory.fileLength(getExistingRemoteFilename(name));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return remoteDirectory.createOutput(getNewRemoteFilename(name), context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return remoteDirectory.createTempOutput(getNewRemoteFilename(prefix), suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        Set<String> remoteFilenames = names.stream().map(this::getExistingRemoteFilename).collect(Collectors.toSet());
        remoteDirectory.sync(remoteFilenames);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        remoteDirectory.rename(getExistingRemoteFilename(source), getNewRemoteFilename(dest));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return remoteDirectory.openInput(getExistingRemoteFilename(name), context);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return remoteDirectory.obtainLock(getExistingRemoteFilename(name));
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return null;
    }

    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        remoteDirectory.copyFrom(from, src, dest, context);
        String checksum = getChecksumOfLocalFile(from, src);
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(src, tempSegmentNameMapping.get(src), checksum);
        segmentsUploadedToRemoteStore.put(src, metadata);
        tempSegmentNameMapping.remove(src);
    }

    public boolean containsFile(String localFilename) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename);
    }

    private String getChecksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    private String getLocalFilename(String remoteFilename) {
        return remoteFilename.split(SEPARATOR)[0];
    }

    private String getExistingRemoteFilename(String localFilename) {
        return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
    }

    private String getNewRemoteFilename(String localFilename) {
        String remoteFileName = localFilename + SEPARATOR + primaryTerm + SEPARATOR + UUIDs.base64UUID();
        tempSegmentNameMapping.put(localFilename, remoteFileName);
        return remoteFileName;
    }
}
