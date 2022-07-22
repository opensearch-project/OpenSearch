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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A RemoteDirectory wrapper
 * @opensearch.internal
 */
public final class RemoteSegmentStoreDirectory extends FilterDirectory {
    public static final String SEPARATOR = "__";
    public static final String COMMIT_MAPPING_PREFIX = "commit_mapping";
    public static final String REFRESH_MAPPING_PREFIX = "refresh_mapping";

    private final RemoteDirectory remoteDataDirectory;
    private final RemoteDirectory remoteMetadataDirectory;
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;
    private String lastRefreshMappingFile;

    public RemoteSegmentStoreDirectory(RemoteDirectory remoteDataDirectory, RemoteDirectory remoteMetadataDirectory) throws IOException {
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        // Read latest mapping file and populate this map with list of files uploaded
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(readLatestMappingFile());
    }

    private Map<String, UploadedSegmentMetadata> readLatestMappingFile() throws IOException {
        Collection<String> commitMappingFiles = remoteMetadataDirectory.listFilesByPrefix(COMMIT_MAPPING_PREFIX);
        Optional<String> latestCommitMappingFile = commitMappingFiles.stream().max(new RemoteFilenameComparator());

        Collection<String> refreshMappingFiles = remoteMetadataDirectory.listFilesByPrefix(REFRESH_MAPPING_PREFIX);
        Optional<String> latestRefreshMappingFile = refreshMappingFiles.stream().max(new RemoteFilenameComparator());

        String latestMappingFilename;
        if(latestRefreshMappingFile.isPresent()) {
            String refreshMappingFile = latestRefreshMappingFile.get();
            this.lastRefreshMappingFile = refreshMappingFile;
            if(latestCommitMappingFile.isPresent()) {
                String commitMappingFile = latestCommitMappingFile.get();
                String[] refreshMappingFileTokens = refreshMappingFile.split(SEPARATOR);
                String[] commitMappingFileTokens = commitMappingFile.split(SEPARATOR);
                int suffixComparison = RemoteFilenameComparator.compareSuffix(refreshMappingFileTokens, commitMappingFileTokens);
                if(suffixComparison >= 0) {
                    latestMappingFilename = refreshMappingFile;
                } else {
                    latestMappingFilename = commitMappingFile;
                }
            } else {
                latestMappingFilename = refreshMappingFile;
            }
        } else if (latestCommitMappingFile.isPresent()){
            latestMappingFilename = latestCommitMappingFile.get();
        } else {
            return new HashMap<>();
        }
        IndexInput indexInput = remoteMetadataDirectory.openInput(latestMappingFilename, IOContext.DEFAULT);
        Map<String, String> segmentMapping = indexInput.readMapOfStrings();
        Map<String, UploadedSegmentMetadata> segmentMetadataMap = segmentMapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            String[] values = entry.getValue().split(SEPARATOR);
            return new UploadedSegmentMetadata(values[0], values[1], values[2]);
        }));
        String mappingPrefix = latestMappingFilename.split(SEPARATOR)[0];
        segmentMetadataMap.put(mappingPrefix, new UploadedSegmentMetadata(mappingPrefix, latestMappingFilename, "0"));
        return segmentMetadataMap;
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
            int suffixComparison = compareSuffix(firstTokens, secondTokens);
            if(suffixComparison == 0) {
                return firstTokens[3].compareTo(secondTokens[3]);
            } else {
                return suffixComparison;
            }
        }

        public static int compareSuffix(String[] firstTokens, String[] secondTokens) {
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
                    return 0;
                }
            }
        }
    }

    public void init() throws IOException {
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(readLatestMappingFile());
    }

    @Override
    public String[] listAll() throws IOException {
        init();
        return segmentsUploadedToRemoteStore.keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        remoteDataDirectory.deleteFile(getExistingRemoteFilename(name));
        segmentsUploadedToRemoteStore.remove(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return remoteDataDirectory.fileLength(getExistingRemoteFilename(name));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return remoteDataDirectory.createOutput(getNewRemoteFilename(name), context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return remoteDataDirectory.createTempOutput(getNewRemoteFilename(prefix), suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        Set<String> remoteFilenames = names.stream().map(this::getExistingRemoteFilename).collect(Collectors.toSet());
        remoteDataDirectory.sync(remoteFilenames);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        remoteDataDirectory.rename(getExistingRemoteFilename(source), getNewRemoteFilename(dest));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return remoteDataDirectory.openInput(getExistingRemoteFilename(name), context);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return remoteDataDirectory.obtainLock(getExistingRemoteFilename(name));
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return null;
    }

    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        String remoteFilename = getNewRemoteFilename(dest);
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        String checksum = getChecksumOfLocalFile(from, src);
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(src, remoteFilename, checksum);
        segmentsUploadedToRemoteStore.put(src, metadata);
    }

    public boolean containsFile(String localFilename) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename);
    }

    public void uploadCommitMapping(Directory storeDirectory, long generation, long primaryTerm) throws IOException {
        String commitFilename = getNewRemoteFilename(COMMIT_MAPPING_PREFIX, generation, primaryTerm);
        uploadMappingFile(storeDirectory, commitFilename);
    }

    public void uploadRefreshMapping(Directory storeDirectory, long generation, long primaryTerm) throws IOException {
        String refreshFilename = getNewRemoteFilename(REFRESH_MAPPING_PREFIX, generation, primaryTerm);
        int suffixComparison = 1;
        if(this.lastRefreshMappingFile != null) {
            suffixComparison = RemoteFilenameComparator.compareSuffix(this.lastRefreshMappingFile.split(SEPARATOR), refreshFilename.split(SEPARATOR));
            if (suffixComparison == 0) {
                refreshFilename = this.lastRefreshMappingFile;
            }
        }
        uploadMappingFile(storeDirectory, refreshFilename);
        if(suffixComparison != 0) {
            this.lastRefreshMappingFile = refreshFilename;
        }
    }

    private void uploadMappingFile(Directory storeDirectory, String filename) throws IOException {
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeMapOfStrings(segmentsUploadedToRemoteStore.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(filename));
        remoteMetadataDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT);
        storeDirectory.deleteFile(filename);
    }

    private String getChecksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    private String getExistingRemoteFilename(String localFilename) {
        return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
    }

    private String getNewRemoteFilename(String localFilename) {
        return localFilename + SEPARATOR + UUIDs.base64UUID();
    }

    private String getNewRemoteFilename(String localFilename, long generation, long primaryTerm) {
        return localFilename + SEPARATOR + Long.toString(generation, Character.MAX_RADIX) + SEPARATOR + primaryTerm + SEPARATOR + UUIDs.base64UUID();
    }

}
