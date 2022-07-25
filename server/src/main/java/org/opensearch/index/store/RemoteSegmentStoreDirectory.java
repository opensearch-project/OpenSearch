/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.List;
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
    private String refreshMappingFileUniqueSuffix;
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final Logger logger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);

    public RemoteSegmentStoreDirectory(RemoteDirectory remoteDataDirectory, RemoteDirectory remoteMetadataDirectory) throws IOException {
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.refreshMappingFileUniqueSuffix = UUIDs.base64UUID();
        // Read latest mapping file and populate this map with list of files uploaded
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(readLatestMappingFile());
    }

    private Map<String, UploadedSegmentMetadata> readLatestMappingFile() throws IOException {
        Map<String, UploadedSegmentMetadata> segmentMetadataMap = new HashMap<>();
        Collection<String> commitMappingFiles = remoteMetadataDirectory.listFilesByPrefix(COMMIT_MAPPING_PREFIX);
        Optional<String> latestCommitMappingFile = commitMappingFiles.stream().max(new MappingFilenameComparator());

        if(latestCommitMappingFile.isPresent()) {
            readMappingFile(latestCommitMappingFile.get(), segmentMetadataMap);
            this.refreshMappingFileUniqueSuffix = latestCommitMappingFile.get().split(SEPARATOR)[3];
        }

        Collection<String> refreshMappingFiles = remoteMetadataDirectory.listFilesByPrefix(REFRESH_MAPPING_PREFIX);
        Optional<String> latestRefreshMappingFile = refreshMappingFiles.stream().filter(file -> {
            if(latestCommitMappingFile.isPresent()) {
                return file.endsWith(latestCommitMappingFile.get().split(SEPARATOR)[3]);
            } else {
                return true;
            }
        }).max(new MappingFilenameComparator());

        String latestRefreshMappingFilename = null;
        if(latestRefreshMappingFile.isPresent()) {
            String refreshMappingFile = latestRefreshMappingFile.get();
            if(latestCommitMappingFile.isPresent()) {
                String commitMappingFile = latestCommitMappingFile.get();
                int suffixComparison = MappingFilenameComparator.compareSuffix(refreshMappingFile, commitMappingFile);
                if(suffixComparison >= 0) {
                    latestRefreshMappingFilename = refreshMappingFile;
                }
            } else {
                latestRefreshMappingFilename = refreshMappingFile;
            }
        }
        if(latestRefreshMappingFilename != null) {
            readMappingFile(latestRefreshMappingFilename, segmentMetadataMap);
        }
        return segmentMetadataMap;
    }

    private void readMappingFile(String mappingFilename, Map<String, UploadedSegmentMetadata> segmentMetadataMap) throws IOException {
        try (IndexInput indexInput = remoteMetadataDirectory.openInput(mappingFilename, IOContext.DEFAULT)) {
            Map<String, String> segmentMapping = indexInput.readMapOfStrings();
            segmentMapping.entrySet().stream().filter(entry -> !segmentMetadataMap.containsKey(entry.getKey())).forEach(entry -> {
                segmentMetadataMap.put(entry.getKey(), UploadedSegmentMetadata.fromString(entry.getValue()));
            });
        }
    }

    static class UploadedSegmentMetadata {
        private static final String SEPARATOR = "::";
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

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            String[] values = uploadedFilename.split(SEPARATOR);
            return new UploadedSegmentMetadata(values[0], values[1], values[2]);
        }
    }

    static class MappingFilenameComparator implements Comparator<String> {

        @Override
        public int compare(String first, String second) {
            int suffixComparison = compareSuffix(first, second);
            if(suffixComparison == 0) {
                return first.split(SEPARATOR)[3].compareTo(second.split(SEPARATOR)[3]);
            } else {
                return suffixComparison;
            }
        }

        public static int compareSuffix(String first, String second) {
            String[] firstTokens = first.split(SEPARATOR);
            String[] secondTokens = second.split(SEPARATOR);
            long firstPrimaryTerm = Long.parseLong(firstTokens[1]);
            long secondPrimaryTerm = Long.parseLong(secondTokens[1]);
            if(firstPrimaryTerm != secondPrimaryTerm) {
                return (int) (firstPrimaryTerm - secondPrimaryTerm);
            } else {
                int firstGeneration = Integer.parseInt(firstTokens[2], Character.MAX_RADIX);
                int secondGeneration = Integer.parseInt(secondTokens[2], Character.MAX_RADIX);
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
        return readLatestMappingFile().keySet().toArray(new String[0]);
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
        return remoteDataDirectory.createOutput(getNewRemoteSegmentFilename(name), context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return remoteDataDirectory.createTempOutput(getNewRemoteSegmentFilename(prefix), suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        Set<String> remoteFilenames = names.stream().map(this::getExistingRemoteFilename).collect(Collectors.toSet());
        remoteDataDirectory.sync(remoteFilenames);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        remoteDataDirectory.rename(getExistingRemoteFilename(source), getNewRemoteSegmentFilename(dest));
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
        String remoteFilename = getNewRemoteSegmentFilename(dest);
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        String checksum = getChecksumOfLocalFile(from, src);
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(src, remoteFilename, checksum);
        segmentsUploadedToRemoteStore.put(src, metadata);
    }

    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename) && segmentsUploadedToRemoteStore.get(localFilename).checksum.equals(checksum);
    }

    public void uploadCommitMapping(Collection<String> committedFiles, Directory storeDirectory, long primaryTerm, long generation) throws IOException {
        String commitFilename = getNewRemoteFilename(COMMIT_MAPPING_PREFIX, primaryTerm, generation);
        uploadMappingFile(committedFiles, storeDirectory, commitFilename);
        this.refreshMappingFileUniqueSuffix = commitFilename.split(SEPARATOR)[3];
    }

    public void uploadRefreshMapping(Collection<String> refreshedFiles, Directory storeDirectory, long primaryTerm, long generation) throws IOException {
        String refreshFilename = getNewRemoteFilename(REFRESH_MAPPING_PREFIX, primaryTerm, generation, this.refreshMappingFileUniqueSuffix);
        uploadMappingFile(refreshedFiles, storeDirectory, refreshFilename);
    }

    private void uploadMappingFile(Collection<String> files, Directory storeDirectory, String filename) throws IOException {
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeMapOfStrings(segmentsUploadedToRemoteStore.entrySet().stream().filter(entry -> files.contains(entry.getKey())).collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
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
        if(segmentsUploadedToRemoteStore.containsKey(localFilename)) {
            return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
        } else {
            return null;
        }
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEPARATOR + UUIDs.base64UUID();
    }
    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(SEPARATOR)[0];
    }

    private String getNewRemoteFilename(String localFilename, long primaryTerm, long generation) {
        return getNewRemoteFilename(localFilename, primaryTerm, generation, UUIDs.base64UUID());
    }

    private String getNewRemoteFilename(String localFilename, long primaryTerm, long generation, String uuid) {
        return localFilename + SEPARATOR + primaryTerm + SEPARATOR + Long.toString(generation, Character.MAX_RADIX) + SEPARATOR + uuid;
    }

    public void deleteStaleCommits(int lastNCommitsToKeep) throws IOException {
        Collection<String> commitMappingFiles = remoteMetadataDirectory.listFilesByPrefix(COMMIT_MAPPING_PREFIX);
        List<String> sortedMappingFileList = commitMappingFiles.stream().sorted(new MappingFilenameComparator()).collect(Collectors.toList());
        if(sortedMappingFileList.size() <= lastNCommitsToKeep) {
            logger.info("Number of commits in remote segment store={}, lastNCommitsToKeep={}", sortedMappingFileList.size(), lastNCommitsToKeep);
            return;
        }
        List<String> latestNCommitFiles = sortedMappingFileList.subList(sortedMappingFileList.size() - lastNCommitsToKeep, sortedMappingFileList.size());
        Map<String, UploadedSegmentMetadata> activeSegmentFilesMetadataMap = new HashMap<>();
        for(String commitFile: latestNCommitFiles) {
            readMappingFile(commitFile, activeSegmentFilesMetadataMap);
        }
        Set<String> activeSegmentRemoteFilenames = activeSegmentFilesMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet());
        for(String commitFile: sortedMappingFileList.subList(0, sortedMappingFileList.size() - lastNCommitsToKeep)) {
            Map<String, UploadedSegmentMetadata> staleSegmentFilesMetadataMap = new HashMap<>();
            readMappingFile(commitFile, staleSegmentFilesMetadataMap);
            Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet());
            staleSegmentRemoteFilenames.stream().filter(file -> !activeSegmentRemoteFilenames.contains(file)).forEach(file -> {
                try {
                    logger.info("Deleting stale segment file {} from remote segment store", file);
                    remoteDataDirectory.deleteFile(file);
                    if(!activeSegmentFilesMetadataMap.containsKey(getLocalSegmentFilename(file))) {
                        segmentsUploadedToRemoteStore.remove(getLocalSegmentFilename(file));
                    }
                } catch (IOException e) {
                    logger.info("Exception while deleting segment files related to commit file {}. Deletion will be re-tried", commitFile);
                }
            });
            remoteMetadataDirectory.deleteFile(commitFile);
            remoteMetadataDirectory.deleteFile(REFRESH_MAPPING_PREFIX + commitFile.substring(COMMIT_MAPPING_PREFIX.length()));
        }
    }
}
