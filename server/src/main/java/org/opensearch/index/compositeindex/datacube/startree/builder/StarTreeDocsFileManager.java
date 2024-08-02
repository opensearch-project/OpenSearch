/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Star tree document file manager.
 * This class manages all the temporary files associated with off heap star tree builder.
 * <p>
 * Star tree documents are stored in multiple 'star-tree.documents' files. The algorithm works as follows:
 * <ol>
 * <li> Initially, aggregated documents are created based on the segment documents.</li>
 * <li> Further, star tree documents are generated (e.g., in the {@code generateStarTreeDocumentsForStarNode} method) by reading the current
 * aggregated documents and creating new aggregated star tree documents, which are appended to the 'star-tree.documents' files. </li>
 * <li> This process is repeated until all combinations of star tree documents are generated. </li>
 * </ol>
 * <p>In cases where previously written star tree documents need to be read from the 'star-tree.documents' files, the current
 * 'star-tree.documents' file is closed, and the values are read. Then, the derived values gets appended to a new 'star-tree.documents' file.
 * This is necessary because Lucene maintains immutability of data, and an {@code IndexOutput} cannot be kept open while creating an
 * {@code IndexInput} on the same file, as all file contents may not be visible in the reader. Therefore, the {@code IndexOutput} must be
 * closed to ensure all data can be read before creating an {@code IndexInput}. Additionally, an {@code IndexOutput} cannot be reopened,
 * so a new file is created for the new star tree documents.
 * <p>The set of 'star-tree.documents' files is maintained, and a tracker array is used to keep track of the start document ID for each file.
 * Once the number of files reaches a set threshold, the files are merged.
 *
 */
public class StarTreeDocsFileManager extends AbstractDocumentsFileManager implements Closeable {
    private static final Logger logger = LogManager.getLogger(StarTreeDocsFileManager.class);
    private static final String STAR_TREE_DOC_FILE_NAME = "star-tree.documents";
    public static final int DEFAULT_FILE_COUNT_MERGE_THRESHOLD = 5;
    private IndexInput starTreeDocsFileInput;
    private RandomAccessInput starTreeDocsFileRandomInput;
    private IndexOutput starTreeDocsFileOutput;
    private final Map<String, Integer> fileToEndDocIdMap;
    private final List<Integer> starTreeDocumentOffsets = new ArrayList<>();
    private int currentFileStartDocId;
    private int numReadableStarTreeDocuments;
    private int starTreeFileCount = -1;
    private int currBytes = 0;
    private final int fileCountMergeThreshold;
    private int numStarTreeDocs = 0;

    public StarTreeDocsFileManager(SegmentWriteState state, StarTreeField starTreeField, List<MetricAggregatorInfo> metricAggregatorInfos)
        throws IOException {
        this(state, starTreeField, metricAggregatorInfos, DEFAULT_FILE_COUNT_MERGE_THRESHOLD);
    }

    public StarTreeDocsFileManager(
        SegmentWriteState state,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        int fileCountThreshold
    ) throws IOException {
        super(state, starTreeField, metricAggregatorInfos);
        fileToEndDocIdMap = new LinkedHashMap<>();
        try {
            starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
            IOUtils.closeWhileHandlingException(this);
            throw e;
        }
        fileCountMergeThreshold = fileCountThreshold;
    }

    /**
     * Creates a new star tree document temporary file to store star tree documents.
     */
    IndexOutput createStarTreeDocumentsFileOutput() throws IOException {
        starTreeFileCount++;
        return tmpDirectory.createTempOutput(STAR_TREE_DOC_FILE_NAME + starTreeFileCount, state.segmentSuffix, state.context);
    }

    @Override
    public void writeStarTreeDocument(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) throws IOException {
        assert isAggregatedDoc == true;
        int numBytes = writeStarTreeDocument(starTreeDocument, starTreeDocsFileOutput, true);
        addStarTreeDocumentOffset(numBytes);
        numStarTreeDocs++;
    }

    @Override
    public StarTreeDocument readStarTreeDocument(int docId, boolean isAggregatedDoc) throws IOException {
        assert isAggregatedDoc == true;
        ensureDocumentReadable(docId);
        return readStarTreeDocument(starTreeDocsFileRandomInput, starTreeDocumentOffsets.get(docId), true);
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        Long[] dims = readDimensions(docId);
        return dims[dimensionId];
    }

    @Override
    public Long[] readDimensions(int docId) throws IOException {
        ensureDocumentReadable(docId);
        Long[] dims = new Long[starTreeField.getDimensionsOrder().size()];
        readDimensions(dims, starTreeDocsFileRandomInput, starTreeDocumentOffsets.get(docId));
        return dims;
    }

    private void addStarTreeDocumentOffset(int bytes) {
        starTreeDocumentOffsets.add(currBytes);
        currBytes += bytes;
        if (docSizeInBytes == -1) {
            docSizeInBytes = bytes;
        }
        assert docSizeInBytes == bytes;
    }

    /**
     * Load the correct StarTreeDocuments file based on the docId
     */
    private void ensureDocumentReadable(int docId) throws IOException {
        ensureDocumentReadable(docId, true);
    }

    private void ensureDocumentReadable(int docId, boolean shouldCreateFileOutput) throws IOException {
        try {
            if (docId >= currentFileStartDocId && docId < numReadableStarTreeDocuments) {
                return;
            }
            IOUtils.closeWhileHandlingException(starTreeDocsFileInput);
            starTreeDocsFileInput = null;
            if (docId < numStarTreeDocs) {
                loadStarTreeDocumentFile(docId);
            }
            if (starTreeDocsFileInput != null) {
                return;
            }
            closeAndMaybeCreateNewFile(shouldCreateFileOutput, numStarTreeDocs);
            loadStarTreeDocumentFile(docId);
        } catch (IOException ex) {
            IOUtils.closeWhileHandlingException(this);
            throw ex;
        }
    }

    /**
     * The fileToByteSizeMap is in the following format
     * file1 == 521
     * file2 == 780
     * which represents that file1 contains all docs till "520".
     * <p>
     * "currentFileStartDocId" tracks the "start doc id" of the range in the present 'star-tree.documents' file
     * "numReadableStarTreeDocuments" tracks the "end doc id + 1" of the range of docs in the present file
     */
    private void loadStarTreeDocumentFile(int docId) throws IOException {
        int currentFileStartDocId = 0;
        for (Map.Entry<String, Integer> entry : fileToEndDocIdMap.entrySet()) {
            if (docId < entry.getValue()) {
                starTreeDocsFileInput = tmpDirectory.openInput(entry.getKey(), state.context);
                starTreeDocsFileRandomInput = starTreeDocsFileInput.randomAccessSlice(
                    starTreeDocsFileInput.getFilePointer(),
                    starTreeDocsFileInput.length() - starTreeDocsFileInput.getFilePointer()
                );
                numReadableStarTreeDocuments = entry.getValue();
                break;
            }
            currentFileStartDocId = entry.getValue();
        }
        this.currentFileStartDocId = currentFileStartDocId;
    }

    /**
     * This case handles when the requested document ID is beyond the range of the currently open 'star-tree.documents' file.
     * In this scenario, the following steps are taken:
     * <p>
     * 1. Close the current 'star-tree.documents' file.
     * 2. Create a new 'star-tree.documents' file if the operation involves appending new documents.
     *    If the operation is only for reading existing documents, a new file is not created.
     */
    private void closeAndMaybeCreateNewFile(boolean shouldCreateFileForAppend, int numStarTreeDocs) throws IOException {
        if (starTreeDocsFileOutput != null) {
            IOUtils.close(starTreeDocsFileOutput);
        }
        currBytes = 0;
        if (starTreeDocsFileOutput != null) {
            fileToEndDocIdMap.put(starTreeDocsFileOutput.getName(), numStarTreeDocs);
        }
        if (shouldCreateFileForAppend) {
            starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
            if (fileToEndDocIdMap.size() >= fileCountMergeThreshold) {
                mergeFiles(numStarTreeDocs);
            }
        }
        if (starTreeDocsFileRandomInput != null) {
            starTreeDocsFileRandomInput = null;
        }
    }

    /**
     * Merge temporary star tree files once the number of files reach threshold
     */
    private void mergeFiles(int numStarTreeDocs) throws IOException {
        long st = System.currentTimeMillis();
        try (IndexOutput mergedOutput = createStarTreeDocumentsFileOutput()) {
            long mergeBytes = mergeFilesToOutput(mergedOutput);
            logger.debug(
                "Created merge file : {} in : {} ms with size of : {} KB",
                starTreeDocsFileOutput.getName(),
                System.currentTimeMillis() - st,
                mergeBytes / 1024
            );

            deleteOldFiles();
            fileToEndDocIdMap.clear();
            fileToEndDocIdMap.put(mergedOutput.getName(), numStarTreeDocs);
            resetStarTreeDocumentOffsets();
        }
    }

    /**
     * Merge all files to single IndexOutput
     */
    private long mergeFilesToOutput(IndexOutput mergedOutput) throws IOException {
        long mergeBytes = 0L;
        for (Map.Entry<String, Integer> entry : fileToEndDocIdMap.entrySet()) {
            IndexInput input = tmpDirectory.openInput(entry.getKey(), state.context);
            mergedOutput.copyBytes(input, input.length());
            mergeBytes += input.length();
            input.close();
        }
        return mergeBytes;
    }

    /**
     * Delete the old star-tree.documents files
     */
    private void deleteOldFiles() throws IOException {
        for (String fileName : fileToEndDocIdMap.keySet()) {
            tmpDirectory.deleteFile(fileName);
        }
    }

    /**
     * Reset the star tree document offsets based on the merged file
     */
    private void resetStarTreeDocumentOffsets() {
        int curr = 0;
        for (int i = 0; i < starTreeDocumentOffsets.size(); i++) {
            starTreeDocumentOffsets.set(i, curr);
            curr += docSizeInBytes;
        }
    }

    @Override
    public void close() {
        try {
            if (starTreeDocsFileOutput != null) {
                IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
                try {
                    tmpDirectory.deleteFile(starTreeDocsFileOutput.getName());
                } catch (IOException ignored) {} // similar to IOUtils.deleteFilesIgnoringExceptions
            }
        } finally {
            IOUtils.closeWhileHandlingException(starTreeDocsFileInput, starTreeDocsFileOutput);
        }
        // Delete all temporary star tree document files
        for (String file : fileToEndDocIdMap.keySet()) {
            try {
                tmpDirectory.deleteFile(file);
            } catch (IOException ignored) {} // similar to IOUtils.deleteFilesIgnoringExceptions
        }
        starTreeDocumentOffsets.clear();
        fileToEndDocIdMap.clear();
    }
}
