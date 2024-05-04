/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class FieldTypeInference {
    private final IndexReader indexReader;
    private final String indexName;
    private final MapperService mapperService;
    // TODO expose using a index setting?
    private int sampleSize;

    // this will lead to the probability of more than 0.95 to select on the document containing this field,
    // when at least 5% of the overall documents contain the field
    private static final int DEFAULT_SAMPLE_SIZE = 60;

    private final int MAX_ATTEMPTS_TO_GENERATE_RANDOM_SAMPLES = 10000;

    public FieldTypeInference(String indexName, MapperService mapperService, IndexReader indexReader) {
        this.indexName = indexName;
        this.mapperService = mapperService;
        this.indexReader = indexReader;
        this.sampleSize = DEFAULT_SAMPLE_SIZE;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public Mapper infer(ValueFetcher valueFetcher) throws IOException {
        int iter = 0;
        int totalDocs = indexReader.numDocs();
        int sampleSize = Math.min(totalDocs, getSampleSize());
        int[] docs = getSortedRandomNum(sampleSize, totalDocs, Math.max(getSampleSize(), MAX_ATTEMPTS_TO_GENERATE_RANDOM_SAMPLES));
        int offset = 0;
        SourceLookup sourceLookup = new SourceLookup();
        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            LeafReader leafReader = leafReaderContext.reader();
            valueFetcher.setNextReader(leafReaderContext);
            if (iter >= docs.length) {
                break;
            }
            int docID = docs[iter] - offset;
            while (docID < leafReader.numDocs()) {
                sourceLookup.setSegmentAndDocument(leafReaderContext, docID);
                List<Object> objects = valueFetcher.fetchValues(sourceLookup);
                Mapper inferredMapper = null;
                if (objects != null && !objects.isEmpty()) {
                    // always using first value in case of multi value field
                    inferredMapper = inferTypeFromObject(objects.get(0));
                }
                if (inferredMapper != null) {
                    return inferredMapper;
                }
                iter++;
                if (iter >= docs.length) {
                    break;
                }
                docID = docs[iter] - offset;
            }
            offset += leafReader.numDocs();
        }
        return null;
    }

    private static int[] getSortedRandomNum(int k, int n, int attempts) {
        Set<Integer> generatedNumbers = new HashSet<>();
        Random random = new Random();
        int itr = 0;
        while (generatedNumbers.size() < k && itr++ < attempts) {
            int randomNumber = random.nextInt(n);
            generatedNumbers.add(randomNumber);
        }
        int[] result = new int[generatedNumbers.size()];
        int i = 0;
        for (int number : generatedNumbers) {
            result[i++] = number;
        }
        Arrays.sort(result);
        return result;
    }

    private Mapper inferTypeFromObject(Object o) throws IOException {
        if (o == null) {
            return null;
        }
        DocumentMapper mapper = mapperService.documentMapper();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("field", o).endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        SourceToParse sourceToParse = new SourceToParse(indexName, "_id", bytesReference, JsonXContent.jsonXContent.mediaType());
        ParsedDocument parsedDocument = mapper.parse(sourceToParse);
        Mapping mapping = parsedDocument.dynamicMappingsUpdate();
        return mapping.root.getMapper("field");
    }
}
