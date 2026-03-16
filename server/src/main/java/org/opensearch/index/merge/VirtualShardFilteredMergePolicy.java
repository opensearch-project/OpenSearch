/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.VirtualShardRoutingHelper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for extracting documents for a target virtual shard.
 *
 * @opensearch.internal
 */
public final class VirtualShardFilteredMergePolicy {

    private static final Logger logger = LogManager.getLogger(VirtualShardFilteredMergePolicy.class);
    private static final int MAX_UNWRAP_DEPTH = 100;

    private VirtualShardFilteredMergePolicy() {}

    /**
     * Extracts documents for a target virtual shard. 
     * Note: The index at {@code outputPath} will be recreated/overwritten.
     */
    public static void isolateVirtualShard(IndexShard shard, IndexMetadata indexMetadata, int vShardId, Path outputPath)
        throws IOException {

        try (var searcher = shard.acquireSearcher("vshard_extract")) {
            List<CodecReader> wrappedReaders = new ArrayList<>();
            org.apache.lucene.codecs.Codec codec = null;

            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                LeafReader leafReader = ctx.reader();
                CodecReader codecReader = unwrapToCodecReader(leafReader);
                if (codecReader == null) {
                    logger.trace("skipping leaf [{}]: unable to unwrap to CodecReader", leafReader.getClass());
                    continue;
                }

                if (codec == null && codecReader instanceof SegmentReader) {
                    codec = ((SegmentReader) codecReader).getSegmentInfo().info.getCodec();
                }

                Bits sourceLiveDocs = leafReader.getLiveDocs();
                wrappedReaders.add(new VirtualShardFilterReader(codecReader, sourceLiveDocs, indexMetadata, vShardId));
            }

            IndexWriterConfig iwc = new IndexWriterConfig(null);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            if (codec != null) {
                iwc.setCodec(codec);
            }

            try (Directory outDir = FSDirectory.open(outputPath); IndexWriter writer = new IndexWriter(outDir, iwc)) {
                if (!wrappedReaders.isEmpty()) {
                    writer.addIndexes(wrappedReaders.toArray(new CodecReader[0]));
                    writer.forceMerge(1);
                }
                writer.commit();
            }
        }
    }

    private static CodecReader unwrapToCodecReader(LeafReader reader) {
        LeafReader current = reader;
        for (int i = 0; i < MAX_UNWRAP_DEPTH; i++) {
            if (current instanceof CodecReader) {
                return (CodecReader) current;
            }
            if (current instanceof FilterLeafReader) {
                current = ((FilterLeafReader) current).getDelegate();
            } else {
                break;
            }
        }
        if (current instanceof CodecReader) {
            return (CodecReader) current;
        }
        logger.error("failed to unwrap to CodecReader after {} iterations, terminal type: {}", MAX_UNWRAP_DEPTH, current.getClass());
        return null;
    }

    static final class VirtualShardFilterReader extends FilterCodecReader {
        private final Bits liveDocs;
        private final int numDocs;

        VirtualShardFilterReader(CodecReader in, Bits sourceLiveDocs, IndexMetadata indexMetadata, int targetVShardId) throws IOException {
            super(in);

            int maxDoc = in.maxDoc();
            int count = 0;
            boolean[] membership = new boolean[maxDoc];

            StoredFields storedFields = in.storedFields();
            for (int docId = 0; docId < maxDoc; docId++) {
                if (sourceLiveDocs != null && !sourceLiveDocs.get(docId)) {
                    continue;
                }
                VShardFieldVisitor visitor = new VShardFieldVisitor();
                storedFields.document(docId, visitor);
                if (visitor.id == null) {
                    continue;
                }
                int computedVShard = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, visitor.id, visitor.routing);
                if (computedVShard == targetVShardId) {
                    membership[docId] = true;
                    count++;
                }
            }

            final boolean[] membershipFinal = membership;
            final Bits srcLive = sourceLiveDocs;
            this.numDocs = count;
            this.liveDocs = new Bits() {
                @Override
                public boolean get(int index) {
                    if (srcLive != null && !srcLive.get(index)) {
                        return false;
                    }
                    return membershipFinal[index];
                }

                @Override
                public int length() {
                    return maxDoc;
                }
            };
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    private static final class VShardFieldVisitor extends StoredFieldVisitor {
        String id;
        String routing;
        private int leftToVisit = 2;

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) throws IOException {
            if (RoutingFieldMapper.NAME.equals(fieldInfo.name)) {
                routing = value;
            } else if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                id = value;
            }
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            switch (fieldInfo.name) {
                case IdFieldMapper.NAME:
                case RoutingFieldMapper.NAME:
                    leftToVisit--;
                    return Status.YES;
                default:
                    return leftToVisit == 0 ? Status.STOP : Status.NO;
            }
        }
    }
}
