/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.ProtobufSearchShardTask;
import org.opensearch.action.search.ProtobufSearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.AliasFilterParsingException;
import org.opensearch.indices.InvalidAliasNameException;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.server.proto.ShardSearchRequestProto;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.transport.TransportRequest;

import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * Shard level request that represents a search.
 * It provides all the methods that the {@link SearchContext} needs.
 * Provides a cache key based on its content that can be used to cache shard level response.
 *
 * @opensearch.internal
 */
public class ProtobufShardSearchRequest extends TransportRequest implements IndicesRequest {
    // TODO: proto message
    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private ShardSearchRequestProto.ShardSearchRequest shardSearchRequestProto;
    // private final String clusterAlias;
    // private final ShardId shardId;
    // private final int numberOfShards;
    // private final SearchType searchType;
    // private final Scroll scroll;
    // private final float indexBoost;
    // private final Boolean requestCache;
    // private final long nowInMillis;
    // private long inboundNetworkTime;
    // private long outboundNetworkTime;
    // private final boolean allowPartialSearchResults;
    // private final String[] indexRoutings;
    // private final String preference;
    // private final OriginalIndices originalIndices;

    // private boolean canReturnNullResponseIfMatchNoDocs;
    // private SearchSortValuesAndFormats bottomSortValues;

    // these are the only mutable fields, as they are subject to rewriting
    // private AliasFilter aliasFilter;
    // private SearchSourceBuilder source;
    // private final ShardSearchContextId readerId;
    // private final TimeValue keepAlive;

    public ProtobufShardSearchRequest(
        OriginalIndices originalIndices,
        ProtobufSearchRequest searchRequest,
        ShardId shardId,
        int numberOfShards,
        AliasFilter aliasFilter,
        float indexBoost,
        long nowInMillis,
        @Nullable String clusterAlias,
        String[] indexRoutings
    ) {
        this(
            originalIndices,
            searchRequest,
            shardId,
            numberOfShards,
            aliasFilter,
            indexBoost,
            nowInMillis,
            clusterAlias,
            indexRoutings,
            null,
            null
        );
    }

    public ProtobufShardSearchRequest(
        OriginalIndices originalIndices,
        ProtobufSearchRequest searchRequest,
        ShardId shardId,
        int numberOfShards,
        AliasFilter aliasFilter,
        float indexBoost,
        long nowInMillis,
        @Nullable String clusterAlias,
        String[] indexRoutings,
        ShardSearchContextId readerId,
        TimeValue keepAlive
    ) {
        this(
            originalIndices,
            shardId,
            numberOfShards,
            searchRequest.searchType(),
            searchRequest.source(),
            searchRequest.requestCache(),
            aliasFilter,
            indexBoost,
            searchRequest.allowPartialSearchResults(),
            indexRoutings,
            searchRequest.preference(),
            searchRequest.scroll(),
            nowInMillis,
            clusterAlias,
            readerId,
            keepAlive
        );
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
    }

    public ProtobufShardSearchRequest(ShardId shardId, long nowInMillis, AliasFilter aliasFilter) {
        this(
            OriginalIndices.NONE,
            shardId,
            -1,
            SearchType.QUERY_THEN_FETCH,
            null,
            null,
            aliasFilter,
            1.0f,
            false,
            Strings.EMPTY_ARRAY,
            null,
            null,
            nowInMillis,
            null,
            null,
            null
        );
    }

    private ProtobufShardSearchRequest(
        OriginalIndices originalIndices,
        ShardId shardId,
        int numberOfShards,
        SearchType searchType,
        SearchSourceBuilder source,
        Boolean requestCache,
        AliasFilter aliasFilter,
        float indexBoost,
        Boolean allowPartialSearchResults,
        String[] indexRoutings,
        String preference,
        Scroll scroll,
        long nowInMillis,
        @Nullable String clusterAlias,
        ShardSearchContextId readerId,
        TimeValue keepAlive
    ) {
        // this.shardId = shardId;
        // this.numberOfShards = numberOfShards;
        // this.searchType = searchType;
        // this.source = source;
        // this.requestCache = requestCache;
        // this.aliasFilter = aliasFilter;
        // this.indexBoost = indexBoost;
        // this.allowPartialSearchResults = allowPartialSearchResults;
        // this.indexRoutings = indexRoutings;
        // this.preference = preference;
        // this.scroll = scroll;
        // this.nowInMillis = nowInMillis;
        // this.inboundNetworkTime = 0;
        // this.outboundNetworkTime = 0;
        // this.clusterAlias = clusterAlias;
        // this.originalIndices = originalIndices;
        // this.readerId = readerId;
        // this.keepAlive = keepAlive;
        // assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
        
        ShardSearchRequestProto.OriginalIndices originalIndicesProto = ShardSearchRequestProto.OriginalIndices.newBuilder()
                        .addAllIndices(Arrays.stream(originalIndices.indices()).collect(Collectors.toList()))
                        .setIndicesOptions(ShardSearchRequestProto.OriginalIndices.IndicesOptions.newBuilder()
                                                .setIgnoreUnavailable(originalIndices.indicesOptions().ignoreUnavailable())
                                                .setAllowNoIndices(originalIndices.indicesOptions().allowNoIndices())
                                                .setExpandWildcardsOpen(originalIndices.indicesOptions().expandWildcardsOpen())
                                                .setExpandWildcardsClosed(originalIndices.indicesOptions().expandWildcardsClosed())
                                                .setExpandWildcardsHidden(originalIndices.indicesOptions().allowAliasesToMultipleIndices())
                                                .setAllowAliasesToMultipleIndices(originalIndices.indicesOptions().allowAliasesToMultipleIndices())
                                                .setForbidClosedIndices(originalIndices.indicesOptions().forbidClosedIndices())
                                                .setIgnoreAliases(originalIndices.indicesOptions().ignoreAliases())
                                                .setIgnoreThrottled(originalIndices.indicesOptions().ignoreThrottled())
                                                .build())
                        .build();
        ShardSearchRequestProto.ShardId shardIdProto = ShardSearchRequestProto.ShardId.newBuilder()
                        .setShardId(shardId.getId())
                        .setHashCode(shardId.hashCode())
                        .setIndexName(shardId.getIndexName())
                        .setIndexUUID(shardId.getIndex().getUUID())
                        .build();
        
        ShardSearchRequestProto.ShardSearchContextId.Builder shardSearchContextId = ShardSearchRequestProto.ShardSearchContextId.newBuilder();
        System.out.println("Reader id:  " + readerId);
        if (readerId != null) {
            shardSearchContextId.setSessionId(readerId.getSessionId());
            shardSearchContextId.setId(readerId.getId());
        }

        ShardSearchRequestProto.ShardSearchRequest.Builder builder = ShardSearchRequestProto.ShardSearchRequest.newBuilder();
        builder.setOriginalIndices(originalIndicesProto);
        builder.setShardId(shardIdProto);
        builder.setNumberOfShards(numberOfShards);
        builder.setSearchType(ShardSearchRequestProto.ShardSearchRequest.SearchType.QUERY_THEN_FETCH);
        builder.setSource(ByteString.copyFrom(convertToBytes(source)));
        builder.setInboundNetworkTime(0);
        builder.setOutboundNetworkTime(0);

        if (requestCache != null) {
            builder.setRequestCache(requestCache);
        }

        if (aliasFilter != null) {
            builder.setAliasFilter(ByteString.copyFrom(convertToBytes(aliasFilter)));
        }
        builder.setIndexBoost(indexBoost);

        if (allowPartialSearchResults != null) {
            builder.setAllowPartialSearchResults(allowPartialSearchResults);
        }

        if (indexRoutings != null) {
            builder.addAllIndexRoutings(Arrays.stream(indexRoutings).collect(Collectors.toList()));
        }

        if (preference != null) {
            builder.setPreference(preference);
        }

        if (scroll != null) {
            builder.setScroll(ByteString.copyFrom(convertToBytes(scroll)));
        }
        builder.setNowInMillis(nowInMillis);

        if (clusterAlias != null) {
            builder.setClusterAlias(clusterAlias);
        }
        if (readerId != null) {
            builder.setReaderId(shardSearchContextId.build());
        }
        
        System.out.println("Keep alive: " + keepAlive);
        if (keepAlive != null) {
            builder.setTimeValue(keepAlive.getStringRep());
        }

        this.shardSearchRequestProto = builder.build();
    }

    public ProtobufShardSearchRequest(byte[] in) throws IOException {
        super(in);
        this.shardSearchRequestProto = ShardSearchRequestProto.ShardSearchRequest.parseFrom(in);
    }

    public ProtobufShardSearchRequest(ShardSearchRequestProto.ShardSearchRequest shardSearchRequest) {
        this.shardSearchRequestProto = shardSearchRequest;
    }

    public ProtobufShardSearchRequest(ProtobufShardSearchRequest clone) {
        this.shardSearchRequestProto = clone.shardSearchRequestProto;
    }

    private byte[] convertToBytes(Object obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return bos.toByteArray();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        super.writeTo(out);
        out.write(this.shardSearchRequestProto.toByteArray());
    }

    // protected final void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
    //     shardId.writeTo(out);
    //     out.writeByte(searchType.id());
    //     if (!asKey) {
    //         out.writeVInt(numberOfShards);
    //     }
    //     out.writeOptionalWriteable(scroll);
    //     out.writeOptionalWriteable(source);
    //     if (out.getVersion().before(Version.V_2_0_0)) {
    //         // types not supported so send an empty array to previous versions
    //         out.writeStringArray(Strings.EMPTY_ARRAY);
    //     }
    //     aliasFilter.writeTo(out);
    //     out.writeFloat(indexBoost);
    //     if (asKey == false) {
    //         out.writeVLong(nowInMillis);
    //     }
    //     out.writeOptionalBoolean(requestCache);
    //     if (asKey == false && out.getVersion().onOrAfter(Version.V_2_0_0)) {
    //         out.writeVLong(inboundNetworkTime);
    //         out.writeVLong(outboundNetworkTime);
    //     }
    //     out.writeOptionalString(clusterAlias);
    //     out.writeBoolean(allowPartialSearchResults);
    //     if (asKey == false) {
    //         out.writeStringArray(indexRoutings);
    //         out.writeOptionalString(preference);
    //     }
    //     if (asKey == false) {
    //         out.writeBoolean(canReturnNullResponseIfMatchNoDocs);
    //         out.writeOptionalWriteable(bottomSortValues);
    //     }
    //     if (asKey == false) {
    //         out.writeOptionalWriteable(readerId);
    //         out.writeOptionalTimeValue(keepAlive);
    //     }
    // }

    @Override
    public String[] indices() {
        if (this.shardSearchRequestProto.getOriginalIndices() == null) {
            return null;
        }
        return this.shardSearchRequestProto.getOriginalIndices().getIndicesList().toArray(new String[0]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (this.shardSearchRequestProto.getOriginalIndices() == null) {
            return null;
        }
        IndicesOptions indicesOptions = new IndicesOptions(EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES, IndicesOptions.Option.FORBID_CLOSED_INDICES, IndicesOptions.Option.IGNORE_THROTTLED),
            EnumSet.of(IndicesOptions.WildcardStates.OPEN));
        return indicesOptions;
    }

    public ShardId shardId() {
        return new ShardId(this.shardSearchRequestProto.getShardId().getIndexName(), this.shardSearchRequestProto.getShardId().getIndexUUID(), this.shardSearchRequestProto.getShardId().getShardId());
    }

    public SearchSourceBuilder source() {
        ByteArrayInputStream in = new ByteArrayInputStream(this.shardSearchRequestProto.getSource().toByteArray());
        try (ObjectInputStream is = new ObjectInputStream(in)) {
            return (SearchSourceBuilder) is.readObject();
        } catch (ClassNotFoundException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public AliasFilter getAliasFilter() {
        ByteArrayInputStream in = new ByteArrayInputStream(this.shardSearchRequestProto.getAliasFilter().toByteArray());
        try (ObjectInputStream is = new ObjectInputStream(in)) {
            return (AliasFilter) is.readObject();
        } catch (ClassNotFoundException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public void setAliasFilter(AliasFilter aliasFilter) {
        this.shardSearchRequestProto.toBuilder().setAliasFilter(ByteString.copyFrom(convertToBytes(aliasFilter)));
    }

    public void source(SearchSourceBuilder source) {
        this.shardSearchRequestProto.toBuilder().setSource(ByteString.copyFrom(convertToBytes(source)));
    }

    public int numberOfShards() {
        return this.shardSearchRequestProto.getNumberOfShards();
    }

    public SearchType searchType() {
        return SearchType.QUERY_THEN_FETCH;
    }

    public float indexBoost() {
        return this.shardSearchRequestProto.getIndexBoost();
    }

    public long nowInMillis() {
        return this.shardSearchRequestProto.getNowInMillis();
    }

    public long getInboundNetworkTime() {
        return this.shardSearchRequestProto.getInboundNetworkTime();
    }

    public void setInboundNetworkTime(long newTime) {
        this.shardSearchRequestProto.toBuilder().setInboundNetworkTime(newTime);
    }

    public long getOutboundNetworkTime() {
        return this.shardSearchRequestProto.getOutboundNetworkTime();
    }

    public void setOutboundNetworkTime(long newTime) {
        this.shardSearchRequestProto.toBuilder().setOutboundNetworkTime(newTime);
    }

    public Boolean requestCache() {
        return this.shardSearchRequestProto.getRequestCache();
    }

    public boolean allowPartialSearchResults() {
        return this.shardSearchRequestProto.getAllowPartialSearchResults();
    }

    public Scroll scroll() {
        ByteArrayInputStream in = new ByteArrayInputStream(this.shardSearchRequestProto.getScroll().toByteArray());
        try (ObjectInputStream is = new ObjectInputStream(in)) {
            return (Scroll) is.readObject();
        } catch (ClassNotFoundException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public String[] indexRoutings() {
        return this.shardSearchRequestProto.getIndexRoutingsList().toArray(new String[0]);
    }

    public String preference() {
        return this.shardSearchRequestProto.getPreference();
    }

    // /**
    //  * Sets the bottom sort values that can be used by the searcher to filter documents
    //  * that are after it. This value is computed by coordinating nodes that throttles the
    //  * query phase. After a partial merge of successful shards the sort values of the
    //  * bottom top document are passed as an hint on subsequent shard requests.
    //  */
    // public void setBottomSortValues(SearchSortValuesAndFormats values) {
    //     this.bottomSortValues = values;
    // }

    // public SearchSortValuesAndFormats getBottomSortValues() {
    //     return bottomSortValues;
    // }

    /**
     * Returns true if the caller can handle null response {@link QuerySearchResult#nullInstance()}.
     * Defaults to false since the coordinator node needs at least one shard response to build the global
     * response.
     */
    public boolean canReturnNullResponseIfMatchNoDocs() {
        return this.shardSearchRequestProto.getCanReturnNullResponseIfMatchNoDocs();
    }

    public void canReturnNullResponseIfMatchNoDocs(boolean value) {
        this.shardSearchRequestProto.toBuilder().setCanReturnNullResponseIfMatchNoDocs(value);
    }

    private static final ThreadLocal<BytesStreamOutput> scratch = ThreadLocal.withInitial(BytesStreamOutput::new);

    /**
     * Returns a non-null value if this request should execute using a specific point-in-time reader;
     * otherwise, using the most up to date point-in-time reader.
     */
    public ShardSearchContextId readerId() {
        System.out.println("Getting readerId");
        if (this.shardSearchRequestProto.hasReaderId() == false) {
            System.out.println("Returning null since the readerId is null");
            return null;
        }
        return new ShardSearchContextId(this.shardSearchRequestProto.getReaderId().getSessionId(), this.shardSearchRequestProto.getReaderId().getId());
    }

    /**
     * Returns a non-null to specify the time to live of the point-in-time reader that is used to execute this request.
     */
    public TimeValue keepAlive() {
        if (!this.shardSearchRequestProto.hasTimeValue()) {
            return null;
        }
        return TimeValue.parseTimeValue(this.shardSearchRequestProto.getTimeValue(), null, "keep_alive");
    }

    public String getClusterAlias() {
        return this.shardSearchRequestProto.getClusterAlias();
    }

    @Override
    public ProtobufTask createProtobufTask(long id, String type, String action, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        return new ProtobufSearchShardTask(id, type, action, getDescription(), parentTaskId, headers, this::getMetadataSupplier);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardId[" + shardId() + "]";
    }

    public String getMetadataSupplier() {
        StringBuilder sb = new StringBuilder();
        if (this.shardSearchRequestProto.getSource() != null) {
            ByteArrayInputStream in = new ByteArrayInputStream(this.shardSearchRequestProto.getSource().toByteArray());
            try (ObjectInputStream is = new ObjectInputStream(in)) {
                SearchSourceBuilder source = (SearchSourceBuilder) is.readObject();
                sb.append("source[").append(source.toString(FORMAT_PARAMS)).append("]");
            } catch (ClassNotFoundException | IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            sb.append("source[]");
        }
        return sb.toString();
    }

    public Rewriteable<Rewriteable> getRewriteable() {
        return new RequestRewritable(this);
    }

    static class RequestRewritable implements Rewriteable<Rewriteable> {

        final ProtobufShardSearchRequest request;

        RequestRewritable(ProtobufShardSearchRequest request) {
            this.request = request;
        }

        @Override
        public Rewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            // System.out.println("Rewriting protobuf request source");
            // SearchSourceBuilder newSource = request.source() == null ? null : Rewriteable.rewrite(request.source(), ctx);
            // System.out.println("Rewriting protobuf request source done");
            // System.out.println("Rewriting protobuf request alias filter");
            // AliasFilter newAliasFilter = Rewriteable.rewrite(request.getAliasFilter(), ctx);
            // System.out.println("Rewriting protobuf request alias filter done");

            SearchSourceBuilder newSource = request.source();
            AliasFilter newAliasFilter = request.getAliasFilter();

            QueryShardContext shardContext = ctx.convertToShardContext();

            FieldSortBuilder primarySort = FieldSortBuilder.getPrimaryFieldSortOrNull(newSource);
            if (shardContext != null
                && primarySort != null
                // && primarySort.isBottomSortShardDisjoint(shardContext, request.getBottomSortValues())
                ) {
                assert newSource != null : "source should contain a primary sort field";
                newSource = newSource.shallowCopy();
                int trackTotalHitsUpTo = ProtobufSearchRequest.resolveTrackTotalHitsUpTo(request.scroll(), request.source());
                if (trackTotalHitsUpTo == TRACK_TOTAL_HITS_DISABLED && newSource.suggest() == null && newSource.aggregations() == null) {
                    newSource.query(new MatchNoneQueryBuilder());
                } else {
                    newSource.size(0);
                }
                request.source(newSource);
                // request.setBottomSortValues(null);
            }

            if (newSource == request.source() && newAliasFilter == request.getAliasFilter()) {
                return this;
            } else {
                request.source(newSource);
                request.setAliasFilter(newAliasFilter);
                return new RequestRewritable(request);
            }
        }
    }

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p>
     * The list of filtering aliases should be obtained by calling Metadata.filteringAliases.
     * Returns {@code null} if no filtering is required.</p>
     */
    public static QueryBuilder parseAliasFilter(
        CheckedFunction<BytesReference, QueryBuilder, IOException> filterParser,
        IndexMetadata metadata,
        String... aliasNames
    ) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        Index index = metadata.getIndex();
        final Map<String, AliasMetadata> aliases = metadata.getAliases();
        Function<AliasMetadata, QueryBuilder> parserFunction = (alias) -> {
            if (alias.filter() == null) {
                return null;
            }
            try {
                return filterParser.apply(alias.filter().uncompressed());
            } catch (IOException ex) {
                throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
            }
        };
        if (aliasNames.length == 1) {
            AliasMetadata alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parserFunction.apply(alias);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BoolQueryBuilder combined = new BoolQueryBuilder();
            for (String aliasName : aliasNames) {
                AliasMetadata alias = aliases.get(aliasName);
                if (alias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
                }
                QueryBuilder parsedFilter = parserFunction.apply(alias);
                if (parsedFilter != null) {
                    combined.should(parsedFilter);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return combined;
        }
    }

    public ShardSearchRequestProto.ShardSearchRequest request() {
        return this.shardSearchRequestProto;
    }
    
}
