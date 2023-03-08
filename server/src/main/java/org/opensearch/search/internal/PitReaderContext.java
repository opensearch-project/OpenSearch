/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;

import java.util.Collections;
import java.util.List;

/**
 * PIT reader context containing PIT specific information such as pit id, create time etc.
 */
public class PitReaderContext extends ReaderContext {

    // Storing the encoded PIT ID as part of PIT reader context for use cases such as list pit API
    private final SetOnce<String> pitId = new SetOnce<>();
    // Creation time of PIT contexts which helps users to differentiate between multiple PIT reader contexts
    private final SetOnce<Long> creationTime = new SetOnce<>();
    /**
     * Shard routing at the time of creation of PIT Reader Context
     */
    private final ShardRouting shardRouting;

    /**
     * Encapsulates segments constituting the shard at the time of creation of PIT Reader Context.
     */
    private final List<Segment> segments;

    public PitReaderContext(
        ShardSearchContextId id,
        IndexService indexService,
        IndexShard indexShard,
        Engine.SearcherSupplier searcherSupplier,
        long keepAliveInMillis,
        boolean singleSession
    ) {
        super(id, indexService, indexShard, searcherSupplier, keepAliveInMillis, singleSession);
        shardRouting = indexShard.routingEntry();
        segments = indexShard.segments(true);
    }

    public String getPitId() {
        return this.pitId.get();
    }

    public void setPitId(final String pitId) {
        this.pitId.set(pitId);
    }

    /**
     * Returns a releasable to indicate that the caller has stopped using this reader.
     * The pit id can be updated and time to live of the reader usage can be extended using the provided
     * <code>keepAliveInMillis</code>.
     */
    public Releasable updatePitIdAndKeepAlive(long keepAliveInMillis, String pitId, long createTime) {
        getRefCounted().incRef();
        tryUpdateKeepAlive(keepAliveInMillis);
        setPitId(pitId);
        setCreationTime(createTime);
        return Releasables.releaseOnce(() -> {
            updateLastAccessTime();
            getRefCounted().decRef();
        });
    }

    public long getCreationTime() {
        return this.creationTime.get() == null ? 0 : this.creationTime.get();
    }

    public void setCreationTime(final long creationTime) {
        this.creationTime.set(creationTime);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public List<Segment> getSegments() {
        return Collections.unmodifiableList(segments);
    }
}
