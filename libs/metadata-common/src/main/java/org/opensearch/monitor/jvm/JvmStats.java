/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.monitor.jvm;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Holds JVM statistics
 *
 * @opensearch.internal
 */
public class JvmStats implements Writeable, ToXContentFragment {

    private static final RuntimeMXBean runtimeMXBean;
    private static final MemoryMXBean memoryMXBean;
    private static final ThreadMXBean threadMXBean;
    private static final ClassLoadingMXBean classLoadingMXBean;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    }

    public static JvmStats jvmStats() {
        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        long heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        long heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();
        memUsage = memoryMXBean.getNonHeapMemoryUsage();
        long nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        List<MemoryPool> pools = new ArrayList<>();
        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
            try {
                MemoryUsage usage = memoryPoolMXBean.getUsage();
                MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
                MemoryUsage collectionUsage = memoryPoolMXBean.getCollectionUsage();
                // For some pools, the collection usage may not be supported and the method returns null.
                if (collectionUsage == null) {
                    collectionUsage = new MemoryUsage(0, 0, 0, 0);
                }
                String name = GcNames.getByMemoryPoolName(memoryPoolMXBean.getName(), null);
                if (name == null) { // if we can't resolve it, its not interesting.... (Per Gen, Code Cache)
                    continue;
                }
                pools.add(
                    new MemoryPool(
                        name,
                        usage.getUsed() < 0 ? 0 : usage.getUsed(),
                        usage.getMax() < 0 ? 0 : usage.getMax(),
                        peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                        peakUsage.getMax() < 0 ? 0 : peakUsage.getMax(),
                        new MemoryPoolGcStats(
                            collectionUsage.getUsed() < 0 ? 0 : collectionUsage.getUsed(),
                            collectionUsage.getMax() < 0 ? 0 : collectionUsage.getMax()
                        )
                    )
                );

            } catch (final Exception ignored) {

            }
        }
        Mem mem = new Mem(heapCommitted, heapUsed, heapMax, nonHeapCommitted, nonHeapUsed, Collections.unmodifiableList(pools));
        Threads threads = new Threads(threadMXBean.getThreadCount(), threadMXBean.getPeakThreadCount());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        GarbageCollector[] collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            collectors[i] = new GarbageCollector(
                GcNames.getByGcName(gcMxBean.getName(), gcMxBean.getName()),
                gcMxBean.getCollectionCount(),
                gcMxBean.getCollectionTime()
            );
        }
        GarbageCollectors garbageCollectors = new GarbageCollectors(collectors);
        List<BufferPool> bufferPoolsList = Collections.emptyList();
        try {
            List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            bufferPoolsList = new ArrayList<>(bufferPools.size());
            for (BufferPoolMXBean bufferPool : bufferPools) {
                bufferPoolsList.add(
                    new BufferPool(bufferPool.getName(), bufferPool.getCount(), bufferPool.getTotalCapacity(), bufferPool.getMemoryUsed())
                );
            }
        } catch (Exception e) {
            // buffer pools are not available
        }

        Classes classes = new Classes(
            classLoadingMXBean.getLoadedClassCount(),
            classLoadingMXBean.getTotalLoadedClassCount(),
            classLoadingMXBean.getUnloadedClassCount()
        );

        return new JvmStats(
            System.currentTimeMillis(),
            runtimeMXBean.getUptime(),
            mem,
            threads,
            garbageCollectors,
            bufferPoolsList,
            classes
        );
    }

    private final long timestamp;
    private final long uptime;
    private final Mem mem;
    private final Threads threads;
    private final GarbageCollectors gc;
    private final List<BufferPool> bufferPools;
    private final Classes classes;

    public JvmStats(
        long timestamp,
        long uptime,
        Mem mem,
        Threads threads,
        GarbageCollectors gc,
        List<BufferPool> bufferPools,
        Classes classes
    ) {
        this.timestamp = timestamp;
        this.uptime = uptime;
        this.mem = mem;
        this.threads = threads;
        this.gc = gc;
        this.bufferPools = bufferPools;
        this.classes = classes;
    }

    public JvmStats(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        uptime = in.readVLong();
        mem = new Mem(in);
        threads = new Threads(in);
        gc = new GarbageCollectors(in);
        bufferPools = in.readList(BufferPool::new);
        classes = new Classes(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVLong(uptime);
        mem.writeTo(out);
        threads.writeTo(out);
        gc.writeTo(out);
        out.writeList(bufferPools);
        classes.writeTo(out);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TimeValue getUptime() {
        return new TimeValue(uptime);
    }

    public Mem getMem() {
        return this.mem;
    }

    public Threads getThreads() {
        return threads;
    }

    public GarbageCollectors getGc() {
        return gc;
    }

    public List<BufferPool> getBufferPools() {
        return bufferPools;
    }

    public Classes getClasses() {
        return classes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.humanReadableField(Fields.UPTIME_IN_MILLIS, Fields.UPTIME, new TimeValue(uptime));

        builder.startObject(Fields.MEM);

        builder.humanReadableField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, new ByteSizeValue(mem.heapUsed));
        if (mem.getHeapUsedPercent() >= 0) {
            builder.field(Fields.HEAP_USED_PERCENT, mem.getHeapUsedPercent());
        }
        builder.humanReadableField(Fields.HEAP_COMMITTED_IN_BYTES, Fields.HEAP_COMMITTED, new ByteSizeValue(mem.heapCommitted));
        builder.humanReadableField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, new ByteSizeValue(mem.heapMax));
        builder.humanReadableField(Fields.NON_HEAP_USED_IN_BYTES, Fields.NON_HEAP_USED, new ByteSizeValue(mem.nonHeapUsed));
        builder.humanReadableField(Fields.NON_HEAP_COMMITTED_IN_BYTES, Fields.NON_HEAP_COMMITTED, new ByteSizeValue(mem.nonHeapCommitted));

        builder.startObject(Fields.POOLS);
        for (MemoryPool pool : mem) {
            builder.startObject(pool.getName());
            builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, new ByteSizeValue(pool.used));
            builder.humanReadableField(Fields.MAX_IN_BYTES, Fields.MAX, new ByteSizeValue(pool.max));

            builder.humanReadableField(Fields.PEAK_USED_IN_BYTES, Fields.PEAK_USED, new ByteSizeValue(pool.peakUsed));
            builder.humanReadableField(Fields.PEAK_MAX_IN_BYTES, Fields.PEAK_MAX, new ByteSizeValue(pool.peakMax));

            builder.startObject(Fields.LAST_GC_STATS);
            builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, new ByteSizeValue(pool.getLastGcStats().used));
            builder.humanReadableField(Fields.MAX_IN_BYTES, Fields.MAX, new ByteSizeValue(pool.getLastGcStats().max));
            builder.field(Fields.USAGE_PERCENT, pool.getLastGcStats().getUsagePercent());
            builder.endObject();

            builder.endObject();
        }
        builder.endObject();

        builder.endObject();

        builder.startObject(Fields.THREADS);
        builder.field(Fields.COUNT, threads.getCount());
        builder.field(Fields.PEAK_COUNT, threads.getPeakCount());
        builder.endObject();

        builder.startObject(Fields.GC);

        builder.startObject(Fields.COLLECTORS);
        for (GarbageCollector collector : gc) {
            builder.startObject(collector.getName());
            builder.field(Fields.COLLECTION_COUNT, collector.getCollectionCount());
            builder.humanReadableField(Fields.COLLECTION_TIME_IN_MILLIS, Fields.COLLECTION_TIME, new TimeValue(collector.collectionTime));
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();

        if (bufferPools != null) {
            builder.startObject(Fields.BUFFER_POOLS);
            for (BufferPool bufferPool : bufferPools) {
                builder.startObject(bufferPool.getName());
                builder.field(Fields.COUNT, bufferPool.getCount());
                builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, new ByteSizeValue(bufferPool.used));
                builder.humanReadableField(
                    Fields.TOTAL_CAPACITY_IN_BYTES,
                    Fields.TOTAL_CAPACITY,
                    new ByteSizeValue(bufferPool.totalCapacity)
                );
                builder.endObject();
            }
            builder.endObject();
        }

        builder.startObject(Fields.CLASSES);
        builder.field(Fields.CURRENT_LOADED_COUNT, classes.getLoadedClassCount());
        builder.field(Fields.TOTAL_LOADED_COUNT, classes.getTotalLoadedClassCount());
        builder.field(Fields.TOTAL_UNLOADED_COUNT, classes.getUnloadedClassCount());
        builder.endObject();

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String JVM = "jvm";
        static final String TIMESTAMP = "timestamp";
        static final String UPTIME = "uptime";
        static final String UPTIME_IN_MILLIS = "uptime_in_millis";

        static final String MEM = "mem";
        static final String HEAP_USED = "heap_used";
        static final String HEAP_USED_IN_BYTES = "heap_used_in_bytes";
        static final String HEAP_USED_PERCENT = "heap_used_percent";
        static final String HEAP_MAX = "heap_max";
        static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        static final String HEAP_COMMITTED = "heap_committed";
        static final String HEAP_COMMITTED_IN_BYTES = "heap_committed_in_bytes";

        static final String NON_HEAP_USED = "non_heap_used";
        static final String NON_HEAP_USED_IN_BYTES = "non_heap_used_in_bytes";
        static final String NON_HEAP_COMMITTED = "non_heap_committed";
        static final String NON_HEAP_COMMITTED_IN_BYTES = "non_heap_committed_in_bytes";

        static final String POOLS = "pools";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String MAX = "max";
        static final String MAX_IN_BYTES = "max_in_bytes";
        static final String PEAK_USED = "peak_used";
        static final String PEAK_USED_IN_BYTES = "peak_used_in_bytes";
        static final String PEAK_MAX = "peak_max";
        static final String PEAK_MAX_IN_BYTES = "peak_max_in_bytes";
        static final String USAGE_PERCENT = "usage_percent";
        static final String LAST_GC_STATS = "last_gc_stats";

        static final String THREADS = "threads";
        static final String COUNT = "count";
        static final String PEAK_COUNT = "peak_count";

        static final String GC = "gc";
        static final String COLLECTORS = "collectors";
        static final String COLLECTION_COUNT = "collection_count";
        static final String COLLECTION_TIME = "collection_time";
        static final String COLLECTION_TIME_IN_MILLIS = "collection_time_in_millis";

        static final String BUFFER_POOLS = "buffer_pools";
        static final String TOTAL_CAPACITY = "total_capacity";
        static final String TOTAL_CAPACITY_IN_BYTES = "total_capacity_in_bytes";

        static final String CLASSES = "classes";
        static final String CURRENT_LOADED_COUNT = "current_loaded_count";
        static final String TOTAL_LOADED_COUNT = "total_loaded_count";
        static final String TOTAL_UNLOADED_COUNT = "total_unloaded_count";
    }

    /**
     * Garbage collector references.
     *
     * @opensearch.internal
     */
    public static class GarbageCollectors implements Writeable, Iterable<GarbageCollector> {

        private final GarbageCollector[] collectors;

        public GarbageCollectors(GarbageCollector[] collectors) {
            this.collectors = collectors;
        }

        public GarbageCollectors(StreamInput in) throws IOException {
            collectors = in.readArray(GarbageCollector::new, GarbageCollector[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(collectors);
        }

        public GarbageCollector[] getCollectors() {
            return this.collectors;
        }

        @Override
        public Iterator<GarbageCollector> iterator() {
            return Arrays.stream(collectors).iterator();
        }
    }

    /**
     * The garbage collector.
     *
     * @opensearch.internal
     */
    public static class GarbageCollector implements Writeable {

        private final String name;
        private final long collectionCount;
        private final long collectionTime;

        public GarbageCollector(String name, long collectionCount, long collectionTime) {
            this.name = name;
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        public GarbageCollector(StreamInput in) throws IOException {
            name = in.readString();
            collectionCount = in.readVLong();
            collectionTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(collectionCount);
            out.writeVLong(collectionTime);
        }

        public String getName() {
            return this.name;
        }

        public long getCollectionCount() {
            return this.collectionCount;
        }

        public TimeValue getCollectionTime() {
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Thread information.
     *
     * @opensearch.internal
     */
    public static class Threads implements Writeable {

        private final int count;
        private final int peakCount;

        public Threads(int count, int peakCount) {
            this.count = count;
            this.peakCount = peakCount;
        }

        public Threads(StreamInput in) throws IOException {
            count = in.readVInt();
            peakCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeVInt(peakCount);
        }

        public int getCount() {
            return count;
        }

        public int getPeakCount() {
            return peakCount;
        }
    }

    /**
     * Stores the memory usage after the Java virtual machine
     * most recently expended effort in recycling unused objects
     * in particular memory pool.
     *
     * @opensearch.internal
     */
    public static class MemoryPoolGcStats implements Writeable {

        private final long used;
        private final long max;

        public MemoryPoolGcStats(long used, long max) {
            this.used = used;
            this.max = max;
        }

        public MemoryPoolGcStats(StreamInput in) throws IOException {
            used = in.readVLong();
            max = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(used);
            out.writeVLong(max);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getMax() {
            return new ByteSizeValue(max);
        }

        public short getUsagePercent() {
            if (max == 0) {
                return -1;
            }
            return (short) (used * 100 / max);
        }
    }

    /**
     * A memory pool.
     *
     * @opensearch.internal
     */
    public static class MemoryPool implements Writeable {

        private final String name;
        private final long used;
        private final long max;
        private final long peakUsed;
        private final long peakMax;
        private final MemoryPoolGcStats lastGcStats;

        public MemoryPool(String name, long used, long max, long peakUsed, long peakMax, MemoryPoolGcStats lastGcStats) {
            this.name = name;
            this.used = used;
            this.max = max;
            this.peakUsed = peakUsed;
            this.peakMax = peakMax;
            this.lastGcStats = lastGcStats;
        }

        public MemoryPool(StreamInput in) throws IOException {
            name = in.readString();
            used = in.readVLong();
            max = in.readVLong();
            peakUsed = in.readVLong();
            peakMax = in.readVLong();
            lastGcStats = new MemoryPoolGcStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(used);
            out.writeVLong(max);
            out.writeVLong(peakUsed);
            out.writeVLong(peakMax);
            lastGcStats.writeTo(out);
        }

        public String getName() {
            return this.name;
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getMax() {
            return new ByteSizeValue(max);
        }

        public ByteSizeValue getPeakUsed() {
            return new ByteSizeValue(peakUsed);
        }

        public ByteSizeValue getPeakMax() {
            return new ByteSizeValue(peakMax);
        }

        /**
         * Returns the heap usage after last garbage collection cycle
         */
        public MemoryPoolGcStats getLastGcStats() {
            return lastGcStats;
        }
    }

    /**
     * Memory data.
     *
     * @opensearch.internal
     */
    public static class Mem implements Writeable, Iterable<MemoryPool> {

        private final long heapCommitted;
        private final long heapUsed;
        private final long heapMax;
        private final long nonHeapCommitted;
        private final long nonHeapUsed;
        private final List<MemoryPool> pools;

        public Mem(long heapCommitted, long heapUsed, long heapMax, long nonHeapCommitted, long nonHeapUsed, List<MemoryPool> pools) {
            this.heapCommitted = heapCommitted;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.nonHeapCommitted = nonHeapCommitted;
            this.nonHeapUsed = nonHeapUsed;
            this.pools = pools;
        }

        public Mem(StreamInput in) throws IOException {
            heapCommitted = in.readVLong();
            heapUsed = in.readVLong();
            nonHeapCommitted = in.readVLong();
            nonHeapUsed = in.readVLong();
            heapMax = in.readVLong();
            pools = in.readList(MemoryPool::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapCommitted);
            out.writeVLong(heapUsed);
            out.writeVLong(nonHeapCommitted);
            out.writeVLong(nonHeapUsed);
            out.writeVLong(heapMax);
            out.writeList(pools);
        }

        @Override
        public Iterator<MemoryPool> iterator() {
            return pools.iterator();
        }

        public ByteSizeValue getHeapCommitted() {
            return new ByteSizeValue(heapCommitted);
        }

        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * returns the maximum heap size. 0 bytes signals unknown.
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        /**
         * returns the heap usage in percent. -1 signals unknown.
         */
        public short getHeapUsedPercent() {
            if (heapMax == 0) {
                return -1;
            }
            return (short) (heapUsed * 100 / heapMax);
        }

        public ByteSizeValue getNonHeapCommitted() {
            return new ByteSizeValue(nonHeapCommitted);
        }

        public ByteSizeValue getNonHeapUsed() {
            return new ByteSizeValue(nonHeapUsed);
        }
    }

    /**
     * A buffer pool.
     *
     * @opensearch.internal
     */
    public static class BufferPool implements Writeable {

        private final String name;
        private final long count;
        private final long totalCapacity;
        private final long used;

        public BufferPool(String name, long count, long totalCapacity, long used) {
            this.name = name;
            this.count = count;
            this.totalCapacity = totalCapacity;
            this.used = used;
        }

        public BufferPool(StreamInput in) throws IOException {
            name = in.readString();
            count = in.readLong();
            totalCapacity = in.readLong();
            used = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(count);
            out.writeLong(totalCapacity);
            out.writeLong(used);
        }

        public String getName() {
            return this.name;
        }

        public long getCount() {
            return this.count;
        }

        public ByteSizeValue getTotalCapacity() {
            return new ByteSizeValue(totalCapacity);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }
    }

    /**
     * Class information.
     *
     * @opensearch.internal
     */
    public static class Classes implements Writeable {

        private final long loadedClassCount;
        private final long totalLoadedClassCount;
        private final long unloadedClassCount;

        public Classes(long loadedClassCount, long totalLoadedClassCount, long unloadedClassCount) {
            this.loadedClassCount = loadedClassCount;
            this.totalLoadedClassCount = totalLoadedClassCount;
            this.unloadedClassCount = unloadedClassCount;
        }

        public Classes(StreamInput in) throws IOException {
            loadedClassCount = in.readLong();
            totalLoadedClassCount = in.readLong();
            unloadedClassCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(loadedClassCount);
            out.writeLong(totalLoadedClassCount);
            out.writeLong(unloadedClassCount);
        }

        public long getLoadedClassCount() {
            return loadedClassCount;
        }

        public long getTotalLoadedClassCount() {
            return totalLoadedClassCount;
        }

        public long getUnloadedClassCount() {
            return unloadedClassCount;
        }
    }
}
