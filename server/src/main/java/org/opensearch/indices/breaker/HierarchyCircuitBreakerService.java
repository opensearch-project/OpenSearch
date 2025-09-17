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

package org.opensearch.indices.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.Booleans;
import org.opensearch.common.breaker.ChildMemoryCircuitBreaker;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.monitor.jvm.GcNames;
import org.opensearch.monitor.jvm.JvmInfo;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.indices.breaker.BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.opensearch.indices.breaker.BreakerSettings.CIRCUIT_BREAKER_OVERHEAD_SETTING;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 *
 * @opensearch.internal
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {
    private static final Logger logger = LogManager.getLogger(HierarchyCircuitBreakerService.class);

    private static final String CHILD_LOGGER_PREFIX = "org.opensearch.indices.breaker.";

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private final Map<String, CircuitBreaker> breakers;

    public static final Setting<Boolean> USE_REAL_MEMORY_USAGE_SETTING = Setting.boolSetting(
        "indices.breaker.total.use_real_memory",
        true,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.total.limit",
        settings -> {
            if (USE_REAL_MEMORY_USAGE_SETTING.get(settings)) {
                return "95%";
            } else {
                return "70%";
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * If an incoming request would cause the field data cache to exceed this size, the request is cancelled.
     */
    public static final Setting<ByteSizeValue> FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.fielddata.limit",
        "40%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "indices.breaker.fielddata.overhead",
        1.03d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "indices.breaker.fielddata.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.request.limit",
        "60%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "indices.breaker.request.overhead",
        1.0d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> REQUEST_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "indices.breaker.request.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "network.breaker.inflight_requests.limit",
        "100%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "network.breaker.inflight_requests.overhead",
        2.0d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "network.breaker.inflight_requests.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    private final boolean trackRealMemoryUsage;
    private volatile BreakerSettings parentSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    private final OverLimitStrategy overLimitStrategy;

    public HierarchyCircuitBreakerService(Settings settings, List<BreakerSettings> customBreakers, ClusterSettings clusterSettings) {
        this(settings, customBreakers, clusterSettings, HierarchyCircuitBreakerService::createOverLimitStrategy);
    }

    HierarchyCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> customBreakers,
        ClusterSettings clusterSettings,
        Function<Boolean, OverLimitStrategy> overLimitStrategyFactory
    ) {
        super();
        HashMap<String, CircuitBreaker> childCircuitBreakers = new HashMap<>();
        childCircuitBreakers.put(
            CircuitBreaker.FIELDDATA,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.FIELDDATA,
                    FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.PERMANENT
                )
            )
        );
        childCircuitBreakers.put(
            CircuitBreaker.IN_FLIGHT_REQUESTS,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.IN_FLIGHT_REQUESTS,
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.TRANSIENT
                )
            )
        );
        childCircuitBreakers.put(
            CircuitBreaker.REQUEST,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.REQUEST,
                    REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.TRANSIENT
                )
            )
        );
        for (BreakerSettings breakerSettings : customBreakers) {
            if (childCircuitBreakers.containsKey(breakerSettings.getName())) {
                throw new IllegalArgumentException(
                    "More than one circuit breaker with the name ["
                        + breakerSettings.getName()
                        + "] exists. Circuit breaker names must be unique"
                );
            }
            childCircuitBreakers.put(breakerSettings.getName(), validateAndCreateBreaker(breakerSettings));
        }
        this.breakers = Collections.unmodifiableMap(childCircuitBreakers);
        this.parentSettings = new BreakerSettings(
            CircuitBreaker.PARENT,
            TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
            1.0,
            CircuitBreaker.Type.PARENT,
            null
        );
        logger.trace(() -> new ParameterizedMessage("parent circuit breaker with settings {}", this.parentSettings));

        this.trackRealMemoryUsage = USE_REAL_MEMORY_USAGE_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(
            TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
            this::setTotalCircuitBreakerLimit,
            this::validateTotalCircuitBreakerLimit
        );
        clusterSettings.addSettingsUpdateConsumer(
            FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
            FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.FIELDDATA, limit, overhead)
        );
        clusterSettings.addSettingsUpdateConsumer(
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS, limit, overhead)
        );
        clusterSettings.addSettingsUpdateConsumer(
            REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
            REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.REQUEST, limit, overhead)
        );
        clusterSettings.addAffixUpdateConsumer(
            CIRCUIT_BREAKER_LIMIT_SETTING,
            CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (name, updatedValues) -> updateCircuitBreakerSettings(name, updatedValues.v1(), updatedValues.v2()),
            (s, t) -> {}
        );

        this.overLimitStrategy = overLimitStrategyFactory.apply(this.trackRealMemoryUsage);
    }

    private void updateCircuitBreakerSettings(String name, ByteSizeValue newLimit, Double newOverhead) {
        CircuitBreaker childBreaker = breakers.get(name);
        if (childBreaker != null) {
            childBreaker.setLimitAndOverhead(newLimit.getBytes(), newOverhead);
            logger.info("Updated limit {} and overhead {} for {}", newLimit.getStringRep(), newOverhead, name);
        }
    }

    private void validateTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(
            CircuitBreaker.PARENT,
            byteSizeValue.getBytes(),
            1.0,
            CircuitBreaker.Type.PARENT,
            null
        );
        validateSettings(new BreakerSettings[] { newParentSettings });
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0, CircuitBreaker.Type.PARENT, null);
    }

    /**
     * Validate that child settings are valid
     */
    public static void validateSettings(BreakerSettings[] childrenSettings) throws IllegalStateException {
        for (BreakerSettings childSettings : childrenSettings) {
            // If the child is disabled, ignore it
            if (childSettings.getLimit() == -1) {
                continue;
            }

            if (childSettings.getOverhead() < 0) {
                throw new IllegalStateException("Child breaker overhead " + childSettings + " must be non-negative");
            }
        }
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return this.breakers.get(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        List<CircuitBreakerStats> allStats = new ArrayList<>(this.breakers.size());
        // Gather the "estimated" count for the parent breaker by adding the
        // estimations for each individual breaker
        for (CircuitBreaker breaker : this.breakers.values()) {
            allStats.add(stats(breaker.getName()));
        }
        // Manually add the parent breaker settings since they aren't part of the breaker map
        allStats.add(
            new CircuitBreakerStats(CircuitBreaker.PARENT, parentSettings.getLimit(), memoryUsed(0L).totalUsage, 1.0, parentTripCount.get())
        );
        return new AllCircuitBreakerStats(allStats.toArray(new CircuitBreakerStats[0]));
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        CircuitBreaker breaker = this.breakers.get(name);
        return new CircuitBreakerStats(
            breaker.getName(),
            breaker.getLimit(),
            breaker.getUsed(),
            breaker.getOverhead(),
            breaker.getTrippedCount()
        );
    }

    /**
     * Tracks memory usage
     *
     * @opensearch.internal
     */
    static class MemoryUsage {
        final long baseUsage;
        final long totalUsage;
        final long transientChildUsage;
        final long permanentChildUsage;

        MemoryUsage(final long baseUsage, final long totalUsage, final long transientChildUsage, final long permanentChildUsage) {
            this.baseUsage = baseUsage;
            this.totalUsage = totalUsage;
            this.transientChildUsage = transientChildUsage;
            this.permanentChildUsage = permanentChildUsage;
        }
    }

    private MemoryUsage memoryUsed(long newBytesReserved) {
        long transientUsage = 0;
        long permanentUsage = 0;

        for (CircuitBreaker breaker : this.breakers.values()) {
            long breakerUsed = (long) (breaker.getUsed() * breaker.getOverhead());
            if (breaker.getDurability() == CircuitBreaker.Durability.TRANSIENT) {
                transientUsage += breakerUsed;
            } else if (breaker.getDurability() == CircuitBreaker.Durability.PERMANENT) {
                permanentUsage += breakerUsed;
            }
        }
        if (this.trackRealMemoryUsage) {
            final long current = currentMemoryUsage();
            return new MemoryUsage(current, current + newBytesReserved, transientUsage, permanentUsage);
        } else {
            long parentEstimated = transientUsage + permanentUsage;
            return new MemoryUsage(parentEstimated, parentEstimated, transientUsage, permanentUsage);
        }
    }

    // package private to allow overriding it in tests
    long currentMemoryUsage() {
        return realMemoryUsage();
    }

    static long realMemoryUsage() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException ex) {
            // This exception can happen (rarely) due to a race condition in the JVM when determining usage of memory pools. We do not want
            // to fail requests because of this and thus return zero memory usage in this case. While we could also return the most
            // recently determined memory usage, we would overestimate memory usage immediately after a garbage collection event.
            assert ex.getMessage().matches("committed = \\d+ should be < max = \\d+");
            logger.info("Cannot determine current memory usage due to JDK-8207200.", ex);
            return 0;
        }
    }

    public long getParentLimit() {
        return this.parentSettings.getLimit();
    }

    /**
     * Checks whether the parent breaker has been tripped
     */
    public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
        final MemoryUsage memoryUsed = memoryUsed(newBytesReserved);
        long parentLimit = this.parentSettings.getLimit();
        if (memoryUsed.totalUsage > parentLimit && overLimitStrategy.overLimit(memoryUsed).totalUsage > parentLimit) {
            this.parentTripCount.incrementAndGet();
            final StringBuilder message = new StringBuilder(
                "[parent] Data too large, data for ["
                    + label
                    + "]"
                    + " would be ["
                    + memoryUsed.totalUsage
                    + "/"
                    + new ByteSizeValue(memoryUsed.totalUsage)
                    + "]"
                    + ", which is larger than the limit of ["
                    + parentLimit
                    + "/"
                    + new ByteSizeValue(parentLimit)
                    + "]"
            );
            if (this.trackRealMemoryUsage) {
                final long realUsage = memoryUsed.baseUsage;
                message.append(", real usage: [");
                message.append(realUsage);
                message.append("/");
                message.append(new ByteSizeValue(realUsage));
                message.append("], new bytes reserved: [");
                message.append(newBytesReserved);
                message.append("/");
                message.append(new ByteSizeValue(newBytesReserved));
                message.append("]");
            }
            message.append(", usages [");
            message.append(this.breakers.entrySet().stream().map(e -> {
                final CircuitBreaker breaker = e.getValue();
                final long breakerUsed = (long) (breaker.getUsed() * breaker.getOverhead());
                return e.getKey() + "=" + breakerUsed + "/" + new ByteSizeValue(breakerUsed);
            }).collect(Collectors.joining(", ")));
            message.append("]");
            // derive durability of a tripped parent breaker depending on whether the majority of memory tracked by
            // child circuit breakers is categorized as transient or permanent.
            CircuitBreaker.Durability durability = memoryUsed.transientChildUsage >= memoryUsed.permanentChildUsage
                ? CircuitBreaker.Durability.TRANSIENT
                : CircuitBreaker.Durability.PERMANENT;
            logger.debug(() -> new ParameterizedMessage("{}", message.toString()));
            throw new CircuitBreakingException(message.toString(), memoryUsed.totalUsage, parentLimit, durability);
        }
    }

    private CircuitBreaker validateAndCreateBreaker(BreakerSettings breakerSettings) {
        // Validate the settings
        validateSettings(new BreakerSettings[] { breakerSettings });
        return breakerSettings.getType() == CircuitBreaker.Type.NOOP
            ? new NoopCircuitBreaker(breakerSettings.getName())
            : new ChildMemoryCircuitBreaker(
                breakerSettings,
                LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                this,
                breakerSettings.getName()
            );
    }

    static OverLimitStrategy createOverLimitStrategy(boolean trackRealMemoryUsage) {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        if (trackRealMemoryUsage && jvmInfo.useG1GC().equals("true")
        // messing with GC is "dangerous" so we apply an escape hatch. Not intended to be used.
            && Booleans.parseBoolean(System.getProperty("opensearch.real_memory_circuit_breaker.g1_over_limit_strategy.enabled"), true)) {
            TimeValue lockTimeout = TimeValue.timeValueMillis(
                Integer.parseInt(System.getProperty("opensearch.real_memory_circuit_breaker.g1_over_limit_strategy.lock_timeout_ms", "500"))
            );
            // hardcode interval, do not want any tuning of it outside code changes.
            return new G1OverLimitStrategy(
                jvmInfo,
                HierarchyCircuitBreakerService::realMemoryUsage,
                createYoungGcCountSupplier(),
                System::currentTimeMillis,
                5000,
                lockTimeout
            );
        } else {
            return memoryUsed -> memoryUsed;
        }
    }

    static LongSupplier createYoungGcCountSupplier() {
        List<GarbageCollectorMXBean> youngBeans = ManagementFactory.getGarbageCollectorMXBeans()
            .stream()
            .filter(mxBean -> GcNames.getByGcName(mxBean.getName(), mxBean.getName()).equals(GcNames.YOUNG))
            .collect(Collectors.toList());
        assert youngBeans.size() == 1;
        assert youngBeans.get(0).getCollectionCount() != -1 : "G1 must support getting collection count";

        if (youngBeans.size() == 1) {
            return youngBeans.get(0)::getCollectionCount;
        } else {
            logger.warn("Unable to find young generation collector, G1 over limit strategy might be impacted [{}]", youngBeans);
            return () -> -1;
        }
    }

    interface OverLimitStrategy {
        MemoryUsage overLimit(MemoryUsage memoryUsed);
    }

    /**
     * Kicks in G1GC if heap gets too high
     *
     * @opensearch.internal
     */
    static class G1OverLimitStrategy implements OverLimitStrategy {
        private final long g1RegionSize;
        private final LongSupplier currentMemoryUsageSupplier;
        private final LongSupplier gcCountSupplier;
        private final LongSupplier timeSupplier;
        private final TimeValue lockTimeout;
        private final long maxHeap;

        private long lastCheckTime = Long.MIN_VALUE;
        private final long minimumInterval;

        private long blackHole;
        private final ReleasableLock lock = new ReleasableLock(new ReentrantLock());

        G1OverLimitStrategy(
            JvmInfo jvmInfo,
            LongSupplier currentMemoryUsageSupplier,
            LongSupplier gcCountSupplier,
            LongSupplier timeSupplier,
            long minimumInterval,
            TimeValue lockTimeout
        ) {
            this.lockTimeout = lockTimeout;
            assert minimumInterval > 0;
            this.currentMemoryUsageSupplier = currentMemoryUsageSupplier;
            this.gcCountSupplier = gcCountSupplier;
            this.timeSupplier = timeSupplier;
            this.minimumInterval = minimumInterval;
            this.maxHeap = jvmInfo.getMem().getHeapMax().getBytes();
            long g1RegionSize = jvmInfo.getG1RegionSize();
            if (g1RegionSize <= 0) {
                this.g1RegionSize = fallbackRegionSize(jvmInfo);
            } else {
                this.g1RegionSize = g1RegionSize;
            }
        }

        static long fallbackRegionSize(JvmInfo jvmInfo) {
            // mimick JDK calculation based on JDK 14 source:
            // https://hg.openjdk.java.net/jdk/jdk14/file/6c954123ee8d/src/hotspot/share/gc/g1/heapRegion.cpp#l65
            // notice that newer JDKs will have a slight variant only considering max-heap:
            // https://hg.openjdk.java.net/jdk/jdk/file/e7d0ec2d06e8/src/hotspot/share/gc/g1/heapRegion.cpp#l67
            // based on this JDK "bug":
            // https://bugs.openjdk.java.net/browse/JDK-8241670
            // JDK-17 updates:
            // https://github.com/openjdk/jdk17u/blob/master/src/hotspot/share/gc/g1/heapRegionBounds.hpp
            // https://github.com/openjdk/jdk17u/blob/master/src/hotspot/share/gc/g1/heapRegion.cpp#L67
            long regionSizeUnrounded = Math.min(
                Math.max(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 2048, ByteSizeUnit.MB.toBytes(1)),
                ByteSizeUnit.MB.toBytes(32)
            );

            long regionSize = Long.highestOneBit(regionSizeUnrounded);
            if (regionSize != regionSizeUnrounded) {
                regionSize <<= 1; /* next power of 2 */
            }

            if (regionSize < ByteSizeUnit.MB.toBytes(1)) {
                regionSize = ByteSizeUnit.MB.toBytes(1);
            } else if (regionSize > ByteSizeUnit.MB.toBytes(32)) {
                regionSize = ByteSizeUnit.MB.toBytes(32);
            }
            return regionSize;
        }

        @Override
        public MemoryUsage overLimit(MemoryUsage memoryUsed) {
            boolean leader = false;
            int allocationIndex = 0;
            long allocationDuration = 0;
            try (ReleasableLock locked = lock.tryAcquire(lockTimeout)) {
                if (locked != null) {
                    long begin = timeSupplier.getAsLong();
                    leader = begin >= lastCheckTime + minimumInterval;
                    overLimitTriggered(leader);
                    if (leader) {
                        long initialCollectionCount = gcCountSupplier.getAsLong();
                        logger.info("attempting to trigger G1GC due to high heap usage [{}]", memoryUsed.baseUsage);
                        long localBlackHole = 0;
                        // number of allocations, corresponding to (approximately) number of free regions + 1
                        int allocationCount = Math.toIntExact((maxHeap - memoryUsed.baseUsage) / g1RegionSize + 1);
                        // allocations of half-region size becomes single humongous alloc, thus taking up a full region.
                        int allocationSize = (int) (g1RegionSize >> 1);
                        long maxUsageObserved = memoryUsed.baseUsage;
                        for (; allocationIndex < allocationCount; ++allocationIndex) {
                            long current = currentMemoryUsageSupplier.getAsLong();
                            if (current >= maxUsageObserved) {
                                maxUsageObserved = current;
                            } else {
                                // we observed a memory drop, so some GC must have occurred
                                break;
                            }
                            if (initialCollectionCount != gcCountSupplier.getAsLong()) {
                                break;
                            }
                            localBlackHole += new byte[allocationSize].hashCode();
                        }

                        blackHole += localBlackHole;
                        logger.trace("black hole [{}]", blackHole);

                        long now = timeSupplier.getAsLong();
                        this.lastCheckTime = now;
                        allocationDuration = now - begin;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // fallthrough
            }

            final long current = currentMemoryUsageSupplier.getAsLong();
            if (current < memoryUsed.baseUsage) {
                if (leader) {
                    logger.info(
                        "GC did bring memory usage down, before [{}], after [{}], allocations [{}], duration [{}]",
                        memoryUsed.baseUsage,
                        current,
                        allocationIndex,
                        allocationDuration
                    );
                }
                return new MemoryUsage(
                    current,
                    memoryUsed.totalUsage - memoryUsed.baseUsage + current,
                    memoryUsed.transientChildUsage,
                    memoryUsed.permanentChildUsage
                );
            } else {
                if (leader) {
                    logger.info(
                        "GC did not bring memory usage down, before [{}], after [{}], allocations [{}], duration [{}]",
                        memoryUsed.baseUsage,
                        current,
                        allocationIndex,
                        allocationDuration
                    );
                }
                // prefer original measurement when reporting if heap usage was not brought down.
                return memoryUsed;
            }
        }

        void overLimitTriggered(boolean leader) {
            // for tests to override.
        }

        TimeValue getLockTimeout() {
            return lockTimeout;
        }
    }
}
