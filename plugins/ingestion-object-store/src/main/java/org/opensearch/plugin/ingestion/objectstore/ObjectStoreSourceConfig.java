/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.core.util.ConfigurationUtils;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Parsed configuration for the {@code object_store} ingestion source, sourced from the
 * {@code index.ingestion_source.param.*} settings. The storage backend is a registered OpenSearch repository, named
 * by the required {@link #REPOSITORY} parameter; {@code source_url} and {@code source_prefix} are
 * repository-relative keys or prefixes (for example {@code dump/events.ndjson} or {@code dump/}), not URLs. Exactly
 * one of {@code source_url} or {@code source_prefix} must be provided, and the chosen {@link ShardSplit} must be
 * compatible with it. This class only parses and type-checks; every cross-field rule is enforced by
 * {@link ObjectStoreSourceConfigValidator}, so an instance of this class is not by itself a valid configuration.
 */
public final class ObjectStoreSourceConfig {

    /**
     * The name of the registered repository that provides the backing {@link org.opensearch.common.blobstore.BlobStore}.
     */
    public static final String REPOSITORY = "repository";
    /**
     * A single repository-relative object key (mutually exclusive with {@link #SOURCE_PREFIX}).
     */
    public static final String SOURCE_URL = "source_url";
    /**
     * A repository-relative object prefix (mutually exclusive with {@link #SOURCE_URL}).
     */
    public static final String SOURCE_PREFIX = "source_prefix";
    /**
     * Framing format; only {@code ndjson} is supported in v0.
     */
    public static final String FORMAT = "format";
    /**
     * The shard split strategy.
     */
    public static final String SHARD_SPLIT = "shard_split";
    /**
     * How a shard discovers the objects it owns: {@code manifest} (the default) or {@code list}.
     */
    public static final String DISCOVERY = "discovery";
    /**
     * The explicit, size-capped list of objects to ingest, as a JSON array of {@code {"key":..,"size":..}} entries
     * with repository-relative keys. Required when {@link #DISCOVERY} is {@code manifest} (the default); every shard
     * reads it from the index metadata and builds the frozen manifest from it. Must be omitted when {@link #DISCOVERY}
     * is {@code list}.
     * <p>
     * The value is a JSON string, not a JSON array, because an ingestion-source param is an index setting and
     * {@code Settings} cannot hold structure: {@code Settings.fromXContent} rejects an array of objects with
     * {@code "only value lists are allowed in serialized settings"}.
     */
    public static final String MANIFEST = "manifest";
    /**
     * Hard per-record byte cap.
     */
    public static final String MAX_RECORD_BYTES = "max_record_bytes";
    /**
     * Hard cap on manifest cardinality.
     */
    public static final String MAX_OBJECTS = "max_objects";
    /**
     * Cost cap on the cumulative number of list requests issued against the backing store.
     */
    public static final String MAX_LIST_REQUESTS = "max_list_requests";
    /**
     * Cost cap on the cumulative number of get (read blob) requests issued against the backing store.
     */
    public static final String MAX_GET_REQUESTS = "max_get_requests";
    /**
     * Cost cap on the cumulative number of bytes read from the backing store.
     */
    public static final String MAX_BYTES = "max_bytes";

    /**
     * Default value for {@link #FORMAT}.
     */
    public static final String DEFAULT_FORMAT = "ndjson";
    /**
     * Default value for {@link #MAX_RECORD_BYTES} (10 MB).
     */
    public static final long DEFAULT_MAX_RECORD_BYTES = 10L * 1024 * 1024;
    /**
     * Default value for {@link #MAX_OBJECTS}.
     */
    public static final long DEFAULT_MAX_OBJECTS = 1_000_000L;
    /**
     * Hard cap on the serialized size of {@link #MANIFEST}. The manifest is carried in an index setting, so its byte
     * size, not its cardinality, is what bounds the cluster state; a larger manifest must use {@code discovery=list}.
     */
    public static final int MAX_MANIFEST_BYTES = 1024 * 1024;

    /**
     * Default value for {@link #DISCOVERY}.
     */
    public static final Discovery DEFAULT_DISCOVERY = Discovery.MANIFEST;

    private final String repository;
    private final String sourceUrl;
    private final String sourcePrefix;
    private final String manifestJson;
    private final String format;
    private final ShardSplit shardSplit;
    private final Discovery discovery;
    private final long maxRecordBytes;
    private final long maxObjects;
    private final long maxListRequests;
    private final long maxGetRequests;
    private final long maxBytes;

    /**
     * Parses configuration from the ingestion source parameter map.
     *
     * @param params the {@code index.ingestion_source.param.*} values
     */
    public ObjectStoreSourceConfig(Map<String, Object> params) {
        Objects.requireNonNull(params, "params must not be null");
        this.repository = ConfigurationUtils.readOptionalStringProperty(params, REPOSITORY);
        this.sourceUrl = ConfigurationUtils.readOptionalStringProperty(params, SOURCE_URL);
        this.sourcePrefix = ConfigurationUtils.readOptionalStringProperty(params, SOURCE_PREFIX);
        this.manifestJson = ConfigurationUtils.readOptionalStringProperty(params, MANIFEST);
        this.format = ConfigurationUtils.readStringProperty(params, FORMAT, DEFAULT_FORMAT);
        this.shardSplit = ShardSplit.fromString(ConfigurationUtils.readOptionalStringProperty(params, SHARD_SPLIT));
        this.discovery = Discovery.fromString(ConfigurationUtils.readOptionalStringProperty(params, DISCOVERY));
        this.maxRecordBytes = readLong(params, MAX_RECORD_BYTES, DEFAULT_MAX_RECORD_BYTES);
        this.maxObjects = readLong(params, MAX_OBJECTS, DEFAULT_MAX_OBJECTS);
        this.maxListRequests = readLong(params, MAX_LIST_REQUESTS, Long.MAX_VALUE);
        this.maxGetRequests = readLong(params, MAX_GET_REQUESTS, Long.MAX_VALUE);
        this.maxBytes = readLong(params, MAX_BYTES, Long.MAX_VALUE);
    }

    private static long readLong(Map<String, Object> params, String key, long defaultValue) {
        Object value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
                return number.longValue();
            }
            if (number instanceof BigInteger) {
                BigInteger big = (BigInteger) number;
                if (big.bitLength() >= Long.SIZE) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Parameter [%s] is outside the range of a long, got [%s]", key, value)
                    );
                }
                return big.longValueExact();
            }
            double asDouble = number.doubleValue();
            if (Double.isFinite(asDouble) == false || asDouble != Math.rint(asDouble)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be an integer, got [%s]", key, value));
            }
            if (asDouble < -9.223372036854776E18 || asDouble >= 9.223372036854776E18) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Parameter [%s] is outside the range of a long, got [%s]", key, value)
                );
            }
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be a long, got [%s]", key, value), e);
        }
    }

    /**
     * Returns the name of the registered repository providing the backing blob store.
     *
     * @return the repository name
     */
    public String getRepository() {
        return repository;
    }

    /**
     * Returns the single repository-relative object key, or {@code null} if a prefix is configured.
     *
     * @return the source URL
     */
    public String getSourceUrl() {
        return sourceUrl;
    }

    /**
     * Returns the repository-relative object prefix, or {@code null} if a single key is configured.
     *
     * @return the source prefix
     */
    public String getSourcePrefix() {
        return sourcePrefix;
    }

    /**
     * Returns the raw JSON string of the explicit v0 manifest (a JSON array of {@code {"key":..,"size":..}} entries),
     * or {@code null} if the {@code manifest} parameter was not provided.
     *
     * @return the raw manifest JSON, or {@code null}
     */
    public String getManifestJson() {
        return manifestJson;
    }

    /**
     * Returns the framing format.
     *
     * @return the format
     */
    public String getFormat() {
        return format;
    }

    /**
     * Returns the shard split strategy.
     *
     * @return the shard split
     */
    public ShardSplit getShardSplit() {
        return shardSplit;
    }

    /**
     * Returns how a shard discovers the objects it owns.
     *
     * @return the discovery mode
     */
    public Discovery getDiscovery() {
        return discovery;
    }

    /**
     * Returns the hard per-record byte cap.
     *
     * @return the max record bytes
     */
    public long getMaxRecordBytes() {
        return maxRecordBytes;
    }

    /**
     * Returns the hard cap on manifest cardinality.
     *
     * @return the max objects
     */
    public long getMaxObjects() {
        return maxObjects;
    }

    /**
     * Returns the cost cap on the cumulative number of list requests, or {@link Long#MAX_VALUE} if unlimited.
     *
     * @return the max list requests
     */
    public long getMaxListRequests() {
        return maxListRequests;
    }

    /**
     * Returns the cost cap on the cumulative number of get requests, or {@link Long#MAX_VALUE} if unlimited.
     *
     * @return the max get requests
     */
    public long getMaxGetRequests() {
        return maxGetRequests;
    }

    /**
     * Returns the cost cap on the cumulative number of bytes read, or {@link Long#MAX_VALUE} if unlimited.
     *
     * @return the max bytes
     */
    public long getMaxBytes() {
        return maxBytes;
    }
}
