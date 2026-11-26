/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.common.Strings;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;

import java.text.Normalizer;
import java.util.Locale;
import java.util.Objects;

/**
 * Enforces the hard-reject validation rules for {@link ObjectStoreSourceConfig} that are checkable from the
 * configuration alone, before any object store is contacted. Every violation raises an
 * {@link IllegalArgumentException} with a specific message describing the offending rule.
 */
final class ObjectStoreSourceConfigValidator {

    private static final String NDJSON = "ndjson";

    private ObjectStoreSourceConfigValidator() {}

    /**
     * Validates the pointer initialization reset state. The {@code object_store} source is a bounded, offset-based
     * source, so tail ({@code latest}) and time-based ({@code reset_by_timestamp}) resets are rejected; only
     * {@code earliest}, {@code reset_by_offset}, and {@code none} are supported.
     *
     * @param resetState the configured reset state (may be {@code null} when unset)
     * @throws IllegalArgumentException if the reset state is {@code latest} or {@code reset_by_timestamp}
     */
    public static void validatePointerReset(StreamPoller.ResetState resetState) {
        if (resetState == StreamPoller.ResetState.LATEST || resetState == StreamPoller.ResetState.RESET_BY_TIMESTAMP) {
            throw new IllegalArgumentException(
                "pointer.init.reset ["
                    + resetState.name().toLowerCase(Locale.ROOT)
                    + "] is not supported by the object_store source; use one of [earliest, reset_by_offset, none]"
            );
        }
    }

    /**
     * Validates an index-level {@code reset_by_offset} against the shard layout. The index-level reset value is a single
     * pointer that core hands to every shard, but a pointer addresses one object (and, under {@code equal_bytes}, one
     * shard's byte range), so it is meaningful to exactly one shard. Under {@code whole_files} and
     * {@code shard_prefixes} every other shard rejects the pointer as not owned on every start and never ingests. Under
     * {@code equal_bytes} a shard whose range ends before the offset finds nothing to read at the pointer and then
     * resumes from the start of its own range, so the bytes before the offset are skipped only within the owning
     * shard's range. The reset is therefore accepted only for a single-shard index.
     *
     * @param resetState the configured reset state (may be {@code null} when unset)
     * @param shardSplit the configured shard split strategy
     * @param numShards  the number of shards of the index
     * @throws IllegalArgumentException if the reset state is {@code reset_by_offset} and the index has more than one shard
     */
    public static void validatePointerResetForShards(StreamPoller.ResetState resetState, ShardSplit shardSplit, int numShards) {
        if (resetState != StreamPoller.ResetState.RESET_BY_OFFSET || numShards <= 1) {
            return;
        }
        String consequence = shardSplit == ShardSplit.EQUAL_BYTES
            ? "the offset falls in exactly one shard's byte range, and a shard whose range ends before the offset still reads its "
                + "whole range, so the bytes before the offset would be skipped only within the owning shard's range"
            : "the pointer's object is owned by exactly one shard, and every other shard rejects the pointer as not owned on "
                + "every start and never ingests";
        throw new IllegalArgumentException(
            "index-level pointer.init.reset [reset_by_offset] is not supported by the object_store source for an index with ["
                + numShards
                + "] shards and ["
                + ObjectStoreSourceConfig.SHARD_SPLIT
                + "="
                + shardSplit.getName()
                + "]: core applies the same pointer to every shard, but "
                + consequence
                + ". Use [earliest] or [none], or a single-shard index, and reset individual shards with "
                + "POST /<index>/ingestion/_resume and reset_settings"
        );
    }

    /**
     * Validates the configured mapper type. v0 supports only the positional {@code _id} produced by the
     * {@code raw_payload} mapper, so every other mapper is rejected to preserve the by-construction no-duplication
     * invariant. The {@code default} mapper, which is also what an unset {@code mapper_type} resolves to, takes the
     * {@code _id} and {@code _op_type} from the record itself (or generates a random {@code _id}) and rejects
     * records without a {@code _source} field as invalid. The {@code field_mapping} mapper (which enables natural-key
     * {@code id_field}, {@code version_field}, and {@code op_type_field}) requires cross-slice key-to-shard routing
     * that is not yet available.
     *
     * @param mapperType the configured mapper type; {@code null} is treated as unset and rejected like {@code default}
     * @throws IllegalArgumentException if the mapper type is not {@code raw_payload}
     */
    public static void validateMapper(IngestionMessageMapper.MapperType mapperType) {
        if (mapperType == IngestionMessageMapper.MapperType.RAW_PAYLOAD) {
            return;
        }
        String configured = mapperType == null ? "unset" : mapperType.getName();
        String reason = mapperType == IngestionMessageMapper.MapperType.FIELD_MAPPING
            ? "it derives _id/_version/_op_type from record fields (id_field/version_field/op_type_field), which needs cross-slice "
                + "key-to-shard routing that v0 does not have"
            : "the default mapper (also what an unset mapper_type resolves to) takes _id and _op_type from the record or generates "
                + "a random _id, and rejects records without a _source field as invalid";
        throw new IllegalArgumentException(
            "mapper_type ["
                + configured
                + "] is not supported by the object_store source in v0; "
                + reason
                + ". Set ["
                + IndexMetadata.SETTING_INGESTION_SOURCE_MAPPER_TYPE
                + ": "
                + IngestionMessageMapper.MapperType.RAW_PAYLOAD.getName()
                + "] so each record gets a positional _id"
        );
    }

    /**
     * Validates the supplied configuration against the hard-reject rules.
     *
     * @param config the parsed object-store source configuration
     * @throws IllegalArgumentException if any rule is violated
     */
    public static void validate(ObjectStoreSourceConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.getDiscovery() == Discovery.LIST) {
            validate(config, ObjectStoreManifest.EMPTY);
            return;
        }
        requirePositive(ObjectStoreSourceConfig.MAX_OBJECTS, config.getMaxObjects());
        validate(config, ManifestParser.parse(config.getManifestJson(), config.getMaxObjects()));
    }

    /**
     * Validates the supplied configuration against the hard-reject rules using an already-parsed manifest, avoiding
     * a second parse of the {@code manifest} JSON when the caller has already produced it.
     *
     * @param config   the parsed object-store source configuration
     * @param manifest the already-parsed manifest
     * @throws IllegalArgumentException if any rule is violated
     */
    public static void validate(ObjectStoreSourceConfig config, ObjectStoreManifest manifest) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(manifest, "manifest must not be null");

        validateRepository(config.getRepository());

        String sourceUrl = config.getSourceUrl();
        String sourcePrefix = config.getSourcePrefix();
        boolean hasUrl = Strings.hasText(sourceUrl);
        boolean hasPrefix = Strings.hasText(sourcePrefix);
        validateShape(hasUrl, hasPrefix);

        validateShardSplit(config.getShardSplit(), hasUrl, hasPrefix);
        validateFormat(config.getFormat());

        String location = hasUrl ? sourceUrl : sourcePrefix;
        validatePathSafety(location);

        requirePositive(ObjectStoreSourceConfig.MAX_RECORD_BYTES, config.getMaxRecordBytes());
        requirePositive(ObjectStoreSourceConfig.MAX_OBJECTS, config.getMaxObjects());
        requirePositive(ObjectStoreSourceConfig.MAX_LIST_REQUESTS, config.getMaxListRequests());
        requirePositive(ObjectStoreSourceConfig.MAX_GET_REQUESTS, config.getMaxGetRequests());
        requirePositive(ObjectStoreSourceConfig.MAX_BYTES, config.getMaxBytes());

        if (config.getDiscovery() == Discovery.LIST) {
            validateListDiscovery(config);
        } else {
            validateManifest(config, manifest, sourceUrl, sourcePrefix);
        }
    }

    private static void validateListDiscovery(ObjectStoreSourceConfig config) {
        if (config.getShardSplit() != ShardSplit.SHARD_PREFIXES) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "="
                    + Discovery.LIST.getName()
                    + "] requires ["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "="
                    + ShardSplit.SHARD_PREFIXES.getName()
                    + "], got ["
                    + config.getShardSplit().getName()
                    + "]"
            );
        }
        if (Strings.hasText(config.getManifestJson())) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "] must not be set when ["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "="
                    + Discovery.LIST.getName()
                    + "]"
            );
        }
    }

    private static void validateManifest(
        ObjectStoreSourceConfig config,
        ObjectStoreManifest manifest,
        String sourceUrl,
        String sourcePrefix
    ) {
        for (String key : manifest.keys()) {
            validateManifestKeySafety(key);
        }
        if (config.getShardSplit() == ShardSplit.EQUAL_BYTES) {
            validateEqualBytesManifest(manifest, sourceUrl);
        } else {
            validatePrefixManifest(manifest, sourcePrefix);
        }
    }

    private static void validateManifestKeySafety(String key) {
        if (isUnsafeRepositoryKey(key)) {
            throw new IllegalArgumentException(unsafeKeyMessage("[" + ObjectStoreSourceConfig.MANIFEST + "] key", key));
        }
    }

    /**
     * Renders the single operator-facing message describing why a repository-relative key was refused, so the
     * {@code manifest} keys and the {@code source_url}/{@code source_prefix} location state the same rule.
     *
     * @param subject the name of the refused value, as it should appear at the start of the message
     * @param value   the refused value
     * @return the rendered message
     */
    private static String unsafeKeyMessage(String subject, String value) {
        return subject
            + " must be a repository-relative key without path traversal [..], empty path segments [//], "
            + "wildcard characters [*, ?], or a leading [/], got ["
            + LogSanitizer.sanitize(value)
            + "]";
    }

    private static void validateEqualBytesManifest(ObjectStoreManifest manifest, String sourceUrl) {
        if (manifest.objectCount() != 1 || manifest.contains(sourceUrl) == false) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "="
                    + ShardSplit.EQUAL_BYTES.getName()
                    + "] requires the ["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "] to contain exactly one entry whose key equals ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "="
                    + LogSanitizer.sanitize(sourceUrl)
                    + "], got ["
                    + manifest.objectCount()
                    + "] entries"
                    + firstKeySuffix(manifest)
            );
        }
        if (manifest.sizeOf(sourceUrl) == 0L) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "="
                    + ShardSplit.EQUAL_BYTES.getName()
                    + "] cannot ingest ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "="
                    + LogSanitizer.sanitize(sourceUrl)
                    + "] because the ["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "] declares it as 0 bytes; a declared-empty object yields no byte range to split, so every shard "
                    + "would own nothing and the index would report completion having ingested no documents"
            );
        }
    }

    /**
     * Renders the first manifest key for an error message, or the empty string for an empty manifest. Only the first key
     * is named, and it is sanitized: a manifest may hold thousands of entries, so embedding all of them would allocate a
     * message proportional to the manifest and would put untrusted key bytes straight into a log line.
     *
     * @param manifest the manifest being reported on
     * @return the rendered suffix, or the empty string
     */
    private static String firstKeySuffix(ObjectStoreManifest manifest) {
        if (manifest.objectCount() == 0) {
            return "";
        }
        return ", the first being [" + LogSanitizer.sanitize(manifest.keys().first()) + "]";
    }

    private static void validatePrefixManifest(ObjectStoreManifest manifest, String sourcePrefix) {
        for (String key : manifest.keys()) {
            if (key.startsWith(sourcePrefix) == false) {
                throw new IllegalArgumentException(
                    "["
                        + ObjectStoreSourceConfig.MANIFEST
                        + "] key ["
                        + LogSanitizer.sanitize(key)
                        + "] must start with ["
                        + ObjectStoreSourceConfig.SOURCE_PREFIX
                        + "="
                        + LogSanitizer.sanitize(sourcePrefix)
                        + "]"
                );
            }
        }
    }

    private static void validateRepository(String repository) {
        if (Strings.hasText(repository) == false) {
            throw new IllegalArgumentException("[" + ObjectStoreSourceConfig.REPOSITORY + "] is required and must not be blank");
        }
    }

    private static void validateShape(boolean hasUrl, boolean hasPrefix) {
        if (hasUrl == false && hasPrefix == false) {
            throw new IllegalArgumentException(
                "exactly one of ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "] or ["
                    + ObjectStoreSourceConfig.SOURCE_PREFIX
                    + "] must be set, but neither was provided"
            );
        }
        if (hasUrl && hasPrefix) {
            throw new IllegalArgumentException(
                "exactly one of ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "] or ["
                    + ObjectStoreSourceConfig.SOURCE_PREFIX
                    + "] must be set, but both were provided"
            );
        }
    }

    private static void validateShardSplit(ShardSplit shardSplit, boolean hasUrl, boolean hasPrefix) {
        if (shardSplit == ShardSplit.EQUAL_BYTES && hasUrl == false) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "="
                    + shardSplit.getName()
                    + "] requires ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "], not ["
                    + ObjectStoreSourceConfig.SOURCE_PREFIX
                    + "]"
            );
        }
        if ((shardSplit == ShardSplit.WHOLE_FILES || shardSplit == ShardSplit.SHARD_PREFIXES) && hasPrefix == false) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "="
                    + shardSplit.getName()
                    + "] requires ["
                    + ObjectStoreSourceConfig.SOURCE_PREFIX
                    + "], not ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "]"
            );
        }
    }

    private static void validateFormat(String format) {
        if (Strings.hasText(format) == false || format.trim().toLowerCase(Locale.ROOT).equals(NDJSON) == false) {
            throw new IllegalArgumentException(
                "[" + ObjectStoreSourceConfig.FORMAT + "] must be [ndjson] in v0, got [" + LogSanitizer.sanitize(format) + "]"
            );
        }
    }

    private static void validatePathSafety(String location) {
        if (isUnsafeRepositoryKey(location)) {
            throw new IllegalArgumentException(
                unsafeKeyMessage("[" + ObjectStoreSourceConfig.SOURCE_URL + "]/[" + ObjectStoreSourceConfig.SOURCE_PREFIX + "]", location)
            );
        }
    }

    /**
     * Returns whether a repository-relative key or prefix is unsafe. The value is NFKC-normalized before the check so
     * that unicode-encoded forms (for example a fullwidth solidus that normalizes to {@code /}) cannot slip a path
     * traversal, empty segment, wildcard, or leading separator past validation. Only the normalized form is inspected;
     * the original key is used verbatim for reads.
     *
     * @param value the repository-relative key or prefix (must not be {@code null})
     * @return {@code true} if the value contains path traversal [..], an empty path segment [//], a wildcard [*, ?], or
     *         a leading [/]
     */
    private static boolean isUnsafeRepositoryKey(String value) {
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFKC);
        if (normalized.contains("//") || normalized.indexOf('*') >= 0 || normalized.indexOf('?') >= 0 || normalized.startsWith("/")) {
            return true;
        }
        for (String segment : normalized.split("/", -1)) {
            if ("..".equals(segment) || ".".equals(segment)) {
                return true;
            }
        }
        return false;
    }

    private static void requirePositive(String param, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("[" + param + "] must be greater than 0, got [" + value + "]");
        }
    }

}
