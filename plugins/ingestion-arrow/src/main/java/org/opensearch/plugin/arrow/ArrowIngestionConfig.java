/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.util.Locale;
import java.util.Map;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.core.util.ConfigurationUtils;

/** Common configuration for arrow ingestion, parsed from {@link IngestionSource#params()} */
public class ArrowIngestionConfig {

    /* required parameters start */
    /** specify the sharding key field, the field must be a string field */
    public static final String SHARDING_KEY_PROP_KEY = "sharding_key";

    /**
     * specify the dataset type (e.g. {@code lance}, {@code iceberg}). The value is matched
     * case-insensitively against the {@link ArrowSourceFactory#getType()} of every {@link
     * ArrowSourceFactory} registered with {@link ArrowPlugin} — see that plugin's {@code
     * loadExtensions} for how a new dataset type is added without forking this plugin.
     */
    public static final String DATASET_TYPE_PROP_KEY = "dataset_type";

    /* required parameters end */

    /**
     * Optional exclusive upper bound on the global row number to consume — the last row indexed
     * will be the configured value minus 1. Absent means the whole dataset is consumed.
     *
     * <p>Intended for iterating on a subset of a large dataset — e.g. debugging a data issue,
     * validating a new mapping / sharding key against a slice before committing to a full reingest,
     * or running a bounded backfill. Not intended as a permanent filter; use a query-time filter
     * for that.
     *
     * <p>Note: the start offset can be set by the initial reset pointer in the pull-based ingestion
     * config, {@link org.opensearch.indices.pollingingest.StreamPoller.ResetState#RESET_BY_OFFSET},
     * so together they carve a [start, row_bound) window.
     */
    public static final String ROW_BOUND = "row_bound";

    /**
     * Optional configuration for the id column configuration. If set, the column will be used as
     * the source of the metadata _id field, if not set, the string value of row number will be used
     * as the metadata _id field to avoid random allocation of ids.
     *
     * <p>The column must be an Arrow string type (Utf8, Utf8View or LargeUtf8) and must be non-null
     * in every row; anything else fails the ingestion. Values are used verbatim, so duplicates
     * across rows collide into a single document.
     */
    public static final String ID_COLUMN = "id_column";

    private final String sourceType;
    private final String shardingKey;
    private final int numShards;
    private final Long rowBound;
    private final String idColumn;

    /**
     * Extract common configs from IngestionSource's parameters and from the index's own metadata,
     * note there could also be source specific parameters that are not parsed by this class, see
     * {@link ArrowSourceFactory}. An example params can be: <br>
     * <code>
     *      {
     *          "sharding_key": "id",
     *          "dataset_type": "lance",
     *          ... // other parameters
     *      }
     *  </code>
     *
     * @param params from {@link IngestionSource#params()}
     * @param indexMetadata the index's metadata
     */
    public ArrowIngestionConfig(Map<String, Object> params, IndexMetadata indexMetadata) {
        this.sourceType =
                ConfigurationUtils.readStringProperty(params, DATASET_TYPE_PROP_KEY)
                        .toUpperCase(Locale.ROOT);
        this.shardingKey = ConfigurationUtils.readStringProperty(params, SHARDING_KEY_PROP_KEY);
        this.numShards = indexMetadata.getNumberOfShards();
        this.rowBound = parseRowBound(params);
        this.idColumn = ConfigurationUtils.readStringProperty(params, ID_COLUMN, "").trim();
    }

    /**
     * Parses {@link #ROW_BOUND} as a positive row count. Absent or blank means unbounded (the whole
     * dataset is consumed).
     */
    private static Long parseRowBound(Map<String, Object> params) {
        String raw = ConfigurationUtils.readStringProperty(params, ROW_BOUND, "").trim();
        if (raw.isEmpty()) {
            return null;
        }
        long bound;
        try {
            bound = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    ROW_BOUND + " must be a positive integer, got: " + raw, e);
        }
        if (bound <= 0) {
            // A non-positive bound consumes nothing, which silently looks like a broken index.
            throw new IllegalArgumentException(
                    ROW_BOUND + " must be a positive integer, got: " + bound);
        }
        return bound;
    }

    /**
     * Returns the configured dataset type.
     *
     * @return the configured dataset type, upper-cased, e.g. {@code "LANCE"}. Matched against
     *     every registered {@link ArrowSourceFactory#getType()} by {@link ArrowConsumerFactory}.
     */
    public String getSourceType() {
        return sourceType;
    }

    /**
     * Returns the sharding key field name.
     *
     * @return sharding key field name
     */
    public String getShardingKey() {
        return shardingKey;
    }

    /**
     * Returns the total number of shards.
     *
     * @return total number of shards
     */
    public int getNumShards() {
        return numShards;
    }

    /**
     * Returns the configured row bound.
     *
     * @return exclusive upper bound on the global row number to consume, or {@code null} if
     *     unbounded. See {@link #ROW_BOUND}.
     */
    public Long getRowBound() {
        return rowBound;
    }

    /**
     * Returns the configured id column name.
     *
     * @return the configured id column name, or the empty string if none was configured. See
     *     {@link #ID_COLUMN}.
     */
    public String getIdColumn() {
        return idColumn;
    }
}
