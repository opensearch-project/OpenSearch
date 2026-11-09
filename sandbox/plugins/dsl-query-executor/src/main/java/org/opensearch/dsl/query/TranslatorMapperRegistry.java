/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.analytics.schema.UnsignedLongType;

import java.util.Map;

/**
 * Two-tier resolver mapping field types to their translator mapper.
 *
 * <p>Tier 1 keys on the exact Java class of the {@code RelDataType}, handling UDT marker types
 * (e.g. ScaledFloatType, UnsignedLongType, IpType) that share the same {@code SqlTypeName}
 * (BIGINT or VARBINARY). Tier 2 keys on {@code SqlTypeName} for Calcite's own types.
 * The fallback is {@link DefaultTranslatorMapper} and NEVER a throw, preserving today's
 * deny-list polarity (one explicit rejection plus a permissive generic tail).
 *
 * <p>Both tier maps are populated incrementally as per-type mappers are introduced.
 */
public final class TranslatorMapperRegistry {

    /** Singleton instance. */
    public static final TranslatorMapperRegistry INSTANCE = new TranslatorMapperRegistry();

    /** Tier 1: UDT marker types keyed on exact class. */
    private final Map<Class<?>, BaseTranslatorMapper> byUdtClass = Map.of(
        ScaledFloatType.class,
        ScaledFloatTranslatorMapper.INSTANCE,
        UnsignedLongType.class,
        UnsignedLongTranslatorMapper.INSTANCE,
        IpType.class,
        IpTranslatorMapper.INSTANCE
    );

    /**
     * Tier 2: Calcite built-in types keyed on SqlTypeName.
     * VARBINARY maps to RejectingTranslatorMapper here, but an IP field (IpType) will match
     * tier 1 first and get IpTranslatorMapper. Tier ordering reproduces the former compound
     * rule in convert(): tier 1 matches IpType.class before tier 2 ever sees VARBINARY, so
     * an ip field gets the IP mapper and a plain binary field gets rejected.
     */
    private final Map<SqlTypeName, BaseTranslatorMapper> bySqlType = Map.of(
        SqlTypeName.TIMESTAMP,
        TimestampTranslatorMapper.INSTANCE,
        SqlTypeName.DATE,
        TimestampTranslatorMapper.INSTANCE,
        SqlTypeName.VARBINARY,
        RejectingTranslatorMapper.INSTANCE
    );

    private TranslatorMapperRegistry() {}

    /**
     * Resolves the mapper for a given field type. Returns tier 1 hit, else tier 2 hit,
     * else the catch-all {@link DefaultTranslatorMapper}.
     *
     * @param fieldType the Calcite type of the field
     * @return the appropriate translator mapper, never null
     */
    public BaseTranslatorMapper resolve(RelDataType fieldType) {
        BaseTranslatorMapper udt = byUdtClass.get(fieldType.getClass());
        if (udt != null) {
            return udt;
        }
        BaseTranslatorMapper byName = bySqlType.get(fieldType.getSqlTypeName());
        return byName != null ? byName : DefaultTranslatorMapper.INSTANCE;
    }
}
