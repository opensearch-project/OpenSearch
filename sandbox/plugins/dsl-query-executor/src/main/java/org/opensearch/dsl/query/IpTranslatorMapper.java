/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.net.InetAddress;
import java.util.Optional;

/**
 * Translator mapper for IP-typed fields. Encodes bound values as 16-byte IPv6-mapped sortable
 * bytes and builds VARBINARY literal comparisons with {@code allowCast=false}, matching legacy
 * {@code IpFieldMapper.rangeQuery} with {@code InetAddressPoint.encode} byte ordering.
 *
 * <p>This mapper is a stateless singleton shared across every IP field in the schema.
 * No per-field state is held; all parameters are derived from the {@code RelDataType} on each call.
 */
final class IpTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final IpTranslatorMapper INSTANCE = new IpTranslatorMapper();

    private IpTranslatorMapper() {}

    /**
     * Translates a single range bound for an IP field to a byte-range comparison.
     * Uses raw inclusivity directly from the query (no adjustment), and builds a VARBINARY
     * literal with allowCast=false to prevent Calcite from wrapping in a CAST.
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        if (value == null) {
            return null;
        }

        byte[] encoded = encodeIpAsIpv6(String.valueOf(value));
        RelDataType varbinaryType = ctx.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.VARBINARY);
        // allowCast=false: prevents Calcite from wrapping the literal in a CAST, preserving
        // the exact byte comparison plan that legacy IpFieldMapper.rangeQuery produces.
        RexNode literal = ctx.getRexBuilder().makeLiteral(new ByteString(encoded), varbinaryType, false);
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        // Operator selected from raw inclusivity flag - never from an adjusted value.
        SqlOperator op;
        if (isLower) {
            op = inclusive ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN;
        } else {
            op = inclusive ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;
        }

        return ctx.getRexBuilder().makeCall(op, fieldRef, literal);
    }

    /**
     * Term queries on IP fields are not yet supported on this path.
     * Legacy {@code IpFieldMapper.termQuery} supports them, but implementing without verified
     * parity would replace a loud crash (ClassCastException) with a possibly silently-wrong answer.
     *
     * @throws ConversionException always, with a clear message for HTTP 400 surfacing
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        throw new ConversionException(
            "Term queries on ip fields are not yet supported on the DSL conversion path. Field: [" + field.getName() + "]"
        );
    }

    /**
     * IPv6-mapped 16-byte encoding matching what the parquet writer stores. IPv4 input is
     * encoded as 10 zero bytes + 0xff 0xff + 4 IPv4 bytes (RFC 4291 section 2.5.5.2).
     * IPv6 is its raw 16 bytes. Identical to {@code CidrMatchFunctionAdapter.encodeIpAsIpv6}
     * and {@code InetAddressPoint.encode} byte layout.
     *
     * <p>Uses {@code InetAddresses.forString} for strict textual IP parsing without DNS
     * resolution, matching legacy behavior (IpFieldMapper uses InetAddresses.forString;
     * hostname input is rejected).
     *
     * @param value textual IPv4 or IPv6 address (e.g. "192.168.0.1" or "::1")
     * @return 16-byte IPv6-mapped encoding
     * @throws ConversionException if the value is not a valid literal IP address
     */
    static byte[] encodeIpAsIpv6(String value) throws ConversionException {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddresses.forString(value);
        } catch (IllegalArgumentException e) {
            throw new ConversionException("Failed to parse IP address value '" + value + "': not a valid IPv4 or IPv6 literal");
        }
        byte[] addr = inetAddress.getAddress();
        if (addr.length == 16) {
            return addr;
        }
        byte[] mapped = new byte[16];
        mapped[10] = (byte) 0xff;
        mapped[11] = (byte) 0xff;
        System.arraycopy(addr, 0, mapped, 12, 4);
        return mapped;
    }
}
