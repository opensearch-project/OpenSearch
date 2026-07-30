/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;

/**
 * Backend-side dispatch for PPL {@code CIDRMATCH(ip, cidr)} that translates
 * the call into a form DataFusion can execute. Three tiers, in order:
 *
 * <ol>
 *   <li><b>Both args literal</b> &mdash; parse the IP and the CIDR at plan
 *       time and fold to a Boolean {@link RexLiteral}, so the call never
 *       reaches DataFusion (which has no CIDRMATCH UDF).
 *   <li><b>VARBINARY column + cidr literal</b> &mdash; expand to a byte-range
 *       {@code AND(col >= low, col <= high)} against the column's 16-byte
 *       IPv6-mapped encoding. {@code IpType} is backed by VARBINARY, so
 *       {@code ip} columns hit this gate too.
 *   <li><b>Anything else</b> &mdash; return the call unchanged. DataFusion
 *       will reject it ("No backend supports scalar function [CIDRMATCH]")
 *       until a Rust UDF covers the dynamic case.
 * </ol>
 *
 * @opensearch.internal
 */
class CidrMatchFunctionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexNode ipOperand = original.getOperands().get(0);
        RexNode cidrOperand = original.getOperands().get(1);

        // Tier 1: both args literal — fold to BOOLEAN literal.
        if (ipOperand instanceof RexLiteral ipLit && cidrOperand instanceof RexLiteral cidrLit) {
            String ipString = ipLit.getValueAs(String.class);
            String cidrString = cidrLit.getValueAs(String.class);
            if (ipString != null && cidrString != null) {
                Boolean result = tryEvaluate(ipString, cidrString);
                if (result != null) {
                    RexBuilder rexBuilder = cluster.getRexBuilder();
                    RelDataType boolType = cluster.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
                    RexNode literal = rexBuilder.makeLiteral(result, boolType, false);
                    if (!literal.getType().equals(original.getType())) {
                        literal = rexBuilder.makeAbstractCast(original.getType(), literal);
                    }
                    return literal;
                }
            }
        }

        // Tier 2: cidr literal + col with VARBINARY underlying — byte-range AND rewrite.
        if (cidrOperand instanceof RexLiteral cidrLit && ipOperand.getType().getSqlTypeName() == SqlTypeName.VARBINARY) {
            String cidrString = cidrLit.getValueAs(String.class);
            if (cidrString == null) {
                return original;
            }
            byte[][] range;
            try {
                range = parseCidrToIpv6Range(cidrString);
            } catch (IllegalArgumentException e) {
                return original;
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataType varbinary = cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY);
            RexNode low = rexBuilder.makeLiteral(new ByteString(range[0]), varbinary, false);
            RexNode high = rexBuilder.makeLiteral(new ByteString(range[1]), varbinary, false);
            return rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ipOperand, low),
                rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ipOperand, high)
            );
        }

        return original;
    }

    /**
     * Parse a CIDR string and return its lower and upper bounds in canonical
     * 16-byte IPv6-mapped form.
     */
    static byte[][] parseCidrToIpv6Range(String cidr) {
        IPAddress range = new IPAddressString(cidr).getAddress();
        if (range == null) {
            throw new IllegalArgumentException("invalid cidr: " + cidr);
        }
        byte[] low = range.getLower().toIPv6().getBytes();
        byte[] high = range.getUpper().toIPv6().getBytes();
        return new byte[][] { low, high };
    }

    /**
     * Folds {@code CIDRMATCH(<ip-literal>, <cidr-literal>)} at plan time. Returns
     * {@code null} if either side fails to parse — caller falls through.
     */
    static Boolean tryEvaluate(String ipString, String cidrString) {
        byte[] ipBytes = encodeIpAsIpv6(ipString);
        if (ipBytes == null) {
            return null;
        }
        byte[][] range;
        try {
            range = parseCidrToIpv6Range(cidrString);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return compareUnsigned(ipBytes, range[0]) >= 0 && compareUnsigned(ipBytes, range[1]) <= 0;
    }

    /**
     * IPv6-mapped 16-byte encoding matching what the parquet writer stores. IPv4
     * input is encoded as 10 zero bytes + {@code 0xff 0xff} + 4 IPv4 bytes
     * (RFC 4291 §2.5.5.2). IPv6 is its raw 16 bytes.
     */
    private static byte[] encodeIpAsIpv6(String value) {
        try {
            byte[] addr = InetAddress.getByName(value).getAddress();
            if (addr.length == 16) {
                return addr;
            }
            byte[] mapped = new byte[16];
            mapped[10] = (byte) 0xff;
            mapped[11] = (byte) 0xff;
            System.arraycopy(addr, 0, mapped, 12, 4);
            return mapped;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    /**
     * Unsigned byte-array compare. {@code Arrays.compareUnsigned} is JDK 9+; this
     * project is on JDK 21+, so it's available, but we wrap for clarity.
     */
    private static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }
}
