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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Base64;
import java.util.List;

/**
 * Converts {@code BINARY(varchar_literal)} into a VARBINARY literal whose bytes
 * match the on-disk encoding of {@code ip} / {@code binary} fields.
 *
 * <p>The SQL plugin emits {@code BINARY(varchar)} whenever a VARCHAR literal is
 * compared to a VARBINARY column (see ExtendedRexBuilder.makeCast in the sql repo).
 * This adapter resolves the placeholder so DataFusion's native byte-comparison
 * operators can run against the parquet column.
 *
 * <p>Disambiguation between {@code ip} and {@code binary} encoding is by the
 * literal's text shape: try IP first, fall back to base64. IP literals contain
 * {@code .} or {@code :} which are invalid base64; base64 literals don't contain
 * those characters so they fail {@code InetAddress.getByName} for IPv4/IPv6 forms.
 *
 * <p>Output is a {@code SqlTypeName.VARBINARY} literal — not BINARY. Calcite's
 * {@code makeBinaryLiteral} produces BINARY with a fixed precision which isthmus
 * serializes as Substrait FixedBinary(N), mismatching the parquet column's
 * variable-length Binary type.
 *
 * <p>TODO: Frontends (SQL plugin / PPL parser) should hand the analytics core a
 * plan whose literals are already in the correct on-disk byte form, with the
 * field-type distinction (ip vs binary) preserved end-to-end. This adapter
 * exists today because OpenSearchSchemaBuilder collapses both ip and binary
 * fields to plain VARBINARY before the plan reaches the SQL plugin's coercion
 * layer, so the encoding decision can't be made there. The adapter recovers
 * the necessary context here in the analytics backend (via FieldStorageInfo)
 * and rewrites the placeholder accordingly.
 *
 * @opensearch.internal
 */
class BinaryFunctionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1
            || !(original.getOperands().get(0) instanceof RexLiteral lit)
            || lit.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return original;
        }
        String value = lit.getValueAs(String.class);
        if (value == null) {
            return original;
        }

        byte[] bytes = encodeAsIp(value);
        if (bytes == null) {
            bytes = encodeAsBinary(value);
        }
        if (bytes == null) {
            throw new IllegalArgumentException(
                "BINARY operand '" + value + "' is neither a valid IP address nor a valid base64-encoded binary value"
            );
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varbinary = cluster.getTypeFactory()
            .createTypeWithNullability(cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY), true);
        return rexBuilder.makeAbstractCast(varbinary, rexBuilder.makeLiteral(new ByteString(bytes), varbinary, false), false);
    }

    /**
     * IPv6-mapped 16-byte encoding matching Lucene's {@code InetAddressPoint.encode} —
     * the same encoding the parquet writer uses for {@code ip} fields. IPv4 is encoded
     * as 10 zero bytes + {@code 0xff 0xff} + 4 IPv4 bytes (RFC 4291 §2.5.5.2).
     * IPv6 is the raw 16 bytes.
     */
    private static byte[] encodeAsIp(String value) {
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

    private static byte[] encodeAsBinary(String value) {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
