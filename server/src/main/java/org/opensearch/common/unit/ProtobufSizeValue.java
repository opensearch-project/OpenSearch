/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.unit;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;

/**
 * Conversion values.
*
* @opensearch.internal
*/
public class ProtobufSizeValue implements ProtobufWriteable {

    private final long size;
    private final SizeUnit sizeUnit;

    public ProtobufSizeValue(long singles) {
        this(singles, SizeUnit.SINGLE);
    }

    public ProtobufSizeValue(long size, SizeUnit sizeUnit) {
        if (size < 0) {
            throw new IllegalArgumentException("size in SizeValue may not be negative");
        }
        this.size = size;
        this.sizeUnit = sizeUnit;
    }

    public ProtobufSizeValue(CodedInputStream in) throws IOException {
        size = in.readInt64();
        sizeUnit = SizeUnit.SINGLE;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(singles());
    }

    public long singles() {
        return sizeUnit.toSingles(size);
    }

    public long getSingles() {
        return singles();
    }

    public long kilo() {
        return sizeUnit.toKilo(size);
    }

    public long getKilo() {
        return kilo();
    }

    public long mega() {
        return sizeUnit.toMega(size);
    }

    public long getMega() {
        return mega();
    }

    public long giga() {
        return sizeUnit.toGiga(size);
    }

    public long getGiga() {
        return giga();
    }

    public long tera() {
        return sizeUnit.toTera(size);
    }

    public long getTera() {
        return tera();
    }

    public long peta() {
        return sizeUnit.toPeta(size);
    }

    public long getPeta() {
        return peta();
    }

    public double kiloFrac() {
        return ((double) singles()) / SizeUnit.C1;
    }

    public double getKiloFrac() {
        return kiloFrac();
    }

    public double megaFrac() {
        return ((double) singles()) / SizeUnit.C2;
    }

    public double getMegaFrac() {
        return megaFrac();
    }

    public double gigaFrac() {
        return ((double) singles()) / SizeUnit.C3;
    }

    public double getGigaFrac() {
        return gigaFrac();
    }

    public double teraFrac() {
        return ((double) singles()) / SizeUnit.C4;
    }

    public double getTeraFrac() {
        return teraFrac();
    }

    public double petaFrac() {
        return ((double) singles()) / SizeUnit.C5;
    }

    public double getPetaFrac() {
        return petaFrac();
    }
}
