/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.codec;

import java.io.IOException;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;


/** Custom doc values format for star tree codec */
public class StarTreeDocValuesFormat extends DocValuesFormat {
    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     */
    private final DocValuesFormat delegate;

    public StarTreeDocValuesFormat() {
        this(new Lucene90DocValuesFormat());
    }

    public StarTreeDocValuesFormat(DocValuesFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state)
        throws IOException {
        return new StarTreeDocValuesWriter(delegate.fieldsConsumer(state), state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state)
        throws IOException {
        return new StarTreeDocValuesReader(delegate.fieldsProducer(state), state);
    }
}
