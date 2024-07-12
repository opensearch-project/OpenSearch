/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.Rounding;
import org.opensearch.index.codec.composite.Composite99Codec;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;

/**
 * Star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class StarTreeDocValuesFormatTests extends BaseDocValuesFormatTestCase {
    @Override
    protected Codec getCodec() {
        MapperService service = Mockito.mock(MapperService.class);
        Mockito.when(service.getCompositeFieldTypes()).thenReturn(Set.of(getStarTreeFieldType()));
        final Logger testLogger = LogManager.getLogger(StarTreeDocValuesFormatTests.class);
        return new Composite99Codec(Lucene99Codec.Mode.BEST_SPEED, service, testLogger);
    }

    private StarTreeMapper.StarTreeFieldType getStarTreeFieldType() {
        List<MetricStat> m1 = new ArrayList<>();
        m1.add(MetricStat.MAX);
        Metric metric = new Metric("sndv", m1);
        List<Rounding.DateTimeUnit> d1CalendarIntervals = new ArrayList<>();
        d1CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        StarTreeField starTreeField = getStarTreeField(d1CalendarIntervals, metric);

        return new StarTreeMapper.StarTreeFieldType("star_tree", starTreeField);
    }

    private static StarTreeField getStarTreeField(List<Rounding.DateTimeUnit> d1CalendarIntervals, Metric metric1) {
        DateDimension d1 = new DateDimension("field", d1CalendarIntervals);
        NumericDimension d2 = new NumericDimension("dv");

        List<Metric> metrics = List.of(metric1);
        List<Dimension> dims = List.of(d1, d2);
        StarTreeFieldConfiguration config = new StarTreeFieldConfiguration(
            100,
            Collections.emptySet(),
            StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP
        );

        return new StarTreeField("starTree", dims, metrics, config);
    }

    public void testStarTreeDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", 1));
        iw.addDocument(doc);
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", 2));
        iw.addDocument(doc);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", 2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        // TODO : validate star tree structures that got created
        directory.close();
    }
}
