/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getDimensionIterators;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getMetricIterators;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.validateStarTree;

public class StarTreeBuilderSortAndAggregateTests extends StarTreeBuilderTestCase {

    public StarTreeBuilderSortAndAggregateTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        super(buildMode);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP },
            new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP }
        );
    }

    public void test_sortAndAggregateStarTreeDocuments() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 12.0, 10.0, randomDouble(), 8.0, 20.0, 10L }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 10.0, 6.0, randomDouble(), 12.0, 10.0, 10L }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 14.0, 12.0, randomDouble(), 6.0, 24.0, 10L }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 9.0, 4.0, randomDouble(), 9.0, 12.0, null }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 11.0, 16.0, randomDouble(), 8.0, 13.0, null }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            Long metric6 = (Long) starTreeDocuments[i].metrics[5];
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5, metric6 }
            );
        }
        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 11L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0, 21L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);
    }

    public void test_sortAndAggregateStarTreeDocument_DoubleMaxAndDoubleMinMetrics() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { Double.MAX_VALUE, 10.0, randomDouble(), 8.0, 20.0, 100L }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 10.0, 6.0, randomDouble(), 12.0, 10.0, 100L }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 14.0, Double.MIN_VALUE, randomDouble(), 6.0, 24.0, 100L }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 9.0, 4.0, randomDouble(), 9.0, 12.0, 100L }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 11.0, 16.0, randomDouble(), 8.0, 13.0, 100L }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { Double.MAX_VALUE + 9, 14.0, 2L, 8.0, 20.0, 200L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, Double.MIN_VALUE + 22, 3L, 6.0, 24.0, 300L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            Long metric6 = (Long) starTreeDocuments[i].metrics[5];
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5, metric6 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 3, 1, builder.getStarTreeDocuments());

    }

    public void test_sortAndAggregateStarTreeDocument_longMaxAndLongMinDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Object[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Object[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Object[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Object[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Object[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5, null }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);
    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetric() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 11.0, null, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 18.0, 3L, 6.0, 24.0, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, null }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }
    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetricField() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 12.0, null, randomDouble(), 8.0, 20.0, null }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 10.0, null, randomDouble(), 12.0, 10.0, null }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 14.0, null, randomDouble(), 6.0, 24.0, null }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 9.0, null, randomDouble(), 9.0, 12.0, 10L }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Object[] { 11.0, null, randomDouble(), 8.0, 13.0, null }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 0.0, 2L, 8.0, 20.0, 11L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 0.0, 3L, 6.0, 24.0, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            Long metric6 = starTreeDocuments[i].metrics[5] != null ? (Long) starTreeDocuments[i].metrics[5] : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, metric6 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }
    }

    public void test_sortAndAggregateStarTreeDocuments_nullAndMinusOneInDimensionField() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, null, 3L, 4L },
            new Object[] { 12.0, null, randomDouble(), 8.0, 20.0, null }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { null, 4L, 2L, 1L },
            new Object[] { 10.0, null, randomDouble(), 12.0, 10.0, null }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, 4L, 2L, 1L },
            new Object[] { 14.0, null, randomDouble(), 6.0, 24.0, null }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, null, 3L, 4L },
            new Object[] { 9.0, null, randomDouble(), 9.0, 12.0, 10L }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { -1L, 4L, 2L, 1L },
            new Object[] { 11.0, null, randomDouble(), 8.0, 13.0, null }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { -1L, 4L, 2L, 1L }, new Object[] { 11.0, 0.0, 1L, 8.0, 13.0, 1L }),
            new StarTreeDocument(new Long[] { 2L, null, 3L, 4L }, new Object[] { 21.0, 0.0, 2L, 8.0, 20.0, 11L }),
            new StarTreeDocument(new Long[] { null, 4L, 2L, 1L }, new Object[] { 24.0, 0.0, 2L, 6.0, 24.0, 2L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            Long metric6 = starTreeDocuments[i].metrics[5] != null ? (long) starTreeDocuments[i].metrics[5] : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, metric6 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }

        assertFalse(expectedStarTreeDocumentIterator.hasNext());

        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_nullDimensionsAndNullMetrics() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, null, null, null }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, null, null, null }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, null, null, null }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, null, null, null }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, null, null, null }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { 0.0, 0.0, 0L, null, null, 5L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = starTreeDocuments[i].metrics[0] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0])
                : null;
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = starTreeDocuments[i].metrics[2] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2])
                : null;
            Long metric4 = starTreeDocuments[i].metrics[3] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3])
                : null;
            Long metric5 = starTreeDocuments[i].metrics[4] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4])
                : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, null }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_nullDimensionsAndFewNullMetrics() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        double sumValue = randomDouble();
        double minValue = randomDouble();
        double maxValue = randomDouble();

        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, randomDouble(), null, maxValue }
        );
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { null, null, null, null, null });
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { null, null, null, minValue, null }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { null, null, null, null, null });
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { sumValue, null, randomDouble(), null, null }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { sumValue, 0.0, 2L, minValue, maxValue, 5L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = starTreeDocuments[i].metrics[0] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0])
                : null;
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = starTreeDocuments[i].metrics[2] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2])
                : null;
            Long metric4 = starTreeDocuments[i].metrics[3] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3])
                : null;
            Long metric5 = starTreeDocuments[i].metrics[4] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4])
                : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, null }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_emptyDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { 12.0, null, randomDouble(), 8.0, 20.0, 10L }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { 10.0, null, randomDouble(), 12.0, 10.0, 10L }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { 14.0, null, randomDouble(), 6.0, 24.0, 10L }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { 9.0, null, randomDouble(), 9.0, 12.0, 10L }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Object[] { 11.0, null, randomDouble(), 8.0, 13.0, 10L }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { 56.0, 0.0, 5L, 6.0, 24.0, 50L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            Long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            Long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            Long metric6 = (Long) starTreeDocuments[i].metrics[5];
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5, metric6 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
            assertEquals(expectedStarTreeDocument.metrics[5], resultStarTreeDocument.metrics[5]);
        }
    }
}
