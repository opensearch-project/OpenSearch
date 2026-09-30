/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ExitableTermsTests extends OpenSearchTestCase {

    /** Ordinary term enumeration must forward the native reuse candidate and flags while creating a fresh cancellation wrapper. */
    public void testIteratorReusesPostings() throws IOException {
        assertPostingsReuse(false);
    }

    /** Automaton-based term expansion must preserve the same postings reuse contract as ordinary iteration. */
    public void testIntersectReusesPostings() throws IOException {
        assertPostingsReuse(true);
    }

    private void assertPostingsReuse(boolean intersect) throws IOException {
        Terms terms = mock(Terms.class);
        TermsEnum delegate = mock(TermsEnum.class);
        PostingsEnum codec = mock(PostingsEnum.class);
        ExitableDirectoryReader.QueryCancellation cancellation = mock(ExitableDirectoryReader.QueryCancellation.class);
        ExitableDirectoryReader.ExitableTerms wrapped = new ExitableDirectoryReader.ExitableTerms(terms, cancellation);
        final TermsEnum iterator;
        if (intersect) {
            CompiledAutomaton automaton = new CompiledAutomaton(new RegExp("a.*").toAutomaton());
            BytesRef startTerm = new BytesRef("ab");
            when(terms.intersect(automaton, startTerm)).thenReturn(delegate);
            iterator = wrapped.intersect(automaton, startTerm);
        } else {
            when(terms.iterator()).thenReturn(delegate);
            iterator = wrapped.iterator();
        }

        when(delegate.postings(null, PostingsEnum.NONE)).thenReturn(codec);
        PostingsEnum first = iterator.postings(null, PostingsEnum.NONE);
        assertSame(codec, ((FilterLeafReader.FilterPostingsEnum) first).unwrap());

        when(delegate.postings(codec, PostingsEnum.FREQS)).thenReturn(codec);
        PostingsEnum second = iterator.postings(first, PostingsEnum.FREQS);
        assertNotSame(first, second);
        assertSame(codec, ((FilterLeafReader.FilterPostingsEnum) second).unwrap());
        verify(delegate).postings(null, PostingsEnum.NONE);
        verify(delegate).postings(codec, PostingsEnum.FREQS);
    }

    /** Reuse is optional for the codec; the cancellation wrapper must accept a replacement postings iterator. */
    public void testCodecCanRejectReuse() throws IOException {
        TermsEnum delegate = mock(TermsEnum.class);
        PostingsEnum oldCodec = mock(PostingsEnum.class);
        PostingsEnum newCodec = mock(PostingsEnum.class);
        ExitableDirectoryReader.QueryCancellation cancellation = mock(ExitableDirectoryReader.QueryCancellation.class);
        TermsEnum iterator = wrap(delegate, cancellation);
        PostingsEnum reuse = new ExitableDirectoryReader.ExitablePostingsEnum(oldCodec, cancellation);

        when(delegate.postings(oldCodec, PostingsEnum.POSITIONS)).thenReturn(newCodec);
        PostingsEnum result = iterator.postings(reuse, PostingsEnum.POSITIONS);
        assertSame(newCodec, ((FilterLeafReader.FilterPostingsEnum) result).unwrap());
        verify(delegate).postings(oldCodec, PostingsEnum.POSITIONS);
    }

    /** Only our cancellation wrapper should be unwrapped; the delegate decides how to handle other reuse candidates. */
    public void testOtherReuseIsPassedThrough() throws IOException {
        PostingsEnum codec = mock(PostingsEnum.class);
        PostingsEnum otherWrapper = new FilterLeafReader.FilterPostingsEnum(codec) {
        };
        for (PostingsEnum reuse : new PostingsEnum[] { codec, otherWrapper }) {
            TermsEnum delegate = mock(TermsEnum.class);
            PostingsEnum resultCodec = mock(PostingsEnum.class);
            TermsEnum iterator = wrap(delegate, mock(ExitableDirectoryReader.QueryCancellation.class));
            when(delegate.postings(reuse, PostingsEnum.NONE)).thenReturn(resultCodec);

            PostingsEnum result = iterator.postings(reuse, PostingsEnum.NONE);
            assertSame(resultCodec, ((FilterLeafReader.FilterPostingsEnum) result).unwrap());
            verify(delegate).postings(reuse, PostingsEnum.NONE);
        }
    }

    /** Cancellation belongs to the current query even when its codec postings were reused from another query. */
    public void testReusedPostingsUseCurrentCancellation() throws IOException {
        TermsEnum delegate = mock(TermsEnum.class);
        PostingsEnum codec = mock(PostingsEnum.class);
        ExitableDirectoryReader.QueryCancellation oldCancellation = mock(ExitableDirectoryReader.QueryCancellation.class);
        ExitableDirectoryReader.QueryCancellation newCancellation = mock(ExitableDirectoryReader.QueryCancellation.class);
        PostingsEnum reuse = new ExitableDirectoryReader.ExitablePostingsEnum(codec, oldCancellation);
        TermsEnum iterator = wrap(delegate, newCancellation);
        when(delegate.postings(codec, PostingsEnum.NONE)).thenReturn(codec);
        PostingsEnum result = iterator.postings(reuse, PostingsEnum.NONE);

        doThrow(new TaskCancelledException("current query cancelled")).when(newCancellation).checkCancelled();
        TaskCancelledException exception = expectThrows(TaskCancelledException.class, result::nextDoc);
        assertEquals("current query cancelled", exception.getMessage());
        verify(newCancellation, times(2)).checkCancelled(); // TermsEnum creation and the first postings operation.
        verifyNoInteractions(oldCancellation);
        verify(codec, never()).nextDoc();
    }

    private TermsEnum wrap(TermsEnum delegate, ExitableDirectoryReader.QueryCancellation cancellation) throws IOException {
        Terms terms = mock(Terms.class);
        when(terms.iterator()).thenReturn(delegate);
        return new ExitableDirectoryReader.ExitableTerms(terms, cancellation).iterator();
    }
}
