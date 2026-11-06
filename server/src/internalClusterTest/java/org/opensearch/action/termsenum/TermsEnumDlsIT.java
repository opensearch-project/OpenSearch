/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Checks that terms_enum never returns a term that only documents hidden from the caller contain. No security plugin
 * runs here, so a reader wrapper simulates document-level security the way the security plugin implements it: it hides
 * documents through each segment's live docs, and only while the request's thread context carries its header.
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class TermsEnumDlsIT extends OpenSearchIntegTestCase {

    /** The request header that turns the simulated document-level security on. */
    static final String DLS_HEADER = "x-test-dls";

    /** A timeout no healthy shard comes close to, so a response is only incomplete when something is wrong. */
    private static final TimeValue GENEROUS_TIMEOUT = TimeValue.timeValueSeconds(30);

    /**
     * Hides every document whose {@code acl} is {@code hidden}, but only when the thread context that acquires the
     * searcher carries {@link #DLS_HEADER}. Like the security plugin, it does not hide anything once that context is
     * lost, so a request that stashes or drops its context fails these tests.
     */
    public static class HideDocsReaderWrapperPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(indexService -> reader -> {
                String dls = indexService.getThreadPool().getThreadContext().getHeader(DLS_HEADER);
                return dls == null ? reader : new HideAclHiddenDirectoryReader(reader);
            });
        }
    }

    /** Hides every field not starting with {@code public} in {@code fls-*} indices, the way field-level security does. */
    public static class HideFieldsPlugin extends Plugin implements MapperPlugin {
        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> index.startsWith("fls-") ? field -> field.startsWith("public") : MapperPlugin.NOOP_FIELD_PREDICATE;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(HideDocsReaderWrapperPlugin.class, HideFieldsPlugin.class);
    }

    public void testHiddenOnlyTermsAreNotReturned() {
        createDlsLogs();
        assertEquals(List.of("public-a", "shared"), terms(dlsClient(), new TermsEnumRequest("dls-logs").field("tag")));
        // without the header nothing is hidden, so it is the context-dependent wrapper that hides secret-x
        assertEquals(List.of("public-a", "secret-x", "shared"), terms(client(), new TermsEnumRequest("dls-logs").field("tag")));
    }

    public void testSearchAfterDoesNotRevealHiddenTerms() {
        createDlsLogs();
        List<String> paged = new ArrayList<>();
        String searchAfter = null;
        for (int page = 0;; page++) {
            assertThat("paging did not end", page, lessThanOrEqualTo(3));
            TermsEnumRequest request = new TermsEnumRequest("dls-logs").field("tag").size(1);
            if (searchAfter != null) {
                request.searchAfter(searchAfter);
            }
            List<String> terms = terms(dlsClient(), request);
            if (terms.isEmpty()) {
                break;
            }
            assertFalse(terms.contains("secret-x"));
            paged.addAll(terms);
            searchAfter = terms.get(terms.size() - 1);
        }
        assertEquals(List.of("public-a", "shared"), paged);
    }

    public void testConstantFieldsWhenAllDocsHidden() {
        assertAcked(
            prepareCreate("dls-const").setSettings(oneShard())
                .setMapping("{\"properties\":{\"team\":{\"type\":\"constant_keyword\",\"value\":\"red\"},\"acl\":{\"type\":\"keyword\"}}}")
        );
        ensureGreen("dls-const");
        client().prepareIndex("dls-const").setSource("acl", "hidden").get();
        client().prepareIndex("dls-const").setSource("acl", "hidden").get();
        refresh("dls-const");
        assertEquals(List.of(), terms(dlsClient(), new TermsEnumRequest("dls-const").field("team")));
        assertEquals(List.of(), terms(dlsClient(), new TermsEnumRequest("dls-const").field("_index")));
        assertEquals(List.of("red"), terms(client(), new TermsEnumRequest("dls-const").field("team")));

        client().prepareIndex("dls-const").setSource("acl", "visible").get();
        refresh("dls-const");
        assertEquals(List.of("red"), terms(dlsClient(), new TermsEnumRequest("dls-const").field("team")));
        assertEquals(List.of("dls-const"), terms(dlsClient(), new TermsEnumRequest("dls-const").field("_index")));
    }

    public void testSearchIdleShardKeepsContext() {
        assertAcked(
            prepareCreate("dls-idle").setSettings(Settings.builder().put(oneShard()).put("index.search.idle.after", TimeValue.ZERO))
                .setMapping("tag", "type=keyword", "acl", "type=keyword")
        );
        ensureGreen("dls-idle");
        client().prepareIndex("dls-idle").setSource("tag", "public-a", "acl", "visible").get();
        client().prepareIndex("dls-idle").setSource("tag", "secret-x", "acl", "hidden").get();
        IndexShard shard = internalCluster().getDataNodeInstance(IndicesService.class)
            .indexServiceSafe(resolveIndex("dls-idle"))
            .getShard(0);
        assertFalse(shard.scheduledRefresh());
        assertTrue(shard.hasRefreshPending());
        // the shard answers from the refresh listener's thread, which must still carry the header; the shard waits for
        // the next scheduled refresh, which the generous timeout of terms(...) leaves time for
        assertEquals(List.of("public-a"), terms(dlsClient(), new TermsEnumRequest("dls-idle").field("tag")));
    }

    public void testMetadataFieldIsExemptFromFieldFilter() {
        assertAcked(
            prepareCreate("fls-meta").setSettings(oneShard()).setMapping("public_tag", "type=keyword", "secret_tag", "type=keyword")
        );
        ensureGreen("fls-meta");
        client().prepareIndex("fls-meta").setSource("public_tag", "open", "secret_tag", "closed").get();
        refresh("fls-meta");
        Predicate<String> fieldFilter = internalCluster().getDataNodeInstance(IndicesService.class).getFieldFilter().apply("fls-meta");
        assertTrue(fieldFilter.test("public_tag"));
        assertFalse(fieldFilter.test("secret_tag"));
        assertFalse("the filter alone would hide _index", fieldFilter.test("_index"));

        assertEquals(List.of("open"), terms(client(), new TermsEnumRequest("fls-meta").field("public_tag")));
        // a hidden field is reported as absent: no terms, and the shard still counts as successful
        assertEquals(List.of(), terms(client(), new TermsEnumRequest("fls-meta").field("secret_tag")));
        // metadata fields are never hidden by the field filter, as in _field_caps
        assertEquals(List.of("fls-meta"), terms(client(), new TermsEnumRequest("fls-meta").field("_index")));
    }

    /** {@code dls-logs} on three shards: {@code public-a} visible, {@code secret-x} hidden, {@code shared} in both. */
    private void createDlsLogs() {
        assertAcked(
            prepareCreate("dls-logs").setSettings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0))
                .setMapping("tag", "type=keyword", "acl", "type=keyword")
        );
        ensureGreen("dls-logs");
        client().prepareIndex("dls-logs").setSource("tag", "public-a", "acl", "visible").get();
        client().prepareIndex("dls-logs").setSource("tag", "secret-x", "acl", "hidden").get();
        // same routing, so both are in one shard, where the hidden doc must not mask the visible one
        client().prepareIndex("dls-logs").setRouting("shared").setSource("tag", "shared", "acl", "hidden").get();
        client().prepareIndex("dls-logs").setRouting("shared").setSource("tag", "shared", "acl", "visible").get();
        refresh("dls-logs");
    }

    private static Settings oneShard() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    /** A client whose requests carry the header that turns the simulated document-level security on. */
    private static Client dlsClient() {
        return client().filterWithHeader(Map.of(DLS_HEADER, "on"));
    }

    /**
     * Runs the request with a generous timeout instead of the one second default, and returns its terms after checking
     * that every shard answered completely.
     */
    private static List<String> terms(Client client, TermsEnumRequest request) {
        TermsEnumResponse response = client.execute(TermsEnumAction.INSTANCE, request.timeout(GENEROUS_TIMEOUT)).actionGet();
        assertTrue(response.isComplete());
        assertEquals(0, response.getFailedShards());
        return response.getTerms();
    }

    /**
     * Removes every document whose {@code acl} is {@code hidden} from each segment's live docs and lowers
     * {@code numDocs()} to match, as a reader wrapper that follows the Lucene contract does.
     */
    private static final class HideAclHiddenDirectoryReader extends FilterDirectoryReader {
        HideAclHiddenDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new HideAclHiddenLeafReader(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new HideAclHiddenDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static final class HideAclHiddenLeafReader extends FilterLeafReader {
        private final FixedBitSet liveDocs;
        private final int numDocs;

        HideAclHiddenLeafReader(LeafReader in) throws IOException {
            super(in);
            FixedBitSet bits = new FixedBitSet(in.maxDoc());
            Bits inLiveDocs = in.getLiveDocs();
            for (int doc = 0; doc < in.maxDoc(); doc++) {
                if (inLiveDocs == null || inLiveDocs.get(doc)) {
                    bits.set(doc);
                }
            }
            PostingsEnum hidden = in.postings(new Term("acl", "hidden"), PostingsEnum.NONE);
            if (hidden != null) {
                for (int doc = hidden.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = hidden.nextDoc()) {
                    bits.clear(doc);
                }
            }
            this.liveDocs = bits;
            this.numDocs = bits.cardinality();
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
