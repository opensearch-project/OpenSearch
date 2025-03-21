/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.translog;

import org.opensearch.OpenSearchException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class SnapshotMatchers {
    private SnapshotMatchers() {

    }

    /**
     * Consumes a snapshot and make sure it's size is as expected
     */
    public static Matcher<Translog.Snapshot> size(int size) {
        return new SizeMatcher(size);
    }

    /**
     * Consumes a snapshot and make sure it's content is as expected
     */
    public static Matcher<Translog.Snapshot> equalsTo(Translog.Operation... ops) {
        return new EqualMatcher(ops);
    }

    /**
     * Consumes a snapshot and make sure it's content is as expected
     */
    public static Matcher<Translog.Snapshot> equalsTo(List<Translog.Operation> ops) {
        return new EqualMatcher(ops.toArray(new Translog.Operation[0]));
    }

    public static Matcher<Translog.Snapshot> containsOperationsInAnyOrder(Collection<Translog.Operation> expectedOperations) {
        return new ContainingInAnyOrderMatcher(expectedOperations);
    }

    /**
     * Consumes a snapshot and makes sure that its operations have all seqno between minSeqNo(inclusive) and maxSeqNo(inclusive).
     */
    public static Matcher<Translog.Snapshot> containsSeqNoRange(long minSeqNo, long maxSeqNo) {
        return new ContainingSeqNoRangeMatcher(minSeqNo, maxSeqNo);
    }

    public static class SizeMatcher extends TypeSafeMatcher<Translog.Snapshot> {

        private final int size;

        public SizeMatcher(int size) {
            this.size = size;
        }

        @Override
        public boolean matchesSafely(Translog.Snapshot snapshot) {
            int count = 0;
            try {
                while (snapshot.next() != null) {
                    count++;
                }
            } catch (IOException ex) {
                throw new OpenSearchException("failed to advance snapshot", ex);
            }
            return size == count;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a snapshot with size ").appendValue(size);
        }
    }

    public static class EqualMatcher extends TypeSafeMatcher<Translog.Snapshot> {

        private final Translog.Operation[] expectedOps;
        String failureMsg = null;

        public EqualMatcher(Translog.Operation[] expectedOps) {
            this.expectedOps = expectedOps;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                Translog.Operation op;
                int i;
                for (i = 0, op = snapshot.next(); op != null && i < expectedOps.length; i++, op = snapshot.next()) {
                    if (expectedOps[i].equals(op) == false) {
                        failureMsg = "position [" + i + "] expected [" + expectedOps[i] + "] but found [" + op + "]";
                        return false;
                    }
                }

                if (i < expectedOps.length) {
                    failureMsg = "expected [" + expectedOps.length + "] ops but only found [" + i + "]";
                    return false;
                }

                if (op != null) {
                    int count = 1; // to account for the op we already read
                    while (snapshot.next() != null) {
                        count++;
                    }
                    failureMsg = "expected [" + expectedOps.length + "] ops but got [" + (expectedOps.length + count) + "]";
                    return false;
                }
                return true;
            } catch (IOException ex) {
                throw new OpenSearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(failureMsg);
        }
    }

    public static class ContainingInAnyOrderMatcher extends TypeSafeMatcher<Translog.Snapshot> {
        private final Collection<Translog.Operation> expectedOps;
        private List<Translog.Operation> notFoundOps;
        private List<Translog.Operation> notExpectedOps;

        static List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
            final List<Translog.Operation> actualOps = new ArrayList<>();
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                actualOps.add(op);
            }
            return actualOps;
        }

        public ContainingInAnyOrderMatcher(Collection<Translog.Operation> expectedOps) {
            this.expectedOps = expectedOps;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                List<Translog.Operation> actualOps = drainAll(snapshot);
                notFoundOps = expectedOps.stream().filter(o -> actualOps.contains(o) == false).collect(Collectors.toList());
                notExpectedOps = actualOps.stream().filter(o -> expectedOps.contains(o) == false).collect(Collectors.toList());
                return notFoundOps.isEmpty() && notExpectedOps.isEmpty();
            } catch (IOException ex) {
                throw new OpenSearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        protected void describeMismatchSafely(Translog.Snapshot snapshot, Description mismatchDescription) {
            if (notFoundOps.isEmpty() == false) {
                mismatchDescription.appendText("not found ").appendValueList("[", ", ", "]", notFoundOps);
            }
            if (notExpectedOps.isEmpty() == false) {
                if (notFoundOps.isEmpty() == false) {
                    mismatchDescription.appendText("; ");
                }
                mismatchDescription.appendText("not expected ").appendValueList("[", ", ", "]", notExpectedOps);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("snapshot contains ").appendValueList("[", ", ", "]", expectedOps).appendText(" in any order.");
        }
    }

    static class ContainingSeqNoRangeMatcher extends TypeSafeMatcher<Translog.Snapshot> {
        private final long minSeqNo;
        private final long maxSeqNo;
        private final List<Long> notFoundSeqNo = new ArrayList<>();

        ContainingSeqNoRangeMatcher(long minSeqNo, long maxSeqNo) {
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                final Set<Long> seqNoList = new HashSet<>();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    seqNoList.add(op.seqNo());
                }
                for (long i = minSeqNo; i <= maxSeqNo; i++) {
                    if (seqNoList.contains(i) == false) {
                        notFoundSeqNo.add(i);
                    }
                }
                return notFoundSeqNo.isEmpty();
            } catch (IOException ex) {
                throw new OpenSearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        protected void describeMismatchSafely(Translog.Snapshot snapshot, Description mismatchDescription) {
            mismatchDescription.appendText("not found seqno ").appendValueList("[", ", ", "]", notFoundSeqNo);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("snapshot contains all seqno from [" + minSeqNo + " to " + maxSeqNo + "]");
        }
    }
}
