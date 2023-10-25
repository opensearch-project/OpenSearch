/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.Member;

/**
 * Creates stack trace elements for members.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
public class StackTraceElements {

    public static Object forMember(final Member member) {
        if (member == null) {
            return SourceProvider.UNKNOWN_SOURCE;
        }

        final Class declaringClass = member.getDeclaringClass();
        final int lineNumber = -1;

        final Class<? extends Member> memberType = MoreTypes.memberType(member);
        final String memberName = memberType == Constructor.class ? "<init>" : member.getName();
        return new StackTraceElement(declaringClass.getName(), memberName, null, lineNumber);
    }

    public static Object forType(final Class<?> implementation) {
        final String fileName = null;
        final int lineNumber = -1;

        return new StackTraceElement(implementation.getName(), "class", fileName, lineNumber);
    }
}
