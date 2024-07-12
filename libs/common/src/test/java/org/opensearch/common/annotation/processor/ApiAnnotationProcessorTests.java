/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.test.OpenSearchTestCase;

import javax.tools.Diagnostic;

import static org.opensearch.common.annotation.processor.CompilerSupport.HasDiagnostic.matching;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@SuppressWarnings("deprecation")
public class ApiAnnotationProcessorTests extends OpenSearchTestCase implements CompilerSupport {
    public void testPublicApiMethodArgumentNotAnnotated() {
        final CompilerResult result = compile("PublicApiMethodArgumentNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodArgumentNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentNotAnnotatedGenerics() {
        final CompilerResult result = compile("PublicApiMethodArgumentNotAnnotatedGenerics.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodThrowsNotAnnotated() {
        final CompilerResult result = compile("PublicApiMethodThrowsNotAnnotated.java", "PublicApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotatedException is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodThrowsNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentNotAnnotatedPackagePrivate() {
        final CompilerResult result = compile("PublicApiMethodArgumentNotAnnotatedPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(4));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotatedPackagePrivate is part of the public APIs but does not have public or protected visibility "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedPackagePrivate)"
                    )
                )
            )
        );

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotatedPackagePrivate is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedPackagePrivate)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentAnnotatedPackagePrivate() {
        final CompilerResult result = compile("PublicApiMethodArgumentAnnotatedPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.AnnotatedPackagePrivate is part of the public APIs but does not have public or protected visibility "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodArgumentAnnotatedPackagePrivate)"
                    )
                )
            )
        );
    }

    public void testPublicApiWithInternalApiMethod() {
        final CompilerResult result = compile("PublicApiWithInternalApiMethod.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element method() is part of the public APIs but is marked as @InternalApi (referenced by org.opensearch.common.annotation.processor.PublicApiWithInternalApiMethod)"
                    )
                )
            )
        );
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorArgumentNotAnnotated() {
        final CompilerResult result = compile("PublicApiConstructorArgumentNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorArgumentAnnotatedInternalApi() {
        final CompilerResult result = compile("PublicApiConstructorArgumentAnnotatedInternalApi.java", "InternalApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiWithExperimentalApiMethod() {
        final CompilerResult result = compile("PublicApiWithExperimentalApiMethod.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodReturnNotAnnotated() {
        final CompilerResult result = compile("PublicApiMethodReturnNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedGenerics() {
        final CompilerResult result = compile("PublicApiMethodReturnNotAnnotatedGenerics.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnNotAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedArray() {
        final CompilerResult result = compile("PublicApiMethodReturnNotAnnotatedArray.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnNotAnnotatedArray)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedBoundedGenerics() {
        final CompilerResult result = compile("PublicApiMethodReturnNotAnnotatedBoundedGenerics.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnNotAnnotatedBoundedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedAnnotation() {
        final CompilerResult result = compile(
            "PublicApiMethodReturnNotAnnotatedAnnotation.java",
            "PublicApiAnnotated.java",
            "NotAnnotatedAnnotation.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotatedAnnotation is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnNotAnnotatedAnnotation)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedWildcardGenerics() {
        final CompilerResult result = compile("PublicApiMethodReturnNotAnnotatedWildcardGenerics.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiWithPackagePrivateMethod() {
        final CompilerResult result = compile("PublicApiWithPackagePrivateMethod.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodReturnSelf() {
        final CompilerResult result = compile("PublicApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testExperimentalApiMethodReturnSelf() {
        final CompilerResult result = compile("ExperimentalApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testDeprecatedApiMethodReturnSelf() {
        final CompilerResult result = compile("DeprecatedApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiPackagePrivate() {
        final CompilerResult result = compile("PublicApiPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.PublicApiPackagePrivate is part of the public APIs but does not have public or protected visibility"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodGenericsArgumentNotAnnotated() {
        final CompilerResult result = compile("PublicApiMethodGenericsArgumentNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodGenericsArgumentNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnAnnotatedArray() {
        final CompilerResult result = compile("PublicApiMethodReturnAnnotatedArray.java", "PublicApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodGenericsArgumentAnnotated() {
        final CompilerResult result = compile("PublicApiMethodGenericsArgumentAnnotated.java", "PublicApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiAnnotatedNotOpensearch() {
        final CompilerResult result = compileWithPackage("org.acme", "PublicApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The type org.acme.PublicApiAnnotated is not residing in org.opensearch.* package and should not be annotated as OpenSearch APIs."
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnAnnotatedGenerics() {
        final CompilerResult result = compile(
            "PublicApiMethodReturnAnnotatedGenerics.java",
            "PublicApiAnnotated.java",
            "NotAnnotatedAnnotation.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.opensearch.common.annotation.processor.NotAnnotatedAnnotation is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.opensearch.common.annotation.processor.PublicApiMethodReturnAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    /**
     * The type could expose protected inner types which are still considered to be a public API when used
     */
    public void testPublicApiWithProtectedInterface() {
        final CompilerResult result = compile("PublicApiWithProtectedInterface.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorAnnotatedInternalApi() {
        final CompilerResult result = compile("PublicApiConstructorAnnotatedInternalApi.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(2));

        assertThat(failure.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }
}
