# Developer Guide

So you want to contribute code to OpenSearch?  Excellent!  We're glad you're here.  Here's what you need to do:

## Importing the project into IntelliJ IDEA directly

OpenSearch builds using Java 14. When importing into IntelliJ you will need
to define an appropriate SDK. The convention is that **this SDK should be named
"14"** so that the project import will detect it automatically. For more details
on defining an SDK in IntelliJ please refer to [their documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk).
SDK definitions are global, so you can add the JDK from any project, or after
project import. Importing with a missing JDK will still work, IntelliJ will
simply report a problem and will refuse to build until resolved.

You can import the OpenSearch project into IntelliJ IDEA via:

 - Select **File > Open**
 - In the subsequent dialog navigate to the root `build.gradle` file
 - In the subsequent dialog select **Open as Project**

## Git clone OpenSearch repo:
 git clone https://github.com/opensearch-project/OpenSearch.git


## Project layout

This repository is split into many top level directories. The most important
ones are:

### `docs`
Documentation for the project.

### `distribution`
Builds our tar and zip archives and our rpm and deb packages.

### `libs`
Libraries used to build other parts of the project. These are meant to be
internal rather than general purpose. We have no plans to
[semver](https://semver.org/) their APIs or accept feature requests for them.
We publish them to maven central because they are dependencies of our plugin
test framework, high level rest client, and jdbc driver but they really aren't
general purpose enough to *belong* in maven central. We're still working out
what to do here.

#### `modules`
Features that are shipped with OpenSearch by default but are not built in to
the server. We typically separate features from the server because they require
permissions that we don't believe *all* of OpenSearch should have or because
they depend on libraries that we don't believe *all* of OpenSearch should
depend on.

For example, reindex requires the `connect` permission so it can perform
reindex-from-remote but we don't believe that the *all* of OpenSearch should
have the "connect". For another example, Painless is implemented using antlr4
and asm and we don't believe that *all* of OpenSearch should have access to
them.

#### `plugins`
Officially supported plugins to OpenSearch. We decide that a feature should
be a plugin rather than shipped as a module because we feel that it is only
important to a subset of users, especially if it requires extra dependencies.

The canonical example of this is the ICU analysis plugin. It is important for
folks who want the fairly language neutral ICU analyzer but the library to
implement the analyzer is 11MB so we don't ship it with OpenSearch by
default.

Another example is the `discovery-gce` plugin. It is *vital* to folks running
in [GCP](https://cloud.google.com/) but useless otherwise and it depends on a
dozen extra jars.

## Project Tools

JDK 14 is required to build OpenSearch. You must have a JDK 14 installed
with the environment variable `JAVA_HOME` referencing the path to Java home for
your JDK 14 installation. By default, tests use the same runtime as `JAVA_HOME`.
However, since RENNAMEME supports JDK 8, the build supports compiling with
JDK 14 and testing on a JDK 8 runtime; to do this, set `RUNTIME_JAVA_HOME`
pointing to the Java home of a JDK 8 installation. Note that this mechanism can
be used to test against other JDKs as well, this is not only limited to JDK 8.

> Note: It is also required to have `JAVA8_HOME`, `JAVA9_HOME`, `JAVA10_HOME`
and `JAVA11_HOME`, and `JAVA12_HOME` available so that the tests can pass.

OpenSearch uses the Gradle wrapper for its build. You can execute Gradle
using the wrapper via the `gradlew` script on Unix systems or `gradlew.bat`
script on Windows in the root of the repository. The examples below show the
usage on Unix.

We support development in IntelliJ versions IntelliJ 2019.2 and
onwards. We would like to support Eclipse, but few of us use it and has fallen
into [disrepair][eclipse].

[Docker](https://docs.docker.com/install/) is required for building some OpenSearch artifacts and executing certain test suites. You can run OpenSearch without building all the artifacts with:

    ./gradlew :run

That'll spend a while building OpenSearch and then it'll start OpenSearch,
writing its log above Gradle's status message. We log a lot of stuff on startup,
specifically these lines tell you that OpenSearch is ready:

    [2020-05-29T14:50:35,167][INFO ][o.e.h.AbstractHttpServerTransport] [runTask-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
    [2020-05-29T14:50:35,169][INFO ][o.e.n.Node               ] [runTask-0] started

But to be honest its typically easier to wait until the console stops scrolling
and then run `curl` in another window like this to check if OpenSearch instance is running:

    curl -u opensearch:password localhost:9200


## Java Language Formatting Guidelines

Java files in the OpenSearch codebase are formatted with the Eclipse JDT
formatter, using the [Spotless
Gradle](https://github.com/diffplug/spotless/tree/master/plugin-gradle)
plugin. This plugin is configured on a project-by-project basis, via
`build.gradle` in the root of the repository. So long as at least one
project is configured, the formatting check can be run explicitly with:

    ./gradlew spotlessJavaCheck

The code can be formatted with:

    ./gradlew spotlessApply

These tasks can also be run for specific subprojects, e.g.

    ./gradlew server:spotlessJavaCheck

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* Lines of code surrounded by `// tag::NAME` and `// end::NAME` comments are included
  in the documentation and should only be 76 characters wide not counting
  leading indentation. Such regions of code are not formatted automatically as
  it is not possible to change the line length rule of the formatter for
  part of a file. Please format such sections sympathetically with the rest
  of the code, while keeping lines to maximum length of 76 characters.
* Wildcard imports (`import foo.bar.baz.*`) are forbidden and will cause
  the build to fail.
* If *absolutely* necessary, you can disable formatting for regions of code
  with the `// tag::NAME` and `// end::NAME` directives, but note that
  these are intended for use in documentation, so please make it clear what
  you have done, and only do this where the benefit clearly outweighs the
  decrease in consistency.
* Note that JavaDoc and block comments i.e. `/* ... */` are not formatted,
  but line comments i.e `// ...` are.
* There is an implicit rule that negative boolean expressions should use
  the form `foo == false` instead of `!foo` for better readability of the
  code. While this isn't strictly enforced, if might get called out in PR
  reviews as something to change.

## Editor / IDE Support

IntelliJ IDEs can
[import](https://blog.jetbrains.com/idea/2014/01/intellij-idea-13-importing-code-formatter-settings-from-eclipse/)
the same settings file, and / or use the [Eclipse Code
Formatter](https://plugins.jetbrains.com/plugin/6546-eclipse-code-formatter)
plugin.

You can also tell Spotless to [format a specific
file](https://github.com/diffplug/spotless/tree/master/plugin-gradle#can-i-apply-spotless-to-specific-files)
from the command line.

## Formatting failures

Sometimes Spotless will report a "misbehaving rule which can't make up its
mind" and will recommend enabling the `paddedCell()` setting. If you
enabled this settings and run the format check again,
Spotless will write files to
`$PROJECT/build/spotless-diagnose-java/` to aid diagnosis. It writes
different copies of the formatted files, so that you can see how they
differ and infer what is the problem.

The `paddedCell()` option is disabled for normal operation in order to
detect any misbehaviour. You can enabled the option from the command line
by running Gradle with `-Dspotless.paddedcell`.


> **NOTE:** If you have imported the project into IntelliJ IDEA the project will
> be automatically configured to add the correct license header to new source
> files based on the source location.



## Running The Full Test Suite

**Note:  OpenSearch hasn't made any changes to the test suite yet beyond fixing tests that broke after removing non-Apache licensed code and non-Apache licensed code checks.  Also, while we're in pre-alpha, some tests may be failing until we finish the forking process.  We should have an issue for all failing tests, but if you find one first, feel free to open one (and fix it :) ).**


Before submitting your changes, run the test suite to make sure that nothing is broken, with:

```sh
./gradlew check
```
If we're still running down issues, you may want to start with just

```sh
./gradlew precommit
```


#### `qa`

Honestly this is kind of in flux and we're not 100% sure where we'll end up.  We welcome your throughts and help.


Right now the directory contains
* Tests that require multiple modules or plugins to work
* Tests that form a cluster made up of multiple versions of OpenSearch like
full cluster restart, rolling restarts, and mixed version tests
* Tests that test the OpenSearch clients in "interesting" places like the
`wildfly` project.
* Tests that test OpenSearch in funny configurations like with ingest
disabled
* Tests that need to do strange things like install plugins that thrown
uncaught `Throwable`s or add a shutdown hook
But we're not convinced that all of these things *belong* in the qa directory.
We're fairly sure that tests that require multiple modules or plugins to work
should just pick a "home" plugin. We're fairly sure that the multi-version
tests *do* belong in qa. Beyond that, we're not sure. If you want to add a new
qa project, open a PR and be ready to discuss options.

#### `server`
The server component of OpenSearch that contains all of the modules and
plugins. Right now things like the high level rest client depend on the server
but we'd like to fix that in the future.

#### `test`
Our test framework and test fixtures. We use the test framework for testing the
server, the plugins, and modules, and pretty much everything else. We publish
the test framework so folks who develop OpenSearch plugins can use it to
test the plugins. The test fixtures are external processes that we start before
running specific tests that rely on them.

For example, we have an hdfs test that uses mini-hdfs to test our
repository-hdfs plugin.


### Gradle Build

We use Gradle to build OpenSearch because it is flexible enough to not only
build and package OpenSearch, but also orchestrate all of the ways that we
have to test OpenSearch.

### Configurations

Gradle organizes dependencies and build artifacts into "configurations" and
allows you to use these configurations arbitrarily. Here are some of the most
common configurations in our build and how we use them:

<dl>
<dt>`implementation`</dt><dd>Dependencies that are used by the project
at compile and runtime but are not exposed as a compile dependency to other dependent projects.
Dependencies added to the `implementation` configuration are considered an implementation detail
that can be changed at a later date without affecting any dependent projects.</dd>
<dt>`api`</dt><dd>Dependencies that are used as compile and runtime dependencies of a project
 and are considered part of the external api of the project.
<dt>`runtimeOnly`</dt><dd>Dependencies that not on the classpath at compile time but
are on the classpath at runtime. We mostly use this configuration to make sure that
we do not accidentally compile against dependencies of our dependencies also
known as "transitive" dependencies".</dd>
<dt>`compileOnly`</dt><dd>Code that is on the classpath at compile time but that
should not be shipped with the project because it is "provided" by the runtime
somehow. OpenSearch plugins use this configuration to include dependencies
that are bundled with OpenSearch's server.</dd>
<dt>`testImplementation`</dt><dd>Code that is on the classpath for compiling tests
that are part of this project but not production code. The canonical example
of this is `junit`.</dd>
</dl>

### Submitting your changes

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that local tests passes, add unit tests for all the new code paths introduced by your change,
    make sure nothing is broken by your change. See the [TESTING](TESTING.asciidoc) file for help running tests. 100% Test UT coverage is required.

2. Rebase your changes

Update your local repository with the most recent code from the main OpenSearch repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.

3. Please provide detailed description on your change.

   What is your change?
   What features are enabled in your change if any?
   What is fixed in your change if any?

4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

Unless your change is trivial, there will probably be discussion about the pull request and, if any changes are needed, we would love to work with you to get your pull request merged into OpenSearch.

5. Code review and approval

   Invite domain expert to review your code.

Please adhere to the general guideline that you should never force push
to a publicly shared branch. Once you have opened your pull request, you
should consider your branch publicly shared. Instead of force pushing
you can just add incremental commits; this is generally easier on your
reviewers. If you need to pick up changes from master, you can merge
master into your branch. A reviewer might ask you to rebase a
long-running pull request in which case force pushing is okay for that
request. Note that squashing at the end of the review process should
also not be done, that can be done when the pull request is [integrated
via GitHub](https://github.com/blog/2141-squash-your-commits).


### Reviewing and accepting your contribution

We deeply appreciate everyone who takes the time to make a contribution.  We will review all contributions as quickly as possible, but there are a few things you can do to help us with the process:

First and foremost, opening an issue and discussing your change before you make it is the best way to smooth the PR process.  This will prevent a rejection because someone else is already working on the problem, or because the solution is incompatable with our architectual direction.

Additionally:
1) Plesae make sure you've run `./gradlew check` before submitting (or './gradlew precommit' while we're still in pre-alpha).  The better tested your change is, the higher our confidence will be in it.
2) Please Make sure your change includes the tests that correspond with your changes, and is formatted well.
3) Please make sure local tests pass, add unit tests for all the new code paths introduced by your change.
4) Please write both unit tests and integration test for your change
5) Smaller changes are easier to digest than large ones.
6) Given the limits of the team, we will reject PRs that are simple refactorings or "tidying up".  So make sure you're clear about what problem your PR is solving.

During the PR process, expect that they'll be some back and forth.  Please try to respond to comments in a timely fashion, and if you don't wish to continue with the PR, let us know.  If a PR takes too many iterations for its complexity or size, we may reject it.  Additionall, if you stop responding, we may close the PR as abandonded.  In either case, if you feel this was done in error, please add a comment on the PR.

If we accept the PR, we will merge your change and usually take care of backporting it to appropriate branches ourselves.

If we reject the PR, we will close the pull request with a comment explaining why. This decision isn't always final: if you feel we have
misunderstood your intended change or otherwise think that we should reconsider then please continue the conversation with a comment on the pull request and we'll do our best to address any further points you raise.
