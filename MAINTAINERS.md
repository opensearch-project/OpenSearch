# OpenSearch Maintainers

## Maintainers

The current maintainers are:

| Maintainer | GitHub ID | Affiliation |
| --------------- | --------- | ----------- |
| Abbas Hussain | [abbashus](https://github.com/abbashus) | Amazon |
| Charlotte Henkle | [CEHENKLE](https://github.com/CEHENKLE) | Amazon |
| Harold Wang | [harold-wang](https://github.com/harold-wang) | Amazon |
| Himanshu Setia | [setiah](https://github.com/setiah) | Amazon |
| Nick Knize | [nknize](https://github.com/nknize) | Amazon |
| Rabi Panda | [adnapibar](adnapibar) | Amazon |
| Sarat Vemulapalli | [saratvemulapalli](https://github.com/saratvemulapalli) | Amazon |
| Tianli Feng | [tlfeng](https://github.com/tlfeng) | Amazon |
| Gopala Krishna Ambareesh | [krishna-ggk](https://github.com/krishna-ggk) |Amazon |
| Vengadanathan Srinivasan | [vengadanathan-s](https://github.com/vengadanathan-s) | Amazon |
| Shweta Thareja |[shwetathareja](https://github.com/shwetathareja) | Amazon |
| Itiyama Sadana | [itiyamas](https://github.com/itiyamas) | Amazon |
| Daniel "dB." Doubrovkine | [dblock](https://github.com/dblock) | Amazon |


## Overview

This document explains who the maintainers are (see above) what they do in the OpenSearch repos and how they should be doing it.  If you're interested in the "whats and hows" of contributing to the project, I'd recommend starting with the [CONTRIBUTING doc](https://github.com/opensearch-project/OpenSearch/blob/main/CONTRIBUTING.md).

## Responsibilities

Maintainers must be active and visible members of the community. This includes:

1.  Reviewing PRs

There's no fixed number of PRs every maintainer needs to look at to be considered "active", but if you have to ask "am I inactive?" that's probably something you should worry about.

2.  Triaging open issues in the community.

The expectation is that you're spending a portion of your day looking at issues.  If we have one a few issues coming in every day, you can do it over your morning coffee.  When you're triaging, you should:
	*Add "help wanted" labels to good issues for new community members.
	*Add "Severity-Blocking" labels if an issue scares you.  The community doesn't have a good way to page us right now, so if you see a blocking bug, mark it as Blocking and engage oncall.
	*Request for more information from a submitter if an issue is not clear.

3. Responding to enhancement requests and forum posts

Please make sure your reading and commenting on enhancements as they come in.

4.  Maintaining the overall health of the repo according to the standards below.

Please help make sure that we're following the practices below.  If you see something that's not quite right, step up and fix it.

5.  Helping with backports

As part of our release process we initially merge into Main and then make conscious decisions about where and how to backport features.  As a maintainer, you need to be looking for fixes and features that need to be backported and then doing the backport.

6.  Running releases

Once we get into a regular cadence of release (past 1.0.0) maintainers should expect to act as release managers (see below).

## Versions

OpenSearch uses [semantic versioning aka semver](https://semver.org/).

Here's a quick primer from [Apache’s versioning doc](https://commons.apache.org/releases/versioning.html) :

A release number is comprised of 3 components: the major release number, the minor release number, and an optional point release number. Here is a sample release number:
2.0.4
and it can be broken into three parts:

    * major release: 2
    * minor release: 0
    * point release: 4

The next release of this component would increment the appropriate part of the release number, depending on the type of release (major, minor, or point). For example, a subsequent minor release would be version 2.1, or a subsequent major release would be 3.0.
Note that release numbers are composed of three _integers_, not three digits. Hence if the current release is 3.9.4, the next minor release is 3.10.0.

OpenSearch and OpenSearch Dashboards will release major version together.   They will NOT synchronize minor release — whenever the team feels they’re ready to release a minor version or patch (modulo the schedule above), they should release.

What we guarantee is that any major release of Dashboards is compatible with the same major release of OpenSearch.  For example: 3.2.1 of Dashboards will work with 3.0.4 of OpenSearch, but 2.3.1 of Dashboards is not guaranteed to work with 3.0.4 of OpenSearch

### Breaking Changes and Backwards Compatibility

We do not release breaking changes except in major releases.  This means you.  *Paddington Hard Stare*.  The goal for 1.x is the compatible with Elasticsearch 6.8 to 7.10.2, so the first time we'd introduce breaking changes is OpenSearch 2.0

## Branches

### Primary Branches
Currently, OpenSearch has three branches: 1.0, 1.x, Main

*Main* is our next major release. This is the location that all merges should take place. It's going to moving fast and dynamic in there.

*1.x* is our next minor release. Once something gets merged into main, you can chose to backport it to 1.x.

*1.0* is our current release. In between minor releases, only hotfixes (security and otherwise) would get backported to 1.0.

When you review a PR apply the next major version label (e.g., 2.0) and if accepted, merge it into Main. If the requester thinks it should be backported and released with 1.0, they should open a separate PR and the reviewer will label it with the 1.x label. Then we'll merge the new PRs to the 1.x branch. If you don't know what release a requestor wants to target, ask!

In the nearish future we'd like to see nightly of all three branches (1.0, 1.x and 2.0) so we can rapidly find regressions, but I'll need to hash that out with infra.

Why do it this way?
Because it lets main evolve quickly, while letting us be a little more circumspect about 1.x and 1.0. It'll also make it easier for us to release if everything is clearly tagged (not having clear tagging made this release kind of a potchke).

### Feature Branches
In general, we don't want to have tons of branches lying around. It makes it confusing and messy - the more branches you have the harder it is to keep track of what has been cherry-picked where.  So please, even if you can open a branch, work on your own fork.  But if you've got a feature or an update that you'll be working for a while, or you'll be working on with other folks, feel free to create a feature branch for it using update/<thing> or feature/<thing>.  Anyone can request a branch an issue, and as a maintainer if you see an issue come up that could benefit from being worked in a branch, feel free to offer to make one.

Once the work is merged in a feature branch, please make sure to delete the branch.

## Labels and Labeling

As a maintainer, part of your responsibility is triage.

By default, all new issues get the following labels:  untriage, bug.  As a maintainer, part of your responsibility is to review all incoming issues and triage them.  Our goal is to never have an issue sit for more than 2 days without a response.

## Releases

Part of your job as a maintainer is to run releases as a Release Manager (RM).  Here's how the process works:

1. *Two weeks before release* A maintainer volunteers to be a release manager.  They initiates the process by posting a topic in the forum of the intent to release the next version (to facilitate discussion and feedback)
2. The RM then proposes a “feature freeze” date (and intent to cut the branch).  This should be a date  *one week* in the future (and therefore one week before release).  During this week, all maintainers will:
	1. Mark “blocker” issues preventing a successful release
	2. Merge all PRs that are ready into the main, or clearly label them as being for a future release.
	3. Review merged PRs and determine if they need a backport to the current release.
	4. If a maintainer is not sure if a merged PR needs a backport, they should post in the PR asking the submitter their intention.
3. *24 hours prior to feature freeze (cutting the branch)*:
    1. RM update the forum post with a “final notice“ message to ensure all blockers are identified (some may be fixed by then).
4. *On day of feature freeze*:
    1. RM posts a “feature freeze” notice that the branching is underway and no more PRs should be merged.
    2. RM branches from the .x branch into a new .m+1 branch. (.x is now the next minor)
    3. RM sends a “branching complete” email that the branch has been cut and a reminder of the release date requesting all blockers be cleared in time of the release
5. *During the week between "feature freeze" and release, all maintainers will*:
	1. Will monitor issues looking for fixes that need to go in to the release.
5. *24 Hours prior to release*:
    1. RM sends a “final notice” for blockers email; also requesting a “speak now or forever hold your peace” on critical blockers. If something is holding up the release a postponed release may occur.
6. *On Day of Release*:
    1. RM tags the branch with the release version
    2. RM begins building and signing artifacts
    3. RM stages artifacts to maven repo
    4. an email is sent once artifacts are published requesting a quorum vote which includes maintainers run smoke tests on the staged artifacts;
    5. RM sums votes
    6. RM publishes tested artifacts


## Pull Requests (PRs)

### Best practices for reviewing PRs
//Todo
