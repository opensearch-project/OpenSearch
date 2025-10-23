# Contributing to OpenSearch

- [Contributing to OpenSearch](#contributing-to-opensearch)
- [First Things First](#first-things-first)
- [Ways to Contribute](#ways-to-contribute)
    - [Bug Reports](#bug-reports)
    - [Feature Requests](#feature-requests)
    - [Documentation Changes](#documentation-changes)
    - [Contributing Code](#contributing-code)
- [Developer Certificate of Origin](#developer-certificate-of-origin)
- [Changelog](#changelog)
- [Review Process](#review-process)
    - [Tips for Success](#tips)
- [Troubleshooting Failing Builds](#troubleshooting-failing-builds)

---

## Contributing to OpenSearch

OpenSearch is a community project built and maintained by people just like **you**. We're glad you're interested in helping out. There are several different ways you can do it, but before we talk about that, let's talk about how to get started.

### First Things First

1. **When in doubt, open an issue** - For almost any type of contribution, the first step is opening an issue. Even if you think you already know what the solution is, writing down a description of the problem you're trying to solve will help everyone get context when they review your pull request. If it's truly a trivial change (e.g. spelling error), you can skip this step -- but as the subject says, when in doubt, [open an issue](https://github.com/opensearch-project/OpenSearch/issues).

2. **Only submit your own work** (or work you have sufficient rights to submit) - Please make sure that any code or documentation you submit is your work or you have the rights to submit. We respect the intellectual property rights of others, and as part of contributing, we'll ask you to sign your contribution with a "Developer Certificate of Origin" (DCO) that states you have the rights to submit this work and you understand we'll use your contribution. There's more information about this topic in the [DCO section](#developer-certificate-of-origin).

### Ways to Contribute

**Please note:** OpenSearch is a fork of [Elasticsearch 7.10.2](https://github.com/elastic/elasticsearch). If you do find references to Elasticsearch (outside of attributions and copyrights!) please [open an issue](https://github.com/opensearch-project/OpenSearch/issues).

#### Bug Reports

A bug is when software behaves in a way that you didn't expect and the developer didn't intend. To help us understand what's going on, we first want to make sure you're working from the latest version. Please make sure you're testing against the [latest version](https://github.com/opensearch-project/OpenSearch).

Once you've confirmed that the bug still exists in the latest version, you'll want to check the bug is not something we already know about. A good way to figure this out is to search for your bug on the [open issues GitHub page](https://github.com/opensearch-project/OpenSearch/issues).

If you've upgraded to the latest version and you can't find it in our open issues list, then you'll need to tell us how to reproduce it. To make the behavior as clear as possible, please provide your steps as `curl` commands which we can copy and paste into a terminal to run it locally, for example:

