# Generate Release Notes

When asked to generate release notes, follow this procedure.

## Inputs

- `PREV_TAG`: The previous release tag (e.g., `3.4.0`)
- `RELEASE_REF`: The release tag or branch to generate notes for (e.g., `3.5.0` or `upstream/3.5`)

If not provided, infer from context. Use `git tag --sort=-creatordate` to find recent tags.

All temporary files throughout this process should be written to `/tmp/release-notes-VERSION/` (e.g., `/tmp/release-notes-3.6.0/`). Create this directory at the start. This makes cleanup easy: `rm -rf /tmp/release-notes-VERSION/`.

## Step 1: Identify commits to exclude

The previous release tag lives on a release branch, not on main. Commits merged to main during the release window (between the branch cut and the tag) may have been backported to the release branch. These already shipped in the previous release and must be excluded.

```
PREV_BRANCH_CUT=$(git merge-base "$PREV_TAG" "$RELEASE_REF")
```

Extract all PR numbers from commits in the previous release window:

```
git log ${PREV_BRANCH_CUT}..${PREV_TAG} --format='%s'
```

Parse PR numbers (pattern `#NNNNN`) from these commit subjects. This is the exclusion set.

## Step 2: Collect candidate commits

```
git log ${PREV_TAG}..${RELEASE_REF} --format='%s' --reverse
```

Remove any commit whose PR number appears in the exclusion set from step 1. Log which commits were excluded and why.

Detect commit/revert pairs (a commit followed by a revert of that commit) and exclude both — the net effect is no change.

## Step 3: Fetch PR metadata

For each non-dependency-bump commit, fetch the PR title, labels, and description in a single call and write the results to a temp file per PR:

```
gh pr view NNNNN --json title,body,labels --jq '{title, labels: [.labels[].name], body}' > /tmp/release-notes-VERSION/pr-NNNNN.json
```

Dependency bumps (commit message matches "Bump X from Y to Z" or similar) do not need a GitHub API call — the commit message is sufficient. These go directly into the Dependencies category.

## Step 4: Filter and categorize

Using the cached PR metadata from step 3:

1. **Hard exclude** any PR with the `skip-changelog` label, regardless of content.

2. **Exclude non-user-facing changes** per this policy:
   - Test additions, modifications, or fixes (flaky test fixes, test refactoring)
   - Incremental PRs for a larger feature (these should already have one entry from the main PR)
   - Documentation changes or internal code refactoring
   - Build-related changes (CI config, Gradle changes)
   - Release machinery (release notes commits, README edits)
   - Maintainer list changes

   Use the commit message, PR description, and labels to make this judgment. When uncertain whether a change is user-facing, include it — a human reviewer can remove it later.

3. **Categorize** each surviving commit using [Keep A Changelog](https://keepachangelog.com/en/1.0.0/) categories. PR labels are useful here (e.g., `enhancement`, `bug`, `breaking`, `dependencies`, `deprecation`):
   - **Added** — new features and capabilities
   - **Changed** — changes to existing functionality
   - **Deprecated** — features that will be removed in a future release
   - **Removed** — features that were removed
   - **Fixed** — bug fixes
   - **Dependencies** — dependency version updates. List Apache Lucene updates first.

4. **Write entries.** For each entry, write a concise one-line summary. Use the commit message and PR description as input but rewrite for clarity and consistency. The target audience for these notes are OpenSearch users, such as end-users, operators, and system administrators.

   Format each entry as:

   ```
   - Summary of change ([#NNNNN](https://github.com/opensearch-project/OpenSearch/pull/NNNNN))
   ```

   Group related commits into a single entry when appropriate (e.g., multiple PRs implementing parts of the same feature, or a series of dependency bumps for the same library).

## Step 5: Track borderline calls

During steps 1–4, record any judgment calls where the categorization or inclusion/exclusion decision is debatable. Examples:

- A PR labeled `bug` that reads more like a behavioral change
- Grouping multiple PRs into a single entry
- Including a PR that could reasonably be considered non-user-facing
- Choosing one category over another when both fit

Write these to `/tmp/release-notes-VERSION/borderline.md` as a bulleted list. Each entry should reference the PR number(s) and briefly explain the decision and alternatives.

## Step 6: Output

Write the release notes file to `release-notes/opensearch.release-notes-VERSION.md` using this template:

```markdown
## Version VERSION Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version VERSION

### Added
...

### Changed
...

### Deprecated
...

### Removed
...

### Fixed
...

### Dependencies
...
```

Omit empty categories. Use `- ` (dash space) for list items.

## Step 7: Commit and push

Identify the user's fork remote from `git remote -v` — look for a remote whose URL contains the user's GitHub username rather than `opensearch-project/OpenSearch`. If ambiguous, ask the user which remote to use.

```
git checkout -b release-notes-VERSION
git add release-notes/opensearch.release-notes-VERSION.md
git commit -s -m "Add release notes for VERSION"
git push <fork-remote> release-notes-VERSION
```

## Step 8: Open PR

Create the PR body in a temp file, then open the PR:

```
gh pr create \
  --base main \
  --head <github-user>:release-notes-VERSION \
  --title "Add release notes for VERSION" \
  --body-file /tmp/release-notes-VERSION/pr-body.md
```

The PR body should follow this structure:

```markdown
### Description
Release notes for OpenSearch VERSION, covering commits from PREV_TAG to RELEASE_REF.

### Borderline calls
- #NNNNN: Placed in **Category** — rationale. Could also be **Other Category**.
- #AAAAA + #BBBBB: Grouped into one entry — rationale.
- ...

### Check List
- [x] Functionality includes testing.

By submitting this pull request, I confirm that my contribution is made under
the terms of the Apache 2.0 license.
```

The borderline calls section gives reviewers specific items to validate. If there are no borderline calls, omit the section.
