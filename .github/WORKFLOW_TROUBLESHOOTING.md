# Troubleshooting Pull Request Checks

This guide helps contributors understand and fix common automated checks that fail on Pull Requests (PRs). When a check fails, a red '❌' will appear next to it. You must click the **"Details"** link next to any failed check to see the full error log.

---

### 1. DCO (Developer Certificate of Origin)
This is a legal requirement. Your PR cannot be merged without it.

* **What it is:** A check that verifies you have legally "signed off" on your commits.
* **Why it Fails:** The error log will mention a missing `Signed-off-by` line. This happens when you forget to use the `-s` flag in your commit command.
* **How to Fix It:** To fix your most recent commit, run `git commit --amend --signoff`. Then, you must update your PR by running `git push --force-with-lease`.

---

### 2. Changelog Verifier
This is a very common failure for new contributors.

* **What it is:** This check makes sure you have included a note about your change in the main `CHANGELOG.md` file.
* **Workflow File:** `.github/workflows/changelog-verifier.yml`
* **Why it Fails:** The error message is usually `Error: No update to CHANGELOG.md found!`.
* **How to Fix It:**
    1.  Open the `CHANGELOG.md` file at the root of the project.
    2.  Find the `## [Unreleased]` section at the top.
    3.  Under the correct category (e.g., `### Fixed`, `### Added`), add a new line describing your change. For example: ` - Created a troubleshooting guide for CI checks. See #12979.`

---

### 3. Gradle Check
This is the largest and most complex check. Be patient when this one fails.

* **What it is:** It builds the entire OpenSearch project and runs thousands of automated tests.
* **Workflow File:** `.github/workflows/gradle-check.yml`
* **Why it Fails:** The error is usually a generic `Process completed with exit code 1`. This can be caused by a bug in your code, a broken test, or a "flaky test" in the system that fails randomly.
* **How to Fix It:** Click the **"Details"** link to open the full log. Scroll to the bottom of the log to find the specific Java test that failed or the error that caused the crash. The fix depends entirely on that specific error message. If you believe the failure is unrelated to your changes, you can try closing and reopening your PR to re-run the checks.

---

### 4. Backport
You may see this check fail if your change is marked for older versions.

* **What it is:** An automated process that tries to copy your change to older release branches of the project.
* **Workflow File:** `.github/workflows/backport.yml`
* **Why it Fails:** It often fails if your code has conflicts with the older codebase.
* **How to Fix It:** Usually, a project maintainer will handle backport failures. If this check fails on your PR, you can often leave a comment asking for help, but it's best to ensure your primary PR for the `main` branch is passing all other checks first.