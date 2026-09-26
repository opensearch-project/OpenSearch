# Build-time third-party crate patches

Some fixes must live inside a crates.io crate — either crate-internal APIs our own Rust can't
reach, or upstream behavior we carry until a fix lands upstream. These patches are applied to the
official crate sources at build time by the generic `registerCratePatch(crate, version, patchFile)`
helper in `../../build.gradle`.

## How it works
For each registered crate the Gradle task (`preparePatched<CrateName>`) does:
1. Download `https://static.crates.io/crates/<crate>/<crate>-<version>.crate` into `build/downloads/`
   (skipped if already present; fails under `--offline` if not cached).
2. Verify the tarball against the checked-in `<crate>-<version>.crate.sha256` — the integrity gate
   (a `[patch.crates-io]` path dep carries no Cargo.lock checksum, so this is what guarantees we
   patched the same bytes that were reviewed).
3. Unpack under `build/patched/<crate>-<version>/`.
4. `git apply -p2 <patch>` (with `GIT_CEILING_DIRECTORIES` set so git treats the unpacked tree as
   standalone), then verify every target file actually changed (git apply exits 0 even when it
   skips a file, so a no-op apply is caught here).

`rust/Cargo.toml`'s `[patch.crates-io]` points each patched crate at `build/patched/<crate>-<version>`,
and `buildRustLibrary` depends on every patch task, so cargo never sees the unpatched crate.
`build/` is gitignored — the patched tree is regenerated on every build.

## Adding / refreshing a patch
1. Add the patch file here as `<crate>-<version>-<slug>.patch`. Generate it against the **unmodified
   crates.io sources** with headers `+++ b/<crate>/<path>` (so `git apply -p2` strips `b/<crate>`).
2. Add `<crate>-<version>.crate.sha256` (format: `<sha256>  <crate>-<version>.crate`;
   `sha256sum` of the crates.io `.crate` tarball).
3. Register it in `../../build.gradle`: add a `registerCratePatch('<crate>', '<version>', '<patchFile>')`
   entry to `cratePatchTasks`.
4. Add the `[patch.crates-io]` entry in `rust/Cargo.toml`:
   `<crate> = { path = "build/patched/<crate>-<version>" }`.
5. Materialize once for a bare `cargo`/rust-analyzer (Gradle does this automatically):
   `./gradlew :sandbox:libs:dataformat-native:preparePatched<CrateName>`.

Bumping a crate version: update the version in all four places (build.gradle, Cargo.toml patch path,
patch filename, sha256 filename) and regenerate the patch + sha256 against the new sources.

## Current patches
- **`parquet-59.2.0-forward-cursor.patch`** — adds forward-cursor APIs `skip_rows` /
  `read_next_batch` (and a `ReadPlan::set_batch_size` helper) to `ParquetRecordBatchReader`, used by
  `forward_reader.rs`. They require the crate's private internals, so they can't live outside the
  crate. Not yet upstream (`apache/arrow-rs#10655`); **drop this patch + its `[patch.crates-io]`
  entry** once the APIs land upstream and DataFusion's arrow/parquet pin includes them.
- **`datafusion-datasource-parquet-55.0.0-infer-skip-pageindex.patch`** — forces
  `ParquetFormat::infer_schema` to `PageIndexPolicy::Skip`. DF55 (apache/datafusion#22857) made it
  fetch the parquet page index (default `Optional` when a file-metadata cache is present), which
  schema inference never uses; our footer-only metadata cache strips it, so it reloaded ~30ms/query
  per segment (absent on DF54). The page index is still loaded lazily per-column at scan by the
  scoped cache. **Upstream candidate** — drop this patch + its `[patch.crates-io]` entry if adopted.
