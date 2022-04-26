## Version 1.3.1 Release Notes

* __Exclude man page symlink in distribution (#2602)__

    [Andrew Ross](mailto:andrross@amazon.com) - Fri, 25 Mar 2022 18:36:36 -0400

    This is a short-term solution to unblock the build process for the 1.3

    release. A tool used in that process (cpio) is failing on a symlink in
    the JDK
    man pages, so this is a hack to just remove that symlink. See
    issue #2517 for
    more details.
     Signed-off-by: Andrew Ross &lt;andrross@amazon.com&gt;

* __Bump the version to 1.3.1 (#2509)__

    [Zelin Hao](mailto:87548827+zelinh@users.noreply.github.com) - Mon, 21 Mar 2022 10:30:00 -0400


    Signed-off-by: Zelin Hao &lt;zelinhao@amazon.com&gt;
