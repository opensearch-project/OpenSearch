import os
import sys
import fileinput
import re

file_path = raw_input("Path to raw notes file (e.g., notes.md): ")
plugin_name = 'job-scheduler'
plugin_version = raw_input('Plugin version (x.x.x.x): ')
app = 'OpenSearch'
app_version = raw_input(app + ' compatibility version (x.x.x): ')

for line in fileinput.input(file_path, inplace=True):
    # Add title & compatibility
    if fileinput.isfirstline():
        line = "## Version " + plugin_version + " Release Notes\n\n" \
            "Compatible with " + app + " " + app_version + "\n" 

    # # Add link to PRs
    # if '*' in line:
    #     start = line.find('#') + 1
    #     end = line.find(')', start)
    #     pr_num = line[start:end]
    #     line = re.sub(searchExp, "([#" + pr_num +
    #                   "](" + link_prefix + pr_num + "))", line)
    sys.stdout.write(line)

# Rename file to be consistent with ODFE standards
new_file_path = "opensearch." + plugin_name + ".release-notes-" + \
    plugin_version + ".md"
os.rename(file_path, new_file_path)

print('\n\nDone!\n')
