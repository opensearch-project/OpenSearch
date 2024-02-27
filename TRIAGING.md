<img src="https://opensearch.org/assets/img/opensearch-logo-themed.svg" height="64px">

The maintainers of the OpenSearch Repo seek to promote an inclusive and engaged community of contributors. In order to facilitate this, weekly triage meetings are open-to-all and attendance is encouraged for anyone who hopes to contribute, discuss an issue, or learn more about the project. To learn more about contributing to the OpenSearch Repo visit the [Contributing](./CONTRIBUTING.md) documentation.

### Do I need to attend for my issue to be addressed/triaged?

Attendance is not required for your issue to be triaged or addressed.  If not accepted the issue will be updated with a comment for next steps.  All new issues are triaged weekly.

You can track if your issue was triaged by watching your GitHub notifications for updates.

### What happens if my issue does not get covered this time?

Each meeting we seek to address all new issues. However, should we run out of time before your issue is discussed, you are always welcome to attend the next meeting or to follow up on the issue post itself.

### How do I join the Triage meeting?

Meetings are hosted regularly at 10:00a - 10:55a Central Time every Wednesday and can be joined via [Chime](https://aws.amazon.com/chime/), with this [meeting link](https://chime.aws/1988437365).

After joining the Chime meeting, you can enable your video / voice to join the discussion.  If you do not have a webcam or microphone available, you can still join in via the text chat.

If you have an issue you'd like to bring forth please prepare a link to the issue so it can be presented and viewed by everyone in the meeting.

### Is there an agenda for each week?

Meetings are 55 minutes and follows this structure:

Yes, each 55-minute meeting follows this structure:
1. **Initial Gathering:** Feel free to turn on your video and engage in informal conversation. Shortly, a volunteer triage [facilitator](#what-is-the-role-of-the-facilitator) will begin the meeting and share their screen.
2. **Record Attendees:** The facilitator will request attendees to share their GitHub profile links. These links will be collected and assembled into a [tag](#how-do-triage-facilitator-tag-comments-during-the-triage-meeting) to annotate comments during the meeting.
3. **Announcements:** Any announcements will be made at the beginning of the meeting.
4. **Review of New Issues:** We start by reviewing all untriaged [issues](https://github.com/search?q=label%3Auntriaged+is%3Aopen++repo%3Aopensearch-project%2FOpenSearch+&type=issues&ref=advsearch&s=created&o=desc) for the OpenSearch repo.
5. **Attendee Requests:** An opportunity for any meeting member to request consideration of an issue or pull request.
6. **Open Discussion:** Attendees can bring up any topics not already covered by filed issues or pull requests.

### What is the role of the facilitator?

The facilitator is crucial in driving the meeting, ensuring a smooth flow of issues into OpenSearch for future contributions. They maintain the meeting's agenda, solicit input from attendees, and record outcomes using the triage tag as items are discussed.

### Do I need to have already contributed to the project to attend a triage meeting?

No prior contributions are required. All interested individuals are welcome and encouraged to attend. Triage meetings offer a fantastic opportunity for new contributors to understand the project and explore various contribution avenues.

### What if I have an issue that is almost a duplicate, should I open a new one to be triaged?

You can always open an [issue](https://github.com/opensearch-project/OpenSearch/issues/new/choose) including one that you think may be a duplicate. If you believe your issue is similar but distinct from an existing one, you are encouraged to file it and explain the differences during the triage meeting.

### What if I have follow-up questions on an issue?

If you have an existing issue you would like to discuss, you can always comment on the issue itself. Alternatively, you are welcome to come to the triage meeting to discuss.

### Is this meeting a good place to get help setting up features on my OpenSearch instance?

While we are always happy to help the community, the best resource for implementation questions is [the OpenSearch forum](https://forum.opensearch.org/).

There you can find answers to many common questions as well as speak with implementation experts.

### What are the issue labels associated with triaging?

Yes, there are several labels that are used to identify the 'state' of issues filed in OpenSearch .
| Label         | When Applied         | Meaning                                                                                                                                 |
|---------------|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `Untriaged` | When issues are created or re-opened. | Issues labeled as 'Untriaged' require the attention of the repository maintainers and may need to be prioritized for quicker resolution. It's crucial to keep the count of 'Untriaged' labels low to ensure all potential security issues are addressed in a timely manner. See [SECURITY.md](https://github.com/opensearch-project/OpenSearch/blob/main/SECURITY.md) for more details on handling these issues. |
| `Help Wanted` | Anytime. | Issues marked as 'Help Wanted' signal that they are actionable and not the current focus of the project maintainers. Community contributions are especially encouraged for these issues. |
| `Good First Issue` | Anytime. | Issues labeled as 'Good First Issue' are small in scope and can be resolved with a single pull request. These are recommended starting points for newcomers looking to make their first contributions. |

### What are the typical outcomes of a triaged issue?

| Outcome      | Label            | Description                                                                                                                                                                                      | Canned Response                                                                                                                                                           |
|--------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Accepted     | `-untriaged`     | The issue has the details needed to be directed towards area owners.                                                                                                                            | "Thanks for filing this issue, please feel free to submit a pull request."                                                                                                 |
| Rejected     | N/A              | The issue will be closed with a reason for why it was rejected. Reasons might include lack of details, or being outside the scope of the project.                                               | "Thanks for creating this issue; however, it isn't being accepted due to {REASON}. Please feel free to open a new issue after addressing the reason."                                |
| Area Triage  | `+{AREALABEL}`  | OpenSearch has many different areas. If it's unclear whether an issue should be accepted, it will be labeled with the area and an owner will be @mentioned for follow-up.                        | "Thanks for creating this issue; the triage meeting was unsure if this issue should be accepted, @{PERSON} or someone from the area please review and then accept or reject this issue?" |
| Transfer     | N/A              | If the issue applies to another repository within the OpenSearch Project, it will be transferred accordingly.                                                                                    | "@opensearch-project/triage, can you please transfer this issue to project {REPOSITORY}." Or, if someone at the meeting has permissions, they can start the transfer.        |

### Is this where I should bring up potential security vulnerabilities?

Due to the sensitive nature of security vulnerabilities, please report all potential vulnerabilities directly by following the steps outlined on the [SECURITY.md](https://github.com/opensearch-project/OpenSearch/blob/main/SECURITY.md) document.

### How do triage facilitator tag comments during the triage meeting?

During the triage meeting, facilitators should use the tag _[Triage - attendees [1](#Profile_link) [2](#Profile_link)]_ to indicate a collective decision. This ensures contributors know the decision came from the meeting rather than an individual and identifies participants for any follow-up queries.

This tag should not be used outside triage meetings.
