# Extracting object-centric event data from GitHub repositories

## Getting started
1. Create a personal GitHub access token. Instructions can be found here: [GitHub Docs - Creating a personal access token (classic)](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic)
1. Save this access token in `config.py`. It will be used to connect to the GitHub REST API.
1. In `script.py`, define from which repository the event data will be extracted and how many issues (this includes pull requests) should be fetched.
1. In `script.py`, define where the data should be stored.
1. Run `python3 ../github_logs/script.py`.

## What data is included?
This part of the Stack't project is still in active development. A list of what kind of data is extracted is given below and will be kept up-to-date.
- object types:
    - `issue` (this includes both pull requests and issues)
    - `user` (GitHub users)
    - `team` (GitHub team)
- object attributes:
    - `issue`: `number`, `title`, `timeline_url`
    - `user`: `id`, `login`, `type`, `url` (html_url)
    - `team`: `slug`, `name`, `privacy`, `url` (html_url)
- event types:
    - all [GitHub timeline events](https://docs.github.com/en/rest/using-the-rest-api/issue-event-types), except `line-commented`
    - `created` (for new issues)
- event attributes:
    - `author_association` (if available in API response)
- event-to-object relations:
    - `created`-to-`issue`: `created`
    - `created`-to-`user`: `created by`
    - `timeline_event`-to-`user`: `actor` (user that did the action) Note: not available yet for event type `committed`.
    - `review_requested`/`review_request_removed`-to-`user`: `requested_reviewer`
    - `review_requested`/`review_request_removed`-to-`team`: `requested_team`
    - `assigned`/`unassigned`-to-`user`: `assignee`
- object-to-object relations:
    - `issue`-to-`user`: `created by`
    - `issue`-to-`user`: `requested_reviewer` (dynamic, set to `null` when `review_request_removed`)
    - `issue`-to-`user`: `assignee` (dynamic, set to `null` when `unassigned`)
