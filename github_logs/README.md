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
- object attributes:
    - `issue`: `number`, `title`, `timeline_url`
    - `user`: `id`, `login`, `type`, `html_url`
- event types:
    - all [GitHub timeline events](https://docs.github.com/en/rest/using-the-rest-api/issue-event-types), except `line-commented`
    - `created` (for new issues)
- object-to-object relations:
    - `issue`-to-`user`: `created by`
- event-to-object relations:
    - `created`-to-`issue`: `created`
    - `created`-to-`user`: `created by`
