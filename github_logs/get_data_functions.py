from github import Github
from github import Auth

def connect_to_github_repo(github_access_token:str,repo_owner:str,repo_name:str):
    '''Creates a connection to a Github repository defined by repo_owner/repo_name, 
    using the github_access_token.'''

    auth = Auth.Token(github_access_token)
    g = Github(auth=auth)
    repo = g.get_repo(f"{repo_owner}/{repo_name}")

    return repo

def get_issues(repo,num_issues:int=10,only_closed:bool=True):
    '''Get the last (order by creation date) `num_issues` issues from 
    the given Github repository. By default the last 10 closed issues are returned.'''

    if only_closed:
        state = 'closed'
    else:
        state = 'all'

    issues = repo.get_issues(state=state , sort='created', direction='desc')[:num_issues]

    return issues

def get_events(issue):
    
    events = issue.get_timeline()

    return events

def get_user_data(user_id,github_access_token:str):

    auth = Auth.Token(github_access_token)
    g = Github(auth=auth)
    user = g.get_user_by_id(user_id)
    user_data = user.raw_data

    return user_data
