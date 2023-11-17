# -*- coding: utf-8 -*-
from git import Repo
from github import Github
import requests


class GitLabApiException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

    def __str__(self):
        return self.message


class GitHubUtil(object):
    def __init__(self, repo_path: str = None):

        self.branch_name_list = None
        self.branches = None

        url = f"https://api.github.com/repos/{repo_path}/branches"
        response = requests.get(url)
        self.branches = response.json()
        branch_name_list = []
        for branch in self.branches:
            branch_name_list.append(branch['name'])



    def get_branches(self):
        return self.branch_name_list

    def get_last_commit_by_branch(self, branch: str = None):
        if self.branches is None:
            return None

        for current_branch in self.branches:
            if current_branch == branch:
                return current_branch['commit']['sha']

        return None


if __name__ == "__main__":
    github = GitHubUtil()
    # github.get_branches("robotslacker/sqlcli")
    github.get_branches("taosdata/TDengine")