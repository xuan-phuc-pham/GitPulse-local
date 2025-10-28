[TOC]

# Local GitPulse - A local and scalable data pipeline recording real-time Github events

## Overview

This project implements an end to end data pipeline that **E**xtract github events data from [GH Archive API](https://www.gharchive.org/)** and stores them to a staging area. The staging data will be **T**ransformed, restructured and then **L**oaded to a local database as my Data Warehouse solution. From that, various data models will be built for visualisation and analysis.
## Tech stack

The whole project is executed in **Docker containers** which contain the following components:
- Workflow orchestrator: [Apache Airflow](https://airflow.apache.org/)
- Data Warehouse: [PostgreSQL](https://www.postgresql.org/) with management tool [pgAdmin](https://www.pgadmin.org/)
- Staging area and data lake: [MinIO S3](https://www.min.io/)
- Large data processing engine: [Apache Spark](https://spark.apache.org/)
- Data Modeling: [dbt](https://www.getdbt.com/)
- Data Visualisation and Analytics: [Superset](https://superset.apache.org/)
## Data source
The data source of the project will be [GH Archive](https://www.gharchive.org/)
GH Archive is a project to **record** the public GitHub timeline, **archive it**, and **make it easily accessible** for further analysis. 

> GitHub provides [15+ event types](https://docs.github.com/en/webhooks-and-events/events/github-event-types), which range from new commits and fork events, to opening new tickets, commenting, and adding members to a project. These events are aggregated into hourly archives, which you can access with any HTTP client. Each archive contains JSON encoded events as reported by the GitHub API.

To pull the file JSON from the API:
```bash
wget https://data.gharchive.org/<year>-<month>-<day>-<hour>.json.gz
```

An example of an event ( among milions of events per hour )

```json
{
  "id": "2489651194",
  "type": "IssueCommentEvent",
  "actor": {
    "id": 7356386,
    "login": "Poulern",
    "gravatar_id": "",
    "url": "https://api.github.com/users/Poulern",
    "avatar_url": "https://avatars.githubusercontent.com/u/7356386?"
  },
  "repo": {
    "id": 27872927,
    "name": "cptnnick/F3_PA",
    "url": "https://api.github.com/repos/cptnnick/F3_PA"
  },
  "payload": {
    "action": "created",
    "issue": {
      "url": "https://api.github.com/repos/cptnnick/F3_PA/issues/1",
      "labels_url": "https://api.github.com/repos/cptnnick/F3_PA/issues/1/labels{/name}",
      "comments_url": "https://api.github.com/repos/cptnnick/F3_PA/issues/1/comments",
      "events_url": "https://api.github.com/repos/cptnnick/F3_PA/issues/1/events",
      "html_url": "https://github.com/cptnnick/F3_PA/issues/1",
      "id": 52843291,
      "number": 1,
      "title": "Fix line 26 in briefing.sqf",
      "user": {
        "login": "Poulern",
        "id": 7356386,
        "avatar_url": "https://avatars.githubusercontent.com/u/7356386?v=3",
        "gravatar_id": "",
        "url": "https://api.github.com/users/Poulern",
        "html_url": "https://github.com/Poulern",
        "followers_url": "https://api.github.com/users/Poulern/followers",
        "following_url": "https://api.github.com/users/Poulern/following{/other_user}",
        "gists_url": "https://api.github.com/users/Poulern/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/Poulern/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/Poulern/subscriptions",
        "organizations_url": "https://api.github.com/users/Poulern/orgs",
        "repos_url": "https://api.github.com/users/Poulern/repos",
        "events_url": "https://api.github.com/users/Poulern/events{/privacy}",
        "received_events_url": "https://api.github.com/users/Poulern/received_events",
        "type": "User",
        "site_admin": false
      },
      "labels": [
        {
          "url": "https://api.github.com/repos/cptnnick/F3_PA/labels/bug",
          "name": "bug",
          "color": "fc2929"
        }
      ],
      "state": "open",
      "locked": false,
      "assignee": {
        "login": "cptnnick",
        "id": 6839081,
        "avatar_url": "https://avatars.githubusercontent.com/u/6839081?v=3",
        "gravatar_id": "",
        "url": "https://api.github.com/users/cptnnick",
        "html_url": "https://github.com/cptnnick",
        "followers_url": "https://api.github.com/users/cptnnick/followers",
        "following_url": "https://api.github.com/users/cptnnick/following{/other_user}",
        "gists_url": "https://api.github.com/users/cptnnick/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/cptnnick/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/cptnnick/subscriptions",
        "organizations_url": "https://api.github.com/users/cptnnick/orgs",
        "repos_url": "https://api.github.com/users/cptnnick/repos",
        "events_url": "https://api.github.com/users/cptnnick/events{/privacy}",
        "received_events_url": "https://api.github.com/users/cptnnick/received_events",
        "type": "User",
        "site_admin": false
      },
      "milestone": null,
      "comments": 2,
      "created_at": "2014-12-25T02:00:16Z",
      "updated_at": "2015-01-01T15:00:14Z",
      "closed_at": null,
      "body": "[02:48:59] Poulern: http://puu.sh/dIqRL/1d8422a923.png\r\n[02:55:08] Poulern: but i get this erorr\r\n[02:55:13] Poulern: every tiem\r\n[02:55:28] Nick van der Lee: I wanted it designated by side instead of faction\r\n[02:56:02] Nick van der Lee: OH\r\n[02:56:15] Nick van der Lee: Try (str(side player) )\r\n[02:56:45] Poulern: _unitSide = toLower (side player);\r\n[02:56:51] Poulern: _unitSide = (str(side player) )\r\n[02:56:53] Poulern: ?\r\n[02:56:55] Poulern: like so\r\n[02:57:12] Nick van der Lee: _unitSide = toLower (str(side player))\r\n[02:58:03] Poulern: if (_unitSide != toLower (faction (leader group player))) then {_unitSide = toLower (faction (leader group player))};\r\n[02:58:06] Poulern: error missing ;\r\n[02:58:21] Nick van der Lee: Delete that part\r\n[02:58:26] Nick van der Lee: It's obsolete\r\n[02:58:38] Poulern: Make sure to fix the main\r\n[02:58:46] Nick van der Lee: Make a ticket\r\n[02:58:48] Poulern: briefing.sqf in the F3_PA\r\n[02:58:51] Poulern: okay man\r\n[02:58:55] Nick van der Lee: I'll need to fix in main F3 too\r\n[02:59:11] Nick van der Lee: It's 3 AM I ain't gonna remember : P"
    },
    "comment": {
      "url": "https://api.github.com/repos/cptnnick/F3_PA/issues/comments/68488497",
      "html_url": "https://github.com/cptnnick/F3_PA/issues/1#issuecomment-68488497",
      "issue_url": "https://api.github.com/repos/cptnnick/F3_PA/issues/1",
      "id": 68488497,
      "user": {
        "login": "Poulern",
        "id": 7356386,
        "avatar_url": "https://avatars.githubusercontent.com/u/7356386?v=3",
        "gravatar_id": "",
        "url": "https://api.github.com/users/Poulern",
        "html_url": "https://github.com/Poulern",
        "followers_url": "https://api.github.com/users/Poulern/followers",
        "following_url": "https://api.github.com/users/Poulern/following{/other_user}",
        "gists_url": "https://api.github.com/users/Poulern/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/Poulern/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/Poulern/subscriptions",
        "organizations_url": "https://api.github.com/users/Poulern/orgs",
        "repos_url": "https://api.github.com/users/Poulern/repos",
        "events_url": "https://api.github.com/users/Poulern/events{/privacy}",
        "received_events_url": "https://api.github.com/users/Poulern/received_events",
        "type": "User",
        "site_admin": false
      },
      "created_at": "2015-01-01T15:00:14Z",
      "updated_at": "2015-01-01T15:00:14Z",
      "body": "Sorry yes this has been fixed. GJ"
    }
  },
  "public": true,
  "created_at": "2015-01-01T15:00:14Z"
}
```

## Architechture

## Workflow in details

## Setup instructions

## Example outputs

## Future improvements

## Contact



