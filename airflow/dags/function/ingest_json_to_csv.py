import requests
import json
import gzip
import io
import pandas as pd
import csv
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile # the file will be cleaned up after closing
from datetime import datetime, timedelta

def get_json(url):
  response = requests.get(url,stream = True)
  if response.status_code == 200:
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
      for line in f:
        yield json.loads(line)
  else:
    print(response.status_code)

def ingest_json_to_csv(**context):
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    s3_conn = S3Hook(aws_conn_id='minio_conn') 

    # Cleaning first

    prefixes = [
        f"gh_data/events/{date}/",
        f"gh_data/users/{date}/",
        f"gh_data/repos/{date}/",
        f"gh_data/orgs/{date}/"
    ]

    for prefix in prefixes:
        keys = s3_conn.list_keys(bucket_name='airflow', prefix=prefix)
        if keys:
            s3_conn.delete_objects(bucket='airflow', keys=keys)
            print(f"✅ Deleted {len(keys)} objects under {prefix}")
        else:
            print(f"⚠️ No objects found under {prefix}")

    # Writing

    for hour in range(24):
        base_url = f"http://data.gharchive.org/{date}-{hour}.json.gz"
        with NamedTemporaryFile("w+", suffix=".csv", delete=True) as e_file,\
        NamedTemporaryFile("w+", suffix=".csv", delete=True) as u_file,\
        NamedTemporaryFile("w+", suffix=".csv", delete=True) as r_file,\
        NamedTemporaryFile("w+", suffix=".csv", delete=True) as o_file:

            e_writer = csv.DictWriter(e_file, fieldnames=["id", "type", "actor_id", "repo_id", "public", "created_at", "org_id"])
            u_writer = csv.DictWriter(u_file, fieldnames=["id", "login", "display_login", "gravatar_id", "url", "avatar_url"])
            r_writer = csv.DictWriter(r_file, fieldnames=["id", "name", "url"])
            o_writer = csv.DictWriter(o_file, fieldnames=["id", "login", "gravatar_id", "url", "avatar_url"])
        
            if e_file.tell() == 0:
                e_writer.writeheader()
            if u_file.tell() == 0:
                u_writer.writeheader()
            if r_file.tell() == 0:
                r_writer.writeheader()
            if o_file.tell() == 0:
                o_writer.writeheader()

            for line in get_json(base_url):
                line.pop("payload", None)
                event_data = {
                    "id": line.get("id"),
                    "type": line.get("type"),
                    "actor_id": line.get("actor", {}).get("id"),
                    "repo_id": line.get("repo", {}).get("id"),
                    "public": line.get("public"),
                    "created_at": line.get("created_at"),
                    "org_id": line.get("org", {}).get("id") if line.get("org") else None
                }
                e_writer.writerow(event_data)
                
                actor = line.get("actor", {})
                if actor:
                    u_writer.writerow({
                        "id": actor.get("id"),
                        "login": actor.get("login"),
                        "display_login": actor.get("display_login"),
                        "gravatar_id": actor.get("gravatar_id"),
                        "url": actor.get("url"),
                        "avatar_url": actor.get("avatar_url")
                    })
                
                repo = line.get("repo", {})
                if repo:
                    r_writer.writerow({
                        "id": repo.get("id"),
                        "name": repo.get("name"),
                        "url": repo.get("url")
                    })
                
                org = line.get("org", {})
                if org:
                    o_writer.writerow({
                        "id": org.get("id"),
                        "login": org.get("login"),
                        "gravatar_id": org.get("gravatar_id"),
                        "url": org.get("url"),
                        "avatar_url": org.get("avatar_url")
                    })
            s3_conn.load_file(
                filename=e_file.name,
                key=f"gh_data/events/{date}/events_{date}_{hour}.csv",
                bucket_name='airflow',
                replace=True
            )
            del e_file
            s3_conn.load_file(
                filename=u_file.name,
                key=f"gh_data/users/{date}/users_{date}_{hour}.csv",
                bucket_name='airflow',
                replace=True
            )
            del u_file
            s3_conn.load_file(
                filename=r_file.name,
                key=f"gh_data/repos/{date}/repos_{date}_{hour}.csv",
                bucket_name='airflow',
                replace=True
            )
            del r_file
            s3_conn.load_file(
                filename=o_file.name,
                key=f"gh_data/orgs/{date}/orgs_{date}_{hour}.csv",
                bucket_name='airflow',
                replace=True
            )
            del o_file