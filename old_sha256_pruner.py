#!/usr/bin/env python3
import pathlib
import time

import requests
import re
import argparse
from typing import List, Dict, Set, Optional
from datetime import datetime, timedelta

sha_digest = str  # e.g. "19cb0d56000e91966025d08f345f751d90882f87aad2a6af7c4602b72225aacf"

# Match QCI sha tags like "20230607_sha256_23f8ac379575c13c8c1eb1d68f8e0334f978174fdbbf97380186e5325461b558"
digest_tag_match = re.compile(r"^(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)_sha256_(?P<digest>[0-9a-f]+)$")
references: Dict[sha_digest, List] = dict()


def delete_tag(repository: str, tag: str, token: str):
    """Delete a tag from a Quay.io repository"""
    delete_url = f"https://quay.io/api/v1/repository/{repository}/tag/{tag}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.delete(delete_url, headers=headers)
    if response.status_code == 204:
        print(f"Tag '{tag}' deleted successfully")
    else:
        print(f"Failed to delete tag '{tag}': {response.status_code} {response.text}")


def fetch_tags(repository: str, token: str, page: int = 1, like: Optional[str] = None):
    """Fetch tags from the Quay.io repository with pagination"""
    like_adder = ''
    if like:
        like_adder = f'&filter_tag_name=like:{like}'
    tags_url = f"https://quay.io/api/v1/repository/{repository}/tag/?page={page}&limit=100&onlyActiveTags=true" + like_adder
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(tags_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        tags = data.get("tags", [])
        has_more = data.get("has_additional", False)
        return tags, has_more
    else:
        raise IOError(f"Failed to fetch tags: {response.status_code} {response.text}")


if __name__ == '__main__':
    start_time = datetime.now()
    parser = argparse.ArgumentParser(description="Process some optional arguments.")

    parser.add_argument('--token', type=str, help='quay.io oauth application token')
    parser.add_argument('--confirm', action='store_true', help='Actually delete and refresh tags')

    # Parse the arguments
    args = parser.parse_args()
    if not args.token:
        print('OAuth token is required')
        exit(1)
    token = args.token
    confirm = args.confirm

    # Fetch all tags with pagination
    page = 1
    has_more = True

    prune_target_tags = set()
    pruned_tags = set()
    tag_count = 0
    mod_by = 5
    while has_more:
        retries = 3
        while retries > 0:
            try:
                tags, has_more = fetch_tags('openshift/ci', token, page, like='_sha256_')
                break
            except Exception as e:
                print(f'Error retrieving tags: {e}')
                time.sleep(5)
                retries -= 1
                if retries == 0:
                    raise

        # Iterate through tags and delete those that match the pattern "%_sha256_%"
        for tag in tags:
            tag_count += 1
            image_tag = tag['name']

            if tag_count % mod_by == 0:
                mod_by = min(mod_by * 2, 1000)
                print(f'{tag_count} tags have been checked')

            match = digest_tag_match.match(image_tag)
            if match:
                digest: sha_digest = match.group('digest')
                year = int(match.group('year'))
                month = int(match.group('month'))
                day = int(match.group('day'))
                digest_tag_date = datetime(year, month, day)

                date_difference = start_time - digest_tag_date
                days_difference = date_difference.days
                if days_difference > 5 and image_tag not in prune_target_tags:
                    prune_target_tags.add(image_tag)
                    if confirm:
                        try:
                            delete_tag('openshift/ci', tag=image_tag, token=token)
                            print(f'Removed {image_tag}')
                            pruned_tags.add(image_tag)
                        except Exception as e:
                            print(f'Error while trying to delete tag {image_tag}: {e}')
                    else:
                        print(f'Would have removed {image_tag}')

        page += 1

    finish_time = datetime.now()
    print(f'Duration: {finish_time - start_time}')
    print(f'Total tags scanned: {tag_count}')
    print(f'Tags pruned (if --confirm): {len(prune_target_tags)}')
    print(f'Tags actually pruned: {len(pruned_tags)}')
