#!/usr/bin/env python3
import dxf
import os
import re
import yaml
import argparse
from typing import List, Dict
from datetime import datetime, timedelta

sha_digest = str  # e.g. "19cb0d56000e91966025d08f345f751d90882f87aad2a6af7c4602b72225aacf"

# Match QCI sha tags like "20230607_sha256_23f8ac379575c13c8c1eb1d68f8e0334f978174fdbbf97380186e5325461b558"
digest_tag_match = re.compile(r"^(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)_sha256_(?P<digest>[0-9a-f]+)$")
references: Dict[sha_digest, List] = dict()


def add_reference(digest: sha_digest, alias: str):
    if digest not in references:
        references[digest] = list()
    references[digest].append(alias)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process some optional arguments.")

    # Add optional arguments that accept a filename
    parser.add_argument('--references', type=str, help='Filename for references')
    parser.add_argument('--delete-tags', action='store_true', help='Actually delete the tags selected for pruning')

    # Parse the arguments
    args = parser.parse_args()
    references_filename = args.references
    delete_tags = args.delete_tags

    start_time = datetime.now()
    dxf_obj = dxf.DXF('quay.io', repo='openshift/ci')
    dxf_obj.authenticate(username=os.getenv('DXF_USERNAME'), password=os.getenv('DXF_PASSWORD'), actions=['*'])
    last_auth_time = datetime.now()
    reauth_period = timedelta(minutes=20)

    if not references_filename:
        tag_count = 0
        mod_by = 5

        for image_tag in dxf_obj.list_aliases(iterate=True):
            tag_count += 1
            match = digest_tag_match.match(image_tag)
            if match:
                digest: sha_digest = match.group('digest')
                add_reference(digest, image_tag)
            else:
                # This is a non-sha tag and must be preserved. HTTP HEAD on the alias
                # to receive digest information.
                digest: sha_digest = dxf_obj.head_manifest_and_response(image_tag)[0].split(':')[-1]  # 'sha256:daf7f3efba70c9a03045d364a5b08977df8b0e5d48a3e11d1021ec588592e59c' => 'daf7f3efba70c9a03045d364a5b08977df8b0e5d48a3e11d1021ec588592e59c'
                add_reference(digest, image_tag)

            if tag_count % mod_by == 0:
                mod_by = min(mod_by * 2, 1000)
                print(f'{tag_count} tags have been discovered so far')

            current_time = datetime.now()
            elapsed_time = current_time - last_auth_time

            if elapsed_time >= reauth_period:
                # Our token will expire periodically. Occasionally reauthenticate.
                dxf_obj.authenticate(username=os.getenv('DXF_USERNAME'), password=os.getenv('DXF_PASSWORD'), actions=['*'])
                last_auth_time = datetime.now()

        with open('qci_references.yaml', mode='w+') as output:
            print('Discovering complete -- writing results to disk')
            yaml.safe_dump(references, output)
    else:
        print(f'Reading from existing references: {references_filename}')
        with open(references_filename, mode='r') as references_input:
            references = yaml.safe_load(references_input)

    prune_candidates: Dict[sha_digest, List] = dict()
    for digest, tag_list in references.items():
        prune_candidate = True
        for tag in tag_list:
            match = digest_tag_match.match(tag)
            if match:
                year = int(match.group('year'))
                month = int(match.group('month'))
                day = int(match.group('day'))
                digest_tag_date = datetime(year, month, day)

                date_difference = start_time - digest_tag_date
                days_difference = date_difference.days
                if days_difference < 5:
                    # The last promotion for this image is not old enough yet.
                    # Old release payloads may still be being used which reference it.
                    prune_candidate = False

            else:
                prune_candidate = False

            if not prune_candidate:
                # If there is any tag suggesting that we keep this image
                # do no prune and don't bother looking at other tags.
                break

        if prune_candidate:
            prune_candidates[digest] = tag_list

    with open('qci_prune_candidates.yaml', mode='w+') as output:
        print('Prune analysis complete -- writing results to disk')
        yaml.safe_dump(prune_candidates, output)

    tag_prune_count = len(prune_candidates)
    if not delete_tags:
        print(f'Found {tag_prune_count} tags that will be deleted with --delete-tags.')
    else:
        print(f'Found {tag_prune_count} tags that will be deleted.')
        for digest, tag_list in prune_candidates.items():
            for tag in tag_list:
                print(f'Deleting: {tag}')
                dxf_obj.del_alias(tag)

            current_time = datetime.now()
            elapsed_time = current_time - last_auth_time
            if elapsed_time >= reauth_period:
                # Our token will expire periodically. Occasionally reauthenticate.
                dxf_obj.authenticate(username=os.getenv('DXF_USERNAME'), password=os.getenv('DXF_PASSWORD'), actions=['*'])
                last_auth_time = datetime.now()

    finish_time = datetime.now()
    print(f'Duration: {finish_time - start_time}')
