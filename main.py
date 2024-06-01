#!/usr/bin/env python3
import dxf
import os
import re
import yaml
import argparse
from typing import List, Dict, Set
from datetime import datetime, timedelta

sha_digest = str  # e.g. "19cb0d56000e91966025d08f345f751d90882f87aad2a6af7c4602b72225aacf"

# Match QCI sha tags like "20230607_sha256_23f8ac379575c13c8c1eb1d68f8e0334f978174fdbbf97380186e5325461b558"
digest_tag_match = re.compile(r"^(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)_sha256_(?P<digest>[0-9a-f]+)$")
references: Dict[sha_digest, List] = dict()


def add_reference(digest: sha_digest, alias: str):
    if digest not in references:
        references[digest] = list()
    references[digest].append(alias)


def build_sha_tag(dt: datetime, digest: sha_digest):
    """
    Given a datetime and digest (e.g. "19cb0d56000e91966025d08f345f751d90882f87aad2a6af7c4602b72225aacf"),
    returns YYYYMMDD_sha256_<digest>
    :param dt: The date for the tag.
    :param digest: The image digest
    :return: A sha256 tag using the QCI format.
    """
    date_prefix = dt.strftime("%Y%m%d")
    return f'{date_prefix}_sha256_{digest}'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process some optional arguments.")

    # Add optional arguments that accept a filename
    parser.add_argument('--confirm', action='store_true', help='Actually delete and refresh tags')

    # Parse the arguments
    args = parser.parse_args()
    confirm = args.confirm

    start_time = datetime.now()

    sanity_match = digest_tag_match.match(build_sha_tag(start_time, '0000'))
    if not sanity_match or int(sanity_match.group('year')) != start_time.year or int(sanity_match.group('month')) != start_time.month or int(sanity_match.group('day')) != start_time.day:
        # Sanity check the tag builder.
        raise IOError('Logic error. Building unmatchable tags.')

    dxf_obj = dxf.DXF('quay.io', repo='openshift/ci')
    dxf_obj.authenticate(username=os.getenv('DXF_USERNAME'), password=os.getenv('DXF_PASSWORD'), actions=['*'])
    last_auth_time = datetime.now()
    reauth_period = timedelta(minutes=20)

    prune_target_tags: Set[str] = set()
    pruned_tags: Set[str] = set()
    tag_count = 0
    refresh_count = 0
    mod_by = 5

    for image_tag in dxf_obj.list_aliases(iterate=True):
        tag_count += 1
        match = digest_tag_match.match(image_tag)
        if match:
            digest: sha_digest = match.group('digest')
            year = int(match.group('year'))
            month = int(match.group('month'))
            day = int(match.group('day'))
            digest_tag_date = datetime(year, month, day)
            add_reference(digest, image_tag)

            date_difference = start_time - digest_tag_date
            days_difference = date_difference.days
            if days_difference > 5 and image_tag not in prune_target_tags:
                prune_target_tags.add(image_tag)
                # Ultimately, we only want images that were promoted to be
                # removed after several days of no longer being referenced
                # by a non-sha tag. To achieve this, the pruner logic
                # should apply a refresher sha tag to every non-sha tagged
                # image each day.
                # Once an image has not been referenced for 5 days straight,
                # the pruner will remove the last sha tag and the image
                # will be garbage collected.
                if confirm:
                    try:
                        dxf_obj.del_alias(image_tag)
                        print(f'Removed {image_tag}')
                        pruned_tags.add(image_tag)
                    except Exception as e:
                        print(f'Error while trying to delete tag {image_tag}: {e}')
                else:
                    print(f'Would have removed {image_tag}')

        else:
            # This is a non-sha tag and must be preserved. HTTP HEAD on the alias
            # to receive digest information.
            digest: sha_digest = dxf_obj.head_manifest_and_response(image_tag)[0].split(':')[-1]  # 'sha256:daf7f3efba70c9a03045d364a5b08977df8b0e5d48a3e11d1021ec588592e59c' => 'daf7f3efba70c9a03045d364a5b08977df8b0e5d48a3e11d1021ec588592e59c'
            add_reference(digest, image_tag)
            # Every time we see such a tag, we retag it with a current datetime sha tag to keep it around for the next 5 days.
            new_tag = build_sha_tag(start_time, digest)
            refresh_count += 1
            if confirm:
                try:
                    dxf_obj.set_alias(new_tag, dxf_obj.get_alias(image_tag))
                    print(f'Refreshed tag for: {image_tag} with {new_tag}')
                except Exception as e:
                    print(f'ERROR applying reference tag {image_tag}')
            else:
                print(f'Would have refreshed tag for: {image_tag} with {new_tag}')

        if tag_count % mod_by == 0:
            mod_by = min(mod_by * 2, 1000)
            print(f'{tag_count} tags have been processed')

        current_time = datetime.now()
        elapsed_time = current_time - last_auth_time

        if elapsed_time >= reauth_period:
            # Our token will expire periodically. Occasionally reauthenticate.
            dxf_obj.authenticate(username=os.getenv('DXF_USERNAME'), password=os.getenv('DXF_PASSWORD'), actions=['*'])
            last_auth_time = datetime.now()

    with open('qci_references.yaml', mode='w+') as output:
        print('Discovery complete -- writing (pre-pruner) registry information to disk')
        yaml.safe_dump(references, output)

    finish_time = datetime.now()
    print(f'Duration: {finish_time - start_time}')
    print(f'Total tags scanned: {tag_count}')
    print(f'Tags refreshed (if --confirm): {refresh_count}')
    print(f'Tags pruned (if --confirm): {len(prune_target_tags)}')
    print(f'Tags actually pruned: {len(pruned_tags)}')
