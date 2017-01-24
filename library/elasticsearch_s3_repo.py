#!/usr/bin/python
# encoding: utf-8

# Daniel Siechniewicz / nullDowntime Ltd <daniel@nulldowntime.com>

DOCUMENTATION = """
---
module: elasticsearch_s3_repo
short_description: Registers a snapshot repository in ElasticSearch.
description:
  - Manages ElasticSearch snapshot repositories.

version_added: "2.1"
author: "Daniel Siechniewicz / nullDowntime Ltd (daniel@nulldowntime.com)"
options:
  region:
    description:
      - The AWS region to use.
    required: false
    aliases: ['aws_region']
  host:
    description:
      - ElasticSearch endpoint
    required: true
    aliases: ['es_host']
  port:
    description:
      - ElasticSearch endpoint port
    required: False
    default: 9200
  bucket:
    description:
      - [S3] bucket where repository will be saved
    default: null
    aliases: ['s3_bucket']
  endpoint:
    description:
      - S3-compatible storage endpoint. Required when not using real S3
    default: null
    required: False
  repo_name:
    description:
      - Name of the elasticsearch snapshot repository to register/remove
    default: null
    required: true
    aliases: ['snapshot_repository_name']
  access_key:
    description:
      - [AWS] access key to sign the requests. Required to register repo, not required to delete.
    required: false
    aliases: ['s3_access_key']
  secret_key:
    description:
      - [AWS] secret key to sign the requests. Required to register repo, not required to delete.
    required: false
    aliases: ['s3_secret_key']
  max_retries:
    descritpion:
      - Maximum number of retries when trying to create a snapshot
    default: 3
    required: false
  protocol:
    description:
      - Protocol to use, http or https
    default: http
    required: false
  state:
    description:
      - present: register repo, absent: deregister(delete) repo
    default: present
    required: false
  path_style_access:
    description:
      - whether to enable path style access to S3(like) storage
    default: true
    required: false
  compress:
    description:
      - whether to enable metadata compression (indices are always compressed)
    default: true
    required: false
requirements:
  - "python >= 2.6"
"""

EXAMPLES = '''

- elasticsearch_s3_repo:
    region: "eu-west-1"
    access_key: "AKIAJ5CC6CARRKOX5V7Q"
    secret_key: "cfDKFSXEo1CC6gfhfhCARRKOX5V7Q"
    host: "logs-q213lkjalsjda.eu-west-1.es.amazonaws.com"
    bucket: "logs"
    endpoint: "my-fake-s3.localdomain.local"
    repo_name: "s3snapshots"
'''

import requests
import json


def get_repo_url(module):
    ''' Takes ansible module object '''
    url = "%s://%s:%d/_snapshot/%s" % (
          module.params['protocol'],
          module.params['host'],
          module.params['port'],
          module.params['repo_name']
    )
    return url


def check_repo_exists(module):
    repo_url = get_repo_url(module)
    resp = requests.get(repo_url)

    out = {'found': False, 'error': False, 'data': None}
    if resp.status_code == 200:
        out['found'] = True
        out['data'] = resp.json()
    elif resp.status_code not in [200, 404]:
        resp.raise_for_status()
        out['error'] = True

    return out


def create_repo(module):
    ''' Takes ansible module object '''
    repo_data = create_repo_data(module)
    repo_url = get_repo_url(module)

    resp = requests.post(
        repo_url,
        data=json.dumps(repo_data))
    resp.raise_for_status()
    return resp


def delete_repo(module):
    ''' Takes ansible module object '''
    repo_url = get_repo_url(module)
    resp = requests.delete(repo_url)
    resp.raise_for_status()
    return resp


def create_repo_data(module):
    ''' Takes ansible module object '''
    repo_data = {
        'type': 's3',
        'settings': {}
    }

    settings = repo_data['settings']

    settings['bucket'] = module.params['bucket']
    settings['endpoint'] = module.params['endpoint']
    settings['region'] = module.params['region']
    settings['access_key'] = module.params['access_key']
    settings['secret_key'] = module.params['secret_key']
    settings['max_retries'] = module.params['max_retries']
    settings['path_style_access'] = module.params['path_style_access']
    settings['compress'] = module.params['compress']
    return repo_data


def main():
    module = AnsibleModule(
        argument_spec=dict(
            host=dict(required=True, aliases=['es_host']),
            port=dict(required=False, type='int', default=9200),
            bucket=dict(required=False, aliases=['s3_bucket']),
            endpoint=dict(required=False, aliases=['s3_endpoint']),
            region=dict(required=False,
                        default='us-east-1',
                        aliases=['aws_region']
                        ),
            access_key=dict(required=False,
                            aliases=['s3_access_key'],
                            no_log=True
                            ),
            secret_key=dict(required=False,
                            aliases=['s3_secret_key'],
                            no_log=True
                            ),
            max_retries=dict(required=False, type='int', default=3),
            protocol=dict(required=False,
                          default='http',
                          choices=['http', 'https']
                          ),
            state=dict(default='present', choices=['present', 'absent']),
            repo_name=dict(required=True,
                           aliases=['snapshot_repository_name']
                           ),
            path_style_access=dict(required=False, default=True, type='bool'),
            compress=dict(required=False, default=True, type='bool'),
        )
    )

    changed = False
    state = module.params['state']

    if state == 'present':
        p = module.params
        msg = []
        if not p['bucket']:
            msg.append('bucket')
        if not p['endpoint']:
            msg.append('endpoint')
        if not p['access_key']:
            msg.append('access_key')
        if not p['secret_key']:
            msg.append('secret_key')
        if msg:
            module.fail_json(
                msg=('required if state=present but not defined: %s' %
                     (', '.join(msg)))
            )

    check_repo = check_repo_exists(module)

    if state == 'present':

        process_repo = False

        if check_repo['found']:
            rd = create_repo_data(module)
            cs = check_repo['data'][module.params['repo_name']]['settings']

            # Check if keys that are present in current settings (read from
            # elasticsearch) are also present in module data. If they are all
            # equal we don't need to update. If we find at least one
            # difference, update is necessary. This is imperfect, as
            # elasticsearch does not return access_key or secret_key, but we
            # must assume we cannot update them. There's also a multitude of
            # other settings that could be set that this ansible module does
            # not know about, but assumption is that repos have been created
            # using this module.
            ms = rd['settings']
            for k in ms:
                if k in cs and str(cs[k]).lower() != str(ms[k]).lower():
                    #changed_msg = 'cs[k] %s != ms[k] %s' % ( str(cs[k]), str(ms[k]) )
                    #module.fail_json(msg='DEBUG', changed_msg=changed_msg)
                    process_repo = True
                    # We found one update, that's enough
                    break
        else:
            process_repo = True

        if process_repo:
            try:
                resp = create_repo(module)
                changed = True
                module.exit_json(changed=changed, repo_data=resp.json())
            except requests.exceptions.RequestException, err_str:
                module.fail_json(msg='Request Failed', reason=err_str)
        else:
            module.exit_json(changed=changed,
                             msg="Repo %s doesn't need an update" % (
                                 module.params['repo_name'])
                             )

    # state=absent and repo has been found in elasticsearch
    elif check_repo['found']:
        try:
            resp = delete_repo(module)
            changed = True
            module.exit_json(changed=changed, repo_data=resp.json())
        except requests.exceptions.RequestException, err_str:
            module.fail_json(msg='Request Failed', reason=err_str)

    # state=absent and repo was not found in elasticsearch
    else:
        module.exit_json(changed=changed,
                         msg='snapshot repository %s not found' % (
                             module.params['repo_name'])
                         )


# import module snippets
from ansible.module_utils.basic import *
main()
