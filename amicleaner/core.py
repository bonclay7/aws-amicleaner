#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from builtins import object
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

from .resources.config import BOTO3_RETRIES, OWNER_ID
from .resources.models import AMI

from datetime import datetime


class OrphanSnapshotCleaner(object):

    """ Finds and removes ebs snapshots left orphaned """

    def __init__(self, ec2=None, owner_id=None):
        self.ec2 = ec2 or boto3.client('ec2', config=Config(retries={'max_attempts': BOTO3_RETRIES}))
        self.owner_id = owner_id or OWNER_ID

    def get_snapshots_filter(self):

        return [{
            'Name': 'status',
            'Values': [
                'completed',
            ]}, {
            'Name': 'description',
            'Values': [
                'Created by CreateImage*'
            ]
        }]

    def get_owner_id(self, images_json):

        """ Return AWS owner id from a ami json list """

        images = images_json or []

        if not images:
            return None

        return images[0].get("OwnerId", "")

    def fetch(self):

        """ retrieve orphan snapshots """

        resp = self.ec2.describe_images(Owners=[self.owner_id])

        used_snaps = [
            ebs.get("Ebs", {}).get("SnapshotId")
            for image in resp.get("Images")
            for ebs in image.get("BlockDeviceMappings")
        ]
        snap_filter = self.get_snapshots_filter()
        owner_id = self.get_owner_id(resp.get("Images"))

        if not owner_id:
            return []

        # all snapshots created for AMIs
        resp = self.ec2.describe_snapshots(
            Filters=snap_filter, OwnerIds=[owner_id]
        )

        all_snaps = [snap.get("SnapshotId") for snap in resp["Snapshots"]]
        return list(set(all_snaps) - set(used_snaps))

    def clean(self, snapshots):

        """
        actually deletes the snapshots with an array
        of snapshots ids
        """
        count = len(snapshots)

        snapshots = snapshots or []

        for snap in snapshots:
            try:
                self.ec2.delete_snapshot(SnapshotId=snap)
            except ClientError as e:
                self.log("{0} deletion failed : {1}".format(snap, e))
                count -= 1

        return count

    def log(self, msg):
        print(msg)


class AMICleaner(object):

    def __init__(self, ec2=None, owner_id=None):
        self.ec2 = ec2 or boto3.client('ec2', config=Config(retries={'max_attempts': BOTO3_RETRIES}))
        self.owner_id = owner_id or OWNER_ID

    @staticmethod
    def get_ami_sorting_key(ami):

        """ return a key for sorting array of AMIs """

        return ami.creation_date

    def remove_amis(self, amis):

        """
        deregister AMIs (array) and removes related snapshots
        :param amis: array of AMI objects
        """

        failed_snapshots = []

        amis = amis or []
        for ami in amis:
            self.ec2.deregister_image(ImageId=ami.id)
            print("{0} deregistered".format(ami.id))
            for block_device in ami.block_device_mappings:
                if block_device.snapshot_id is not None:
                    try:
                            self.ec2.delete_snapshot(
                                SnapshotId=block_device.snapshot_id
                            )
                    except ClientError:
                        failed_snapshots.append(block_device.snapshot_id)
                    print("{0} deleted\n".format(block_device.snapshot_id))

        return failed_snapshots

    def remove_amis_from_ids(self, ami_ids):

        """
        takes a list of AMI ids, verify on aws and removes them
        :param ami_ids: array of AMI ids
        """

        if not ami_ids:
            return False

        my_custom_images = self.ec2.describe_images(
            Owners=[self.owner_id],
            ImageIds=ami_ids
        )
        amis = []
        for image_json in my_custom_images.get('Images'):
            ami = AMI.object_with_json(image_json)
            amis.append(ami)

        return self.remove_amis(amis)

    def map_candidates(self, candidates_amis=None, mapping_strategy=None):

        """
        Given a dict of AMIs to clean, and a mapping strategy (see config.py),
        this function returns a dict of grouped amis with the mapping strategy
        name as a key.

        example :
        mapping_strategy = {"key": "name", "values": ["ubuntu", "debian"]}
        or
        mapping_strategy = {"key": "tags", "values": ["env", "role"], "excluded": ["master", "develop"]}

        print map_candidates(candidates_amis, mapping_strategy)
        ==>
        {
            "ubuntu": [obj1, obj3],
            "debian": [obj2, obj5]
        }

        or
        ==>
        {
            "prod.nginx": [obj1, obj3],
            "prod.tomcat": [obj2, obj5],
            "test.nginx": [obj6, obj7],
        }
        """

        if not candidates_amis:
            return {}

        mapping_strategy = mapping_strategy or {}

        if not mapping_strategy:
            return candidates_amis

        candidates_map = dict()
        for ami in candidates_amis:
            # case : grouping on name
            if mapping_strategy.get("key") == "name":
                for mapping_value in mapping_strategy.get("values"):
                    if mapping_value in ami.name:
                        mapping_list = candidates_map.get(mapping_value) or []
                        mapping_list.append(ami)
                        candidates_map[mapping_value] = mapping_list
            # case : grouping on tags
            elif mapping_strategy.get("key") == "tags":
                mapping_value = self.tags_values_to_string(
                    ami.tags,
                    mapping_strategy.get("values")
                )
                if mapping_strategy.get("excluded"):
                    for excluded_mapping_value in mapping_strategy.get("excluded"):
                        if excluded_mapping_value not in mapping_value:
                            mapping_list = candidates_map.get(mapping_value) or []
                            mapping_list.append(ami)
                            candidates_map[mapping_value] = mapping_list
                else:
                    mapping_list = candidates_map.get(mapping_value) or []
                    mapping_list.append(ami)
                    candidates_map[mapping_value] = mapping_list

        return candidates_map

    @staticmethod
    def tags_values_to_string(tags, filters=None):
        """
        filters tags(key,value) array and return a string with tags values
        :tags is an array of AWSTag objects
        """

        if tags is None:
            return None

        tag_values = []

        filters = filters or []
        filters_to_string = ".".join(filters)

        for tag in tags:
            if not filters:
                tag_values.append(tag.value)
            elif tag.key in filters_to_string:
                tag_values.append(tag.value)

        return ".".join(sorted(tag_values))

    def reduce_candidates(self, mapped_candidates_ami, keep_previous=0, ami_min_days=-1):

        """
        Given a array of AMIs to clean this function return a subsequent
        list by preserving a given number of them (history) based on creation
        time and rotation_strategy param
        """

        result_amis = []
        result_amis.extend(mapped_candidates_ami)

        if ami_min_days > 0:
            for ami in mapped_candidates_ami:
                f_date = datetime.strptime(ami.creation_date, '%Y-%m-%dT%H:%M:%S.%fZ')
                present = datetime.now()
                delta = present - f_date
                if delta.days < ami_min_days:
                    result_amis.remove(ami)

        mapped_candidates_ami = result_amis

        if not keep_previous:
            return mapped_candidates_ami

        if not mapped_candidates_ami:
            return mapped_candidates_ami

        amis = sorted(
            mapped_candidates_ami,
            key=self.get_ami_sorting_key,
            reverse=True
        )

        return amis[keep_previous:]
