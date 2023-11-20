#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from builtins import object
import boto3
from botocore.config import Config
from .resources.config import BOTO3_RETRIES
from .resources.models import AMI


class Fetcher(object):

    """ Fetches function for AMI candidates to deletion """

    def __init__(self, ec2=None, autoscaling=None):

        """ Initializes aws sdk clients """

        self.ec2 = ec2 or boto3.client('ec2', config=Config(retries={'max_attempts': BOTO3_RETRIES}))
        self.asg = autoscaling or boto3.client('autoscaling')

    #find amis for lc unattached to asg
    def fetch_unattached_lc(self):
        used_lcs = self.get_used_lc_names()

        resp = self.asg.get_paginator('describe_launch_configurations')
        page_iterator = resp.paginate()
        all_lcs = []
        for lc in page_iterator:
            for lcn in lc['LaunchConfigurations']:
                all_lcs.append(lcn['LaunchConfigurationName'])

        unused_lcs = list(set(all_lcs) - set(used_lcs))

        amis = []
        for lc in unused_lcs:
            resp = self.asg.describe_launch_configurations(LaunchConfigurationNames=[lc])
            amis += [lc["ImageId"]
                     for lc in resp.get("LaunchConfigurations", [])]
        """Remove duplicates"""
        amis = list(set(amis))

        #make sure found launch configuration amis are not used by any launch template attached to asg
        used_lt_amis = self.get_used_lt_amis()
        amis = [i for i in amis if i not in used_lt_amis]

        return amis

    #find amis for lt unattached to asg
    def fetch_unattached_lt(self):

        used_lts = self.get_used_lt_ids()

        resp = self.ec2.describe_launch_templates(MaxResults=200)
        all_lts = (lt.get("LaunchTemplateId", "")
                   for lt in resp.get("LaunchTemplates", []))

        unused_lts = list(set(all_lts) - set(used_lts))

        amis = []
        for lt in unused_lts:
            resp = self.ec2.describe_launch_template_versions(LaunchTemplateId=lt, Versions=['$Latest'])
            try:
                amis += [ltd["ImageId"]
                         for ltd in (ltv.get("LaunchTemplateData", [])
                         for ltv in resp.get("LaunchTemplateVersions", []))]
            except KeyError:
               continue
        """Remove duplicates"""
        amis = list(set(amis))

        #make sure found launch template amis are not used by any launch configuration attached to asg
        used_lc_amis = self.get_used_lc_amis()
        amis = [i for i in amis if i not in used_lc_amis]

        return amis

    #get all launch template amis attached to asg
    def get_used_lt_amis(self):
        used_lts = self.get_used_lt_ids()
        used_lt_amis = []
        for lt in used_lts:
            resp = self.ec2.describe_launch_template_versions(LaunchTemplateId=lt, Versions=['$Latest'])
            try:
                used_lt_amis += [ltd["ImageId"]
                         for ltd in (ltv.get("LaunchTemplateData", [])
                         for ltv in resp.get("LaunchTemplateVersions", []))]
            except KeyError:
               continue
        """Remove duplicates"""
        used_lt_amis = list(set(used_lt_amis))

        return used_lt_amis

    #get all launch configuration amis attached to asg
    def get_used_lc_amis(self):
        used_lcs = self.get_used_lc_names()
        used_lc_amis = []
        for lc in used_lcs:
            resp = self.asg.describe_launch_configurations(LaunchConfigurationNames=[lc])
            used_lc_amis += [lc["ImageId"]
                          for lc in resp.get("LaunchConfigurations", [])]
        """Remove duplicates"""
        used_lc_amis = list(set(used_lc_amis))

        return used_lc_amis

    #get all launch configuration names attached to asg
    def get_used_lc_names(self):
        resp = self.asg.get_paginator('describe_auto_scaling_groups')
        page_iterator = resp.paginate()
        lc_names = []
        for page in page_iterator:
            for asg in page['AutoScalingGroups']:
                if "LaunchConfigurationName""" in asg.keys():
                    lc_names.append(asg['LaunchConfigurationName'])

        return lc_names

    #get all launch template IDs attached to asg
    def get_used_lt_ids(self):
        resp = self.asg.get_paginator('describe_auto_scaling_groups')
        page_iterator = resp.paginate()
        lt_ids = []
        for page in page_iterator:
            for asg in page['AutoScalingGroups']:
                if "LaunchTemplate" in asg.keys():
                    lt_ids.append(asg['LaunchTemplate']['LaunchTemplateId'])

        return lt_ids

    def fetch_available_amis(self):

        """ Retrieve from your aws account your custom AMIs"""

        available_amis = dict()

        my_custom_images = self.ec2.describe_images(Owners=['self'])
        for image_json in my_custom_images.get('Images'):
            ami = AMI.object_with_json(image_json)
            available_amis[ami.id] = ami

        return available_amis

    def fetch_zeroed_asg(self):

        """
        Find AMIs for autoscaling groups who's desired capacity is set to 0
        """

        resp = self.asg.describe_auto_scaling_groups()
        # fetch by launch configuration
        zeroed_lcs = [asg.get("LaunchConfigurationName")
                      for asg in resp.get("AutoScalingGroups", [])
                      if asg.get("DesiredCapacity", 0) == 0 and asg.get("LaunchConfigurationName", False)]

        resp = self.asg.describe_launch_configurations(
            LaunchConfigurationNames=zeroed_lcs
        )

        amis = [lc.get("ImageId", "")
                for lc in resp.get("LaunchConfigurations", [])]

        # fetch by launch template
        zeroed_lts = self.get_launch_templates(resp)

        amis += self.get_launch_template_amis(zeroed_lts)

        return amis

    def get_launch_templates(self, asg_resp):
        lts = []
        for asg in asg_resp.get("AutoScalingGroups", []):
            if "LaunchTemplate" in asg.keys():
                lts.append(asg["LaunchTemplate"])
            elif "MixedInstancesPolicy" in asg.keys():
                lts.append(asg["LaunchTemplate"]["LaunchTemplateSpecification"])
        return lts

    def get_launch_template_amis(self, launch_tpls):
        amis = []
        for lt in launch_tpls:
            resp = self.ec2.describe_launch_template_versions(
                LaunchTemplateId=lt["LaunchTemplateId"], Versions=[lt["Version"]])
            amis.append(resp["LaunchTemplateVersions"][0]["ImageId"])
        return amis

    def fetch_instances(self):

        """ Find AMIs for not terminated EC2 instances """

        resp = self.ec2.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': [
                        'pending',
                        'running',
                        'shutting-down',
                        'stopping',
                        'stopped'
                    ]
                }
            ]
        )
        amis = [i.get("ImageId", None)
                for r in resp.get("Reservations", [])
                for i in r.get("Instances", [])]

        return amis
