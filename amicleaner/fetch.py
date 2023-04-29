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

    def fetch_available_amis(self):

        """ Retrieve from your aws account your custom AMIs"""

        available_amis = dict()

        my_custom_images = self.ec2.describe_images(Owners=['self'])
        for image_json in my_custom_images.get('Images'):
            ami = AMI.object_with_json(image_json)
            available_amis[ami.id] = ami

        return available_amis

    def fetch_unattached_lc(self):

        """
        Find AMIs for launch configurations unattached
        to autoscaling groups
        """

        resp = self.asg.describe_auto_scaling_groups()
        used_lc = (asg.get("LaunchConfigurationName", "")
                   for asg in resp.get("AutoScalingGroups", []))

        resp = self.asg.describe_launch_configurations()
        all_lcs = (lc.get("LaunchConfigurationName", "")
                   for lc in resp.get("LaunchConfigurations", []))

        unused_lcs = list(set(all_lcs) - set(used_lc))

        resp = self.asg.describe_launch_configurations(
            LaunchConfigurationNames=unused_lcs
        )
        amis = [lc.get("ImageId")
                for lc in resp.get("LaunchConfigurations", [])]

        return amis

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
