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

    def fetch_unattached_lt(self):

        """
        Find AMIs for launch templates unattached
        to autoscaling groups
        """

        resp = self.asg.describe_auto_scaling_groups()
        used_lt = (asg.get("LaunchTemplate", {}).get("LaunchTemplateName")
                   for asg in resp.get("AutoScalingGroups", []))

        resp = self.ec2.describe_launch_templates()
        all_lts = (lt.get("LaunchTemplateName", "")
                   for lt in resp.get("LaunchTemplates", []))

        unused_lts = list(set(all_lts) - set(used_lt))

        amis = []
        for lt_name in unused_lts:
            resp = self.ec2.describe_launch_template_versions(
                LaunchTemplateName=lt_name
            )
            amis.append(lt_latest_version.get("LaunchTemplateData", {}).get("ImageId")
                        for lt_latest_version in resp.get("LaunchTemplateVersions", []))

        return amis

    def fetch_zeroed_asg_lc(self):

        """
        Find AMIs for autoscaling groups who's desired capacity is set to 0
        """

        resp = self.asg.describe_auto_scaling_groups()
        zeroed_lcs = [asg.get("LaunchConfigurationName", "")
                      for asg in resp.get("AutoScalingGroups", [])
                      if asg.get("DesiredCapacity", 0) == 0 and len(asg.get("LaunchConfigurationNames", [])) > 0]

        resp = self.asg.describe_launch_configurations(
            LaunchConfigurationNames=zeroed_lcs
        )

        amis = [lc.get("ImageId", "")
                for lc in resp.get("LaunchConfigurations", [])]

        return amis

    def fetch_zeroed_asg_lt(self):

        """
        Find AMIs for autoscaling groups who's desired capacity is set to 0
        """

        resp = self.asg.describe_auto_scaling_groups()
        # This does not support multiple versions of the same launch template being used
        zeroed_lts = [asg.get("LaunchTemplate", {})
                      for asg in resp.get("AutoScalingGroups", [])
                      if asg.get("DesiredCapacity", 0) == 0]

        zeroed_lt_names = [lt.get("LaunchTemplateName", "")
                        for lt in zeroed_lts]

        zeroed_lt_versions = [lt.get("LaunchTemplateVersion", "")
                        for lt in zeroed_lts]

        resp = self.ec2.describe_launch_templates(
            LaunchTemplateNames=zeroed_lt_names
        )

        amis = []
        for lt_name, lt_version in zip(zeroed_lt_names, zeroed_lt_versions):
            resp = self.ec2.describe_launch_template_versions(
                LaunchTemplateName=lt_name
                # Cannot be empty... Versions=[lt_version] - unsure how to pass param only if present in Python
            )
            amis.append(lt_latest_version.get("LaunchTemplateData", {}).get("ImageId")
                        for lt_latest_version in resp.get("LaunchTemplateVersions", []))

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
