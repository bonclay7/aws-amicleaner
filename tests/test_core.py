# -*- coding: utf-8 -*-

from datetime import datetime
from moto import mock_ec2

from amicleaner.core import AMICleaner, OrphanSnapshotCleaner
from amicleaner.resources.models import AMI, AWSTag, AWSBlockDevice


def test_map_candidates_with_null_arguments():
    assert AMICleaner().map_candidates({}, {}) == {}


def test_tags_values_to_string():
    first_tag = AWSTag()
    first_tag.key = "Key1"
    first_tag.value = "Value1"

    second_tag = AWSTag()
    second_tag.key = "Key2"
    second_tag.value = "Value2"

    third_tag = AWSTag()
    third_tag.key = "Key3"
    third_tag.value = "Value3"

    fourth_tag = AWSTag()
    fourth_tag.key = "Key4"
    fourth_tag.value = "Value4"

    long_tag = AWSTag()
    long_tag.key = "long-tag"
    long_tag.value = "LongValue"

    longer_tag = AWSTag()
    longer_tag.key = "long-tag-longer"
    longer_tag.value = "LongerValue"

    tags = [first_tag, third_tag, second_tag, fourth_tag]
    filters = ["Key2", "Key3"]

    tags_values_string = AMICleaner.tags_values_to_string(tags, filters)
    assert tags_values_string is not None
    assert tags_values_string == "Value2.Value3"

    longer_tags = [first_tag, third_tag, second_tag, fourth_tag, long_tag, longer_tag]
    longer_filters = ["Key2", "Key3", "long-tag-longer"]

    tags_values_string = AMICleaner.tags_values_to_string(longer_tags, longer_filters)
    assert tags_values_string is not None
    assert tags_values_string == "LongerValue.Value2.Value3"


def test_tags_values_to_string_with_none():
    assert AMICleaner.tags_values_to_string(None) is None


def test_tags_values_to_string_without_filters():
    first_tag = AWSTag()
    first_tag.key = "Key1"
    first_tag.value = "Value1"

    second_tag = AWSTag()
    second_tag.key = "Key2"
    second_tag.value = "Value2"

    third_tag = AWSTag()
    third_tag.key = "Key3"
    third_tag.value = "Value3"

    tags = [first_tag, third_tag, second_tag]
    filters = []

    tags_values_string = AMICleaner.tags_values_to_string(tags, filters)
    assert tags_values_string is not None
    assert tags_values_string == "Value1.Value2.Value3"


def test_map_with_names():
    # creating tests objects
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.creation_date = datetime.now()

    second_ami = AMI()
    second_ami.id = 'ami-28c2b349'
    second_ami.name = "ubuntu-20160103"
    second_ami.creation_date = datetime.now()

    third_ami = AMI()
    third_ami.id = 'ami-28c2b350'
    third_ami.name = "debian-20160104"
    third_ami.creation_date = datetime.now()

    # creating amis to drop dict
    candidates = [first_ami, second_ami, third_ami]

    # grouping strategy
    grouping_strategy = {"key": "name", "values": ["ubuntu", "debian"]}

    grouped_amis = AMICleaner().map_candidates(candidates, grouping_strategy)
    assert grouped_amis is not None
    assert len(grouped_amis.get('ubuntu')) == 2
    assert len(grouped_amis.get('debian')) == 1


def test_map_with_tags():
    # tags
    stack_tag = AWSTag()
    stack_tag.key = "stack"
    stack_tag.value = "web-server"

    env_tag = AWSTag()
    env_tag.key = "env"
    env_tag.value = "prod"

    # creating tests objects
    # prod and web-server
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.tags.append(stack_tag)
    first_ami.tags.append(env_tag)
    first_ami.creation_date = datetime.now()

    # just prod
    second_ami = AMI()
    second_ami.id = 'ami-28c2b349'
    second_ami.name = "ubuntu-20160103"
    second_ami.tags.append(env_tag)
    second_ami.creation_date = datetime.now()

    # prod and web-server
    third_ami = AMI()
    third_ami.id = 'ami-28c2b350'
    third_ami.name = "debian-20160104"
    third_ami.tags.append(stack_tag)
    third_ami.tags.append(env_tag)
    third_ami.creation_date = datetime.now()

    # creating amis to drop dict
    candidates = [first_ami, second_ami, third_ami]

    # grouping strategy
    grouping_strategy = {"key": "tags", "values": ["stack", "env"]}
    grouped_amis = AMICleaner().map_candidates(candidates, grouping_strategy)
    assert grouped_amis is not None
    assert len(grouped_amis.get("prod")) == 1
    assert len(grouped_amis.get("prod.web-server")) == 2


def test_map_with_tag_exclusions():
    # tags
    stack_tag = AWSTag()
    stack_tag.key = "stack"
    stack_tag.value = "web-server"

    env_tag = AWSTag()
    env_tag.key = "env"
    env_tag.value = "prod"

    # creating tests objects
    # prod and web-server
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.tags.append(stack_tag)
    first_ami.tags.append(env_tag)
    first_ami.creation_date = datetime.now()

    # just prod
    second_ami = AMI()
    second_ami.id = 'ami-28c2b349'
    second_ami.name = "ubuntu-20160103"
    second_ami.tags.append(env_tag)
    second_ami.creation_date = datetime.now()

    # just web-server
    third_ami = AMI()
    third_ami.id = 'ami-28c2b350'
    third_ami.name = "debian-20160104"
    third_ami.tags.append(stack_tag)
    third_ami.creation_date = datetime.now()

    # creating amis to drop dict
    candidates = [first_ami, second_ami, third_ami]

    # grouping strategy
    grouping_strategy = {"key": "tags", "values": ["stack", "env"], "excluded": ["prod"]}
    grouped_amis = AMICleaner().map_candidates(candidates, grouping_strategy)
    assert grouped_amis is not None
    assert grouped_amis.get("prod") is None
    assert grouped_amis.get("prod.web-server") is None
    assert len(grouped_amis.get("web-server")) == 1


def test_reduce_without_rotation_number():
    # creating tests objects
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.creation_date = datetime(2016, 1, 10)

    # just prod
    second_ami = AMI()
    second_ami.id = 'ami-28c2b349'
    second_ami.name = "ubuntu-20160103"
    second_ami.creation_date = datetime(2016, 1, 11)

    # prod and web-server
    third_ami = AMI()
    third_ami.id = 'ami-28c2b350'
    third_ami.name = "debian-20160104"
    third_ami.creation_date = datetime(2016, 1, 12)

    # creating amis to drop dict
    candidates = [second_ami, third_ami, first_ami]

    assert AMICleaner().reduce_candidates(candidates) == candidates


def test_reduce_without_snapshot_id():
    # creating block device
    first_block_device = AWSBlockDevice()
    first_block_device.snapshot_id = None

    # creating tests objects
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.block_device_mappings.append(first_block_device)

    # creating amis to drop dict
    candidates = [first_ami]

    assert AMICleaner().reduce_candidates(candidates) == candidates


def test_reduce():
    # creating tests objects
    first_ami = AMI()
    first_ami.id = 'ami-28c2b348'
    first_ami.name = "ubuntu-20160102"
    first_ami.creation_date = datetime(2016, 1, 10)

    # just prod
    second_ami = AMI()
    second_ami.id = 'ami-28c2b349'
    second_ami.name = "ubuntu-20160103"
    second_ami.creation_date = datetime(2016, 1, 11)

    # prod and web-server
    third_ami = AMI()
    third_ami.id = 'ami-28c2b350'
    third_ami.name = "debian-20160104"
    third_ami.creation_date = datetime(2016, 1, 12)

    # keep 2 recent amis
    candidates = [second_ami, third_ami, first_ami]
    rotation_number = 2
    cleaner = AMICleaner()
    left = cleaner.reduce_candidates(candidates, rotation_number)
    assert len(left) == 1
    assert left[0].id == first_ami.id

    # keep 1 recent ami
    rotation_number = 1
    left = cleaner.reduce_candidates(candidates, rotation_number)
    assert len(left) == 2
    assert left[0].id == second_ami.id

    # keep 5 recent amis
    rotation_number = 5
    left = cleaner.reduce_candidates(candidates, rotation_number)
    assert len(left) == 0


def test_remove_ami_from_none():
    assert AMICleaner().remove_amis(None) == []


@mock_ec2
def test_fetch_snapshots_from_none():

    cleaner = OrphanSnapshotCleaner()

    assert len(cleaner.get_snapshots_filter()) > 0
    assert type(cleaner.fetch()) is list
    assert len(cleaner.fetch()) == 0


"""
@mock_ec2
def test_fetch_snapshots():
    base_ami = "ami-1234abcd"

    conn = boto3.client('ec2')
    reservation = conn.run_instances(
        ImageId=base_ami, MinCount=1, MaxCount=1
    )
    instance = reservation["Instances"][0]

    # create amis
    images = []
    for i in xrange(5):
        image = conn.create_image(
            InstanceId=instance.get("InstanceId"),
            Name="test-ami"
        )
        images.append(image.get("ImageId"))

    # deleting two amis, creating orphan snpashots condition
    conn.deregister_image(ImageId=images[0])
    conn.deregister_image(ImageId=images[1])

    cleaner = OrphanSnapshotCleaner()
    assert len(cleaner.fetch()) == 0
"""
