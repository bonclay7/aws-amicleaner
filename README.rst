aws-amicleaner
==============

Cleanup your old unused ami and related snapshots

|Travis CI| |codecov.io| |pypi|

Description
-----------

This tool enables you to clean your custom `Amazon Machine Images (AMI)
<http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html>`__ and
related `EBS Snapshots
<http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSSnapshots.html>`__.

You can either run in ``fetch and clean`` mode where the tool will
retrieve all your private **AMIs** and EC2 instances, exclude AMIs being
holded by your EC2 instances (it can be useful if you use autoscaling,
and so on ...). It applies a filter based on their **names** or **tags**
and a number of **previous AMIs** you want to keep. You can also check and
delete EBS snapshots left orphaned by manual deletion of AMIs.

It can simply remove AMIs with a list of provided ids.

Prerequisites
-------------

-  `awscli <http://docs.aws.amazon.com/cli/latest/userguide/installing.html>`__
-  `python 2.7 or 3+`
-  `python pip <https://pip.pypa.io/en/stable/installing/>`__

This tool assumes your AWS credentials are in your environment, either with AWS
credentials variables :

.. code:: bash

    export AWS_DEFAULT_REGION='your region'
    export AWS_ACCESS_KEY_ID='with token Access ID'
    export AWS_SECRET_ACCESS_KEY='with token AWS Secret'

or with ``awscli`` :

.. code:: bash

    export AWS_PROFILE=profile-name

Minimum AWS IAM permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run the script properly, your ``aws`` user must have at least these
permissions in ``iam``:

.. code:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Stmt1458638250000",
                "Effect": "Allow",
                "Action": [
                    "ec2:DeleteSnapshot",
                    "ec2:DeregisterImage",
                    "ec2:DescribeImages",
                    "ec2:DescribeInstances",
                    "ec2:DescribeSnapshots",
                    "autoscaling:DescribeAutoScalingGroups",
                    "autoscaling:DescribeLaunchConfigurations"
                ],
                "Resource": [
                    "*"
                ]
            }
        ]
    }

Installation
------------

amicleaner is available on pypi and can be installed on your system with pip

From pypi
~~~~~~~~~

.. code:: bash

    [sudo] pip install aws-amicleaner

From source
~~~~~~~~~~~

You can also clone or download from github the source and install with pip

.. code:: bash

    cd aws-amicleaner/
    pip install [--user] -e .

With docker
~~~~~~~~~~~

You can also clone or download from github the source and build a docker image

.. code:: bash

    cd aws-amicleaner/
    docker build -t amicleaner .
    docker run -it --rm amicleaner

Usage
-----


Getting help
~~~~~~~~~~~~

.. code:: bash

    amicleaner --help


Fetch and clean
~~~~~~~~~~~~~~~

Print report of groups and amis to be cleaned

.. code:: bash

    amicleaner --full-report

Keep previous number of AMIs

.. code:: bash

    amicleaner --full-report --keep-previous 10

Regroup by name or tags

.. code:: bash

    amicleaner --mapping-key tags --mapping-values role env

Exclude amis based on tag values

.. code:: bash

    amicleaner --mapping-key tags --mapping-values role env -excluded-mapping-values prod

Skip confirmation, can be useful for automation

.. code:: bash

    amicleaner -f --keep-previous 2


Activate orphan snapshots checking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    amicleaner --check-orphans


Delete a list of AMIs
~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    amicleaner --from-ids ami-abcdef01 ami-abcdef02


.. |Travis CI| image:: https://travis-ci.org/bonclay7/aws-amicleaner.svg?branch=master
   :target: https://travis-ci.org/bonclay7/aws-amicleaner
.. |codecov.io| image:: https://codecov.io/github/bonclay7/aws-amicleaner/coverage.svg?branch=master
   :target: https://codecov.io/github/bonclay7/aws-amicleaner?branch=master
.. |pypi| image:: https://img.shields.io/pypi/v/aws-amicleaner.svg
   :target: https://pypi.python.org/pypi/aws-amicleaner


See this `blog article
<http://techblog.d2-si.eu/2017/06/15/cleaning-your-amazon-machine-images.html>`__
for more information.
