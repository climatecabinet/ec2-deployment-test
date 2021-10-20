#!/bin/bash
#
# This needs valid creds so it can pull from git
# prob should be a dedicated user that only has access to what it needs
uname=<username>
utoken=<token>
targetfolder=~/ec2-deployment-test

cd $targetfolder
git pull https://$uname:$utoken@github.com/climatecabinet/ec2-deployment-test.git
source .env/bin/activate
echo -e "\n\n****** Running on `date` ******" >> /home/ubuntu/maintainlog
python run.py helper maintain >> /home/ubuntu/maintainlog 2>&1
