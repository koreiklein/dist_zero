#!/bin/bash

# For provisioning the base aws centos image to be used when spawning new dist_zero instances on aws.
# This script was written to run on a fresh instance of the "CentOS 7 (x86_64) - with Updates HVM" aws marketplace
# ami, described here: https://aws.amazon.com/marketplace/pp/B00O7WM7QW?ref=cns_1clkPro

# A small amount of additional provisioning will need to take place to configure each specific instance once
# it has been spawned.

# Set the domain name of the server being used to build the image.
MYSERVER=ec2-54-202-121-134.us-west-2.compute.amazonaws.com

RSYNC='rsync -avz -e "ssh -oStrictHostKeyChecking=no -i .keys/dist_zero.pem" --exclude "*.pyc" --exclude "*__pycache__*" --exclude ".pytest_cache*" --exclude ".tmp*" --exclude ".git*" --exclude ".keys*"'

bash -c "$RSYNC scripts/dist-zero.service centos@$MYSERVER:dist-zero.service"

ssh -i .keys/dist_zero.pem centos@$MYSERVER <<EOF
  sudo yum -y update
  sudo yum -y install yum-utils
  sudo yum -y groupinstall development
  # The default yum packages do not yet (7/23/2018) have python 3.  We need to use packages from https://ius.io/
  sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm
  sudo yum -y install python36u
  sudo yum -y install python36u-pip
  sudo pip3.6 install --upgrade pip
  sudo pip3.6 install pipenv==2018.5.18

  sudo mkdir -p /dist_zero
  sudo mkdir -p /logs
  sudo mkdir -p /load_balancer

  # Set up dist_zero user
  sudo useradd dist_zero
  sudo mkdir -p ~dist_zero/.ssh
  sudo cp ~centos/.ssh/authorized_keys ~dist_zero/.ssh/authorized_keys
  sudo chown dist_zero:dist_zero ~dist_zero/.ssh/authorized_keys /dist_zero /logs

  # Set up dist-zero daemon as a systemd service Unit
  sudo chown root:root dist-zero.service
  sudo mv dist-zero.service /lib/systemd/system/dist-zero.service
  sudo systemctl daemon-reload
  # Do not enable or actually run the dist-zero daemon until the specific instance is spawned.


  sudo bash -c "echo 'Cmnd_Alias DIST_ZERO_SERVICE = /usr/bin/systemctl start dist-zero, /usr/bin/systemctl stop dist-zero, /usr/bin/systemctl reload dist-zero, /usr/bin/systemctl restart dist-zero, /usr/bin/systemctl status dist-zero, /usr/bin/systemctl enable dist-zero, /usr/bin/systemctl disable dist-zero' >> /etc/sudoers"
  sudo bash -c "echo 'Cmnd_Alias HAPROXY_SERVICE = /usr/bin/systemctl enable rh-haproxy18-haproxy, /usr/bin/systemctl start rh-haproxy18-haproxy, /usr/bin/systemctl reload rh-haproxy18-haproxy, /usr/bin/systemctl disable rh-haproxy18-haproxy, /usr/bin/systemctl stop rh-haproxy18-haproxy' >> /etc/sudoers"

  sudo bash -c "echo 'dist_zero ALL=(root) NOPASSWD: DIST_ZERO_SERVICE' >> /etc/sudoers"
  sudo bash -c "echo 'dist_zero ALL=(root) NOPASSWD: /sbin/sysctl -w net.ipv4.tcp_tw_recycle=*' >> /etc/sudoers"
EOF

# Rsync some files over.  When a specific instance is spawned, these should be rsynced again to send
# over any updates.
bash -c "$RSYNC dist_zero dist_zero@$MYSERVER:/dist_zero/"
bash -c "$RSYNC Pipfile dist_zero@$MYSERVER:/dist_zero/Pipfile"
bash -c "$RSYNC Pipfile.lock dist_zero@$MYSERVER:/dist_zero/Pipfile.lock"

ssh -i .keys/dist_zero.pem dist_zero@$MYSERVER <<EOF
  cd /dist_zero
  pipenv --python 3.6.5
  pipenv sync
EOF

# Install and enable haproxy
ssh -i .keys/dist_zero.pem dist_zero@$MYSERVER <<EOF
  sudo yum -y install centos-release-scl
  sudo yum -y install rh-haproxy18-haproxy rh-haproxy18-haproxy-syspaths socat
  sudo chown dist_zero:dist_zero /etc/haproxy/haproxy.cfg
  sudo systemctl enable rh-haproxy18-haproxy
  sudo systemctl start rh-haproxy18-haproxy

  # Necessary for haproxy to be able to proxy http to servers on nonstandard http ports
  sudo semanage port --add --type http_port_t --proto tcp 10000-20000
EOF

# Remaining steps before we can actually run the dist-zero daemon:
#   - start an instance from the image
#   - rsync any changes to ./Pipfile, ./Pipfile.lock or ./dist_zero/
#   - copy a .env file to the instance
#   - copy a machine_config.json file to the instance
#   - use systemd to enable and start the dist-zero service unit

