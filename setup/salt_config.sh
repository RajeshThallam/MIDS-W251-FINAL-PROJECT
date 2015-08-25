# Note:  Edit for hosts and public key

# on spark1
export PRIVATE_IP1=10.55.119.69
export PRIVATE_IP2=10.55.119.70
export PRIVATE_IP3=10.55.119.71

ssh-keygen -N '' -f ~/.ssh/id_rsa
export PUBLIC_KEY=`cat ~/.ssh/id_rsa.pub | cut -d ' ' -f 2`

curl -o /tmp/install_salt.sh -L https://bootstrap.saltstack.com && sh /tmp/install_salt.sh -Z -M git 2015.5

cat > /etc/salt/roster <<EOF
spark1: $PRIVATE_IP1
spark2: $PRIVATE_IP2
spark3: $PRIVATE_IP3
EOF

mv /etc/salt/master /etc/salt/master~orig
cat > /etc/salt/master <<EOF
file_roots:
  base:
    - /srv/salt
fileserver_backend:
  - roots
pillar_roots:
  base:
    - /srv/pillar
EOF

mkdir -p /srv/{salt,pillar} && service salt-master restart
## NOTE! INTERVENTION REQUIRED
## on a machine configured w/ your softlayer creds
#   slcli vs list # to grab instance ids
## for each host in cluster
#   slcli vs credentials <instanceid> 
salt-ssh -i '*' cmd.run 'uname -a'


#salt-ssh '*' state.host

cat > /srv/salt/top.sls <<EOF
base:
  '*':
    - hosts
    - root.ssh
    - root.bash_profile
EOF


cat > /srv/salt/hosts.sls <<EOF
localhost-hosts-entry:
   host.present:
    - ip: 127.0.0.1
    - names:
      - localhost
      - localhost.localdomain
node1-hosts-entry:
  host.present:
    - ip: $PRIVATE_IP1
    - names:
      - spark1
node2-hosts-entry:
  host.present:
    - ip: $PRIVATE_IP2
    - names:
      - spark2
node3-hosts-entry:
  host.present:
    - ip: $PRIVATE_IP3
    - names:
      - spark3
EOF

mkdir /srv/salt/root

cat > /srv/salt/root/ssh.sls <<EOF
$PUBLIC_KEY:
  ssh_auth.present:
    - user: root
    - enc: ssh-rsa
    - comment: root@spark1
EOF

cat > /srv/salt/root/bash_profile <<'EOF'
# .bash_profile
# Get the aliases and functions
if [ -f ~/.bashrc ]; then
  . ~/.bashrc
fi
# User specific environment and startup programs
export PATH=$PATH:$HOME/bin
# Java
export JAVA_HOME="$(readlink -f $(which java) | grep -oP '.*(?=/bin)')"
# Spark
export SPARK_HOME="/usr/local/spark"
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOF

cat > /srv/salt/root/bash_profile.sls <<EOF
/root/.bash_profile:
  file.managed:
    - source: salt://root/bash_profile
    - overwrite: true
EOF

salt-ssh '*' state.highstate

salt-ssh '*' cmd.run 'yum install -y java-1.8.0-openjdk-headless'

salt-ssh '*' cmd.run "curl http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz | tar -zx -C /usr/local --show-transformed --transform='s,/*[^/]*,spark,'"

cat > /usr/local/spark/conf/slaves << EOF
spark1
spark2
spark3
EOF

source ~/.bash_profile