slcli vs create -D mids-rt-w251.com -H 'spark1' -c 4 -m 8192 -o CENTOS_LATEST_64 -d sjc01 --disk 100 -k rt-sl-ssh -n100 --vlan-private 925549 --vlan-public 925547
slcli vs create -D mids-rt-w251.com -H 'spark2' -c 4 -m 8192 -o CENTOS_LATEST_64 -d sjc01 --disk 100 -k rt-sl-ssh -n100 --vlan-private 925549 --vlan-public 925547
slcli vs create -D mids-rt-w251.com -H 'spark3' -c 4 -m 8192 -o CENTOS_LATEST_64 -d sjc01 --disk 100 -k rt-sl-ssh -n100 --vlan-private 925549 --vlan-public 925547
slcli vs create -D mids-rt-w251.com -H 'elastisearch1' -c 4 -m 8192 -o CENTOS_LATEST_64 -d sjc01 --disk 100 -k rt-sl-ssh -n100 --vlan-private 925549 --vlan-public 925547
slcli vs create -D mids-rt-w251.com -H 'elastisearch2' -c 4 -m 8192 -o CENTOS_LATEST_64 -d sjc01 --disk 100 -k rt-sl-ssh -n100 --vlan-private 925549 --vlan-public 925547