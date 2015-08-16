#!/bin/bash

# this data ingestion script downloads raw wiki page traffic files from 
# dumps.wikimedia.org and uploads to swift object storage. The files are 
# available at hourly frequency for batch upload (historical).

# this script requires python swiftclient to be installed.
#    sudo yum install python-setuptools
#    sudo easy_install pip
#    sudo pip install --upgrade setuptools
#    sudo pip install python-swiftclient

# flow to download the files from wikidump and upload to swift
# 1. download files from dumps.wikimedia.org
#    1.1 for year 2015 onwards, loop through each month configured
#        1.1.1 create list of files available to download (only pageount files)
#        1.1.2 loop though each hour (distributed on three nodes)
#            1. get files for that hour from all days of the month
#            2. if download fails, script exits
#            3. else get ready to upload to swift
#            4. upload file to swift container
#            5. if upload fails, script exits

print_stats() { 
    echo "[`hostname`] [`date`] # of files downloaded from wiki dumps = ${download_file_count}"
    echo "[`hostname`] [`date`] # of files uploaded to swift storage  = ${upload_file_count}"
}

# invoke configuration file
export SCRIPT_HOME=/root/wrk/wiki
. ${SCRIPT_HOME}/project_wiki.cfg

[ ! -e ${download_dir} ] && mkdir ${download_dir}

# define variables
log_file="download.`hostname`.log"
download_complete="download.complete.`hostname`"
#months="2015-02 2015-03 2015-04 2015-05 2015-06 2015-07 2015-08"
months="2015-01"

case `hostname` in
   "spark1")
       start=0; 
       end=7;
   ;;
   "spark2")
       start=8; 
       end=15;
   ;;
   "spark3")
       start=16; 
       end=23;
   ;;
esac

exec 3>&1 1>>${log_dir}/${log_file} 2>&1

echo "[`hostname`] [`date`] removing previously created download complete files"
rm -f ${download_dir}/${download_complete}

echo "[`hostname`] [`date`] preparing to download files on `hostname`"

download_file_count=0
upload_file_count=0

# set year and month
for month in ${months}
do
    echo "[`hostname`] [`date`] downloading for year and month ${month}"
    l_download_url="${download_url}/${month}"
    wget -nc -q -P ${download_dir} ${l_download_url}
    cat ${download_dir}/${month} | grep "<li>" | awk -F'\"' '{print $2}'  | grep '^pagecounts-' > ${download_dir}/download_file.lst 
    
    cd ${download_dir}
    
    # set hour
    for (( hr=$start; hr<=$end; hr++ ))
    do    
        hr_1=`printf %02d ${hr}`
        echo "[`hostname`] [`date`] downloading files for hour ${hr}"
        for file in `awk -F"-" -vhr="$hr_1" '{ if ( substr($3,1,2) == hr1 ) {print $0} }' ${download_dir}/download_file.lst`
        do
            echo "[`hostname`] [`date`] starting to download file ${file}"
            l_download_url_file="${l_download_url}/${file}"
            
            # download file
            [ -f ${download_dir}/${file} ] && rm -f ${download_dir}/${file}
            wget -q -P ${download_dir} ${l_download_url_file}
            if [ $? -eq 0 ]
            then
                download_file_count=`expr $download_file_count + 1`
                echo "[`hostname`] [`date`] downloaded file ${file}"

                # upload files to openstack swift
                echo "[`hostname`] [`date`] starting to upload file ${file} to object storage"
                swift upload ${swift_container} ${file}
                if [ $? -eq 0 ]
                then
                    upload_file_count=`expr $upload_file_count + 1`
                    echo "[`hostname`] [`date`] completed uploading file ${file} to object storage"
                    echo "[`hostname`] [`date`] removing raw file ${download_dir}/${file}"
                    [ -f ${download_dir}/${file} ] && rm -f ${download_dir}/${file}
                else
                    echo "[`hostname`] [`date`] failed to upload file ${file} to object storage. exiting script"
                    print_stats
                    exit 1
                fi
            else
                echo "[`hostname`] [`date`] failed to download file ${l_download_url_file}. exiting script"
                print_stats
                exit 2
            fi
        done
        # list openstack swift container and object storage
        echo "[`hostname`] [`date`] listing contents of the container ${swift_container}"
        swift list ${swift_container} --long
    done
done

print_stats

touch ${download_dir}/${download_complete}
echo "[`hostname`] [`date`] download from wiki dumps and upload to swift completed"