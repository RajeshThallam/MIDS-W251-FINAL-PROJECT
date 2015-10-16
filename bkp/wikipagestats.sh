#!/bin/bash

source_dir="/home/rt/wrk/w251/test"

for file in $(ls -1 $source_dir/*.gz)
do
    base=$(basename $file)
	page_date = $(echo $base | cut -f2 -d"-")
	page_time = $(echo $base | cut -f3 -d"-" | cut -c1-4)
	gunzip -c $file | awk -F" " -V dt=$page_date -V tm=$page_time '$0 ~ /en (.*) ([0-9].*) ([0-9].*)/ && $2 ~ !/(.*).(jpg|gif|png|JPG|GIF|PNG|txt|ico)/ { print dt tm, $2, $3 }'
done

sort -k2,2 -k1,1

diff count[1] - count[0]

awk -F" " 'BEGIN { 
		prev_dttm  = ""; 
		prev_key   = "";
		prev_count = 0;
	} 
	{ 
		if ( prev_key == $2 )
		{
		    if ( prev_count == 0 )
				prev_count = 1;
			print $2, $1, prev_dttm, ($2/prev_count - 1);
		}		 
		prev_dttm  = $1; 
		prev_key   = $2; 
		prev_count = $3;
	}'
	
