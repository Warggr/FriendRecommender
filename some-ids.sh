#!/usr/bin/bash

for i in 924 8941 8942 9019 9020 9021 9022 9990 9992 9993; do
	cat output/second-mapper/part-r-00000 | grep -e "^$i\s";
done
