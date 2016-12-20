#!/bin/bash

for pi in $(cat /home/bmcgough/storath/treesize-folder-owners.txt | awk -F= '{print $1}'); do grep $pi /home/bmcgough/storath/treesize-folder-owners.txt | awk -F= '{print $2}' | awk -v pi="$pi" -F: '{for(i=1; i <= NF; i++){ print $i","pi}}'; done
