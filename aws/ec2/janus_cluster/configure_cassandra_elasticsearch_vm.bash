#!/bin/bash

#
# Description: This script configures Janus clusters Cassandra and ElasticSearch
# VM during startup of the VMs from a shutdown state.  The primary function of the
# script is to mount needed file systems and copy data sets if needed.
#
# Error codes returned: 0 - success
#                       1 - error parsing arguments
#                       2 - required arguments missing
#                  

SCRIPT_NAME=`basename $0`

function print_usage {
   echo "Usage: `basename $0` [--help]"
   echo "   [--data-mount-type ssd | efs]"
   echo "   [--efs-source-path <path to dataset>"
   echo "   [--ssd-device-1 <device name 1>"
   echo "   [--ssd-device-2 <device name 2>"
   echo "Parameter descriptions:"
   echo "   --data-mount-type (required) The file system type the dataset will be stored on.  If ssd, an xfs file system will be created on a local SSD to store the data and the data will be copied from an EFS path to the ssd file system.  If efs, the EFS file system will be used by the cassandra/elasticsearch system.  In either case, this script assumes that the dataset already exists on EFS."
   echo "   --efs-source-path (required) The full path of the directory containing the dataset."
   echo "   --ssd-device-1 (required if <data-mount-type> is ssd) the device name of the first ssd device"
   echo "   --ssd-device-2 (required if <data-mount-type> is ssd) the device name of the second ssd device"
}

function get_arg_value () {
   if [ "$REMAINING_ARGS_NUM" -eq 0 ]; then
      echo "ERROR($SCRIPT_NAME): Missing argument to '$1' flag"  1>&2
      print_usage
      exit 1
   fi

   if [[ "$2" = "--" ]]; then
      echo "ERROR($SCRIPT_NAME): Missing value for parameter '$1'" 1>&2
      print_usage
      exit 1
   fi

   echo $2
}


function get_boolean_arg_value () {
   if [ "$REMAINING_ARGS_NUM" -eq 0 ]; then
      echo "ERROR($SCRIPT_NAME): Missing argument to '$1' flag"  1>&2
      print_usage
      exit 1
   fi

   if [[ "$2" = "--" ]]; then
      echo "ERROR($SCRIPT_NAME): Missing value for parameter '$1'" 1>&2
      print_usage
      exit 1
   fi

   if [[ "$2" != "true" ]] && [[ "$2" != "false" ]]; then
      echo "ERROR($SCRIPT_NAME): Value of parameter '$1' must be 'true' or 'false'" 1>&2
      print_usage
      exit 1
   fi

   echo $2
}


REMAINING_ARGS_NUM=$#
NEXT_ARG_VAL=false

if [[ $# -eq 0 ]]; then
   echo "ERROR($SCRIPT_NAME): No parameters given" 1>&2
   print_usage
   exit 1
fi

while [ "$REMAINING_ARGS_NUM" -gt 0 ]
do

   REMAINING_ARGS_NUM=`expr $REMAINING_ARGS_NUM - 1`
   param=$1
   shift

   if [[ "$param" = "--help" ]]; then
      print_usage
      exit 0
   fi

   if [[ "${param:0:2}" = "--" ]]; then
      if [[ "$param" = "--data-mount-type" ]]; then
         DATA_MOUNT_TYPE=$(get_arg_value "$param" "$1")
      elif [[ "$param" = "--efs-source-path" ]]; then
         EFS_SOURCE_PATH=$(get_arg_value "$param" "$1")
      elif [[ "$param" = "--ssd-device-1" ]]; then
         SSD_DEVICE_1=$(get_arg_value "$param" "$1")
      elif [[ "$param" = "--ssd-device-2" ]]; then
         SSD_DEVICE_2=$(get_arg_value "$param" "$1")
      else
         echo "ERROR($SCRIPT_NAME): Unknown parameter flag $param" 1>&2
         print_usage
         exit 1
      fi
   else
      echo "ERROR($SCRIPT_NAME): Parameter flag must start with '--'" 1>&2
      print_usage
      exit 1
   fi

   REMAINING_ARGS_NUM=`expr $REMAINING_ARGS_NUM - 1`
   shift
done


if [ -z "$DATA_MOUNT_TYPE" ]; then
   echo "ERROR($SCRIPT_NAME): --data-mount-type parameter not defined" 1>&2
   print_usage
   exit 2
else
   if [[ "$DATA_MOUNT_TYPE" = "ssd" ]]; then
      if [ -z "$SSD_DEVICE_1" ]; then
         echo "ERROR($SCRIPT_NAME): --ssd-device-1 parameter not defined" 1>&2
         print_usage
         exit 2
      elif [ -z "$SSD_DEVICE_2" ]; then
         echo "ERROR($SCRIPT_NAME): --ssd-device-2 parameter not defined" 1>&2
         print_usage
         exit 2
      fi
   elif ! [[ "$DATA_MOUNT_TYPE" = "efs" ]]; then
      echo "ERROR($SCRIPT_NAME): --data-mount-type parameter has unsupported value '"$DATA_MOUNT_TYPE"'"
      print_usage
      exit 2
   fi
fi
         
if [ -z "$EFS_SOURCE_PATH" ]; then
   echo "ERROR($SCRIPT_NAME): --efs-source-path parameter not defined" 1>&2
   print_usage
   exit 2
fi


