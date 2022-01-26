#!/bin/bash

#
# SCRIPT: bcpx
# AUTHOR: Maurice Hickey
# DATE:   Feb 2020
# REV:    
#
# PLATFORM: 
#
# PURPOSE: 
#
# REV LIST:
#
# set -n   
# Uncomment to check script syntax, without execution.
#          
# NOTE: Do not forget to put the comment back in or
#          
#       the shell script will not execute!
# set -x   
# Uncomment to debug this shell script
#

# #######################
# External Functions
# #######################
SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]
do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to 
                                               # resolve it relative to the path where the 
                                               # symlink file was located
done

DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"


if [[ -e ${DIR}/pd_func_lib.sh ]]
then
  source ${DIR}/pd_func_lib.sh
else
  log 'Error: %s not found\n\n' "${DIR}/pd_func_lib.sh" >&2
  exit_abnormal
fi


# #######################
# Variables
# #######################
declare -i dry_run=0
declare -i no_delete=0
declare -i use_delete=0
declare -i use_truncate=0
declare -i using_format=0
declare -i verbose=0
declare -i num_files=0
declare -i gen_fmt=0
declare -i pjobs=1

declare -l database
declare -u hint
declare -l project
declare -l table
declare -a hdfs_files
declare -l datadir

# #######################
# Functions
# #######################
#######################################################################
function template() {

   
   # Expected args - list the args here
   # 1 - arg 1
   # n - arg n
   # n+1 - resultvar - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=1

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   # local var1="$1"
   # local var2="$2"
   local __resultvar=$1

   # end of function setup

   __rc="Tested"
   eval $__resultvar="'${__rc}'"
}

#######################################################################
function checkcmd() {

   
   # Expected args - list the args here
   # 1 - cmd to check for
   # 2 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc
   local __which_cmd

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=2

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __cmd=$1
   local __resultvar=$2

   # end of function setup

   # Check for cmd
   __which_cmd=$(which ${__cmd} 2> /dev/null)

   __rc=$?

   if (( __rc == 1 ))
   then
      log "${__cmd} cmd not found" >&2
   fi 

   if (( verbose == 1 ))
   then
      log "${__cmd} - ${__which_cmd}"
   fi

   eval $__resultvar="${__rc}"

}

#######################################################################
function checkfile() {

   
   # Expected args - list the args here
   # 1 - file to check for
   # 2 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=2

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __file=$1
   local __resultvar=$2

   # end of function setup

   # Check for file
   if [[ -f ${__file} ]]
   then
      __rc=0
   else
      __rc=1
      log "File: ${__file} not found" >&2
   fi

   eval $__resultvar="${__rc}"

}


#######################################################################
function checktable() {

   
   # Expected args - list the args here
   # 1 - host
   # 2 - instance
   # 3 - database
   # 4 - table
   # 5 - user
   # 6 - password
   # 7 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=7

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __host=$1
   local __instance=$2
   local __database=$3
   local __table=$4
   local __user=$5
   local __pwd=$6
   local __resultvar=$7

   local __cmd

   # end of function setup

   __cmd="sqlcmd -S ${__host}\\\\${__instance} -d ${__database} -U ${__user} -Q 'SELECT COUNT(*) FROM ${__table}' -b"

   log "Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      __rc=0
   fi

   eval $__resultvar="${__rc}"

}

#######################################################################
function checktruncatetable {
   
   # Expected args - list the args here
   # 1 - truncate host
   # 2 - truncate instance
   # 3 - truncate database
   # 4 - target database
   # 5 - target table
   # 6 - truncate user
   # 7 - truncate password
   # 8 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=8

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __truncate_host=$1
   local __truncate_instance=$2
   local __truncate_database=$3
   local __target_database=$4
   local __target_table=$5
   local __truncate_user=$6
   local __truncate_pwd=$7
   local __resultvar=$8

   local __cmd

   # end of function setup

   __cmd="sqlcmd -S ${__truncate_host}\\\\${__truncate_instance} -d ${__truncate_database} -U ${__truncate_user} -Q \"SET NOCOUNT ON;SELECT COUNT(*) FROM truncate_table_reference_t where db_nm='${__target_database}' and tbl_nm='${__target_table}'\" -b -h -1"

   log "Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__truncate_pwd}' ${__cmd}"
      __row_count=$(eval "${__cmd}")
      __rc=$?
      if (( __rc == 0 && __row_count == 0 ))
      then
         log "No row found in process config database ${__truncate_database} for target ${__target_database}.${__target_table}" >&2
         __rc=1
      fi
   else
      __rc=0
   fi

   eval $__resultvar="${__rc}"
}


#######################################################################
function truncatetable {
   
   # Expected args - list the args here
   # 1 - truncate host
   # 2 - truncate instance
   # 3 - truncate database
   # 4 - target database
   # 5 - target table
   # 6 - truncate user
   # 7 - truncate password
   # 8 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=8

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __truncate_host=$1
   local __truncate_instance=$2
   local __truncate_database=$3
   local __target_database=$4
   local __target_table=$5
   local __truncate_user=$6
   local __truncate_pwd=$7
   local __resultvar=$8

   local __cmd
   local __mktemp

   # end of function setup

   __mktemp=$(mktemp --suffix=.sql trunc_XXXX)
   __rc=$?

   if (( __rc == 0 ))
   then
      log "${__mktemp} temp SQL file created for truncate"
      echo "SET NOCOUNT ON;EXECUTE proc_updt_truncate_table_reference '${__target_database}','dbo','${__target_table}'" > ${__mktemp}
   else
      log "Temporary file creation for truncate failed" >&2
      exit_abnormal
   fi

   __cmd="sqlcmd -S ${__truncate_host}\\\\${__truncate_instance} -d ${__truncate_database} -U ${__truncate_user} -i ${__mktemp}"

   log "Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__truncate_pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
      if (( __rc == 0 ))
      then
         rm "${__mktemp}"
      fi
   else
      __rc=0
   fi

   eval $__resultvar="${__rc}"
}


#######################################################################
function deletetable {
   
   # Expected args - list the args here
   # 1 - host
   # 2 - instance
   # 3 - database
   # 4 - table
   # 5 - user
   # 6 - password
   # 7 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=7

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __host=$1
   local __instance=$2
   local __database=$3
   local __table=$4
   local __user=$5
   local __pwd=$6
   local __resultvar=$7

   local __cmd

   # end of function setup

   __cmd="sqlcmd -S ${__host}\\\\${__instance} -d ${__database} -U ${__user} -Q \"SET NOCOUNT ON;DELETE FROM ${__table}\" -b -h -1"

   log "Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      __rc=0
   fi

   eval $__resultvar="${__rc}"
}

#######################################################################
function fetchfmt() {

   
   # Expected args - list the args here
   # 1  - host
   # 2  - instance
   # 3  - database
   # 4  - table
   # 5  - user
   # 6  - password
   # 7  - bcp directory
   # 8  - project
   # 9  - field terminator
   # 10 - row terminator
   # 11 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=11

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __host=$1
   local __instance=$2
   local __database=$3
   local __table=$4
   local __user=$5
   local __password=$6
   local __bcpdir=$7
   local __project=$8
   local __fieldterm=$9
   local __rowterm=${10}
   local __resultvar=${11}

   local __fmtfile
   local __cmd

   # end of function setup

   __fmtfile="${__bcpdir}/${__project}/${__table}/fmt/${__table}.fmt"

   __rc=0

   # If fmt file exists then do not overwrite
   if [[ -f "${__fmtfile}" ]]
   then
      log "${__fmtfile} already exists, will not be overwritten"
   else
      __cmd="bcp ${__table} format nul -f ${__fmtfile} -S ${__host}\\\\${__instance} -d ${__database} -U ${__user} -c -t '${__fieldterm}' -P '${__password}' -r ${__rowterm}"

      log "Executing cmd: ${__cmd}"

      if (( dry_run == 0 ))
      then
         __cmd="${__cmd} -P '${__password}'"
         eval "${__cmd}"
         __rc=$?
      else
         __rc=0
      fi
   fi

   eval $__resultvar="${__rc}"

}

#######################################################################
function checkbcpdirs() {

   
   # Expected args - list the args here
   # 1 - bcpdir
   # 2 - project
   # 3 - table
   # 4 - return variable name

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=4

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __bcpdir=$1
   local __project=$2
   local __table=$3
   local __resultvar=$4

   # end of function setup

   if [[ -d ${__bcpdir} ]]
   then
      if [[ -d ${__bcpdir}/${__project}/${__table} ]]
      then
         for d in data err fmt log
         do
            if [[ -d ${__bcpdir}/${__project}/${__table}/${d} ]]
            then
               :
            else
               mkdir -p ${__bcpdir}/${__project}/${__table}/${d}
               __rc=$?
               if (( __rc == 0 ))
               then
                  log "${__bcpdir}/${__project}/${__table}/${d} directory created"
               else
                  log "${__bcpdir}/${__project}/${__table}/${d} directory creation failed" >&2
                  __rc=1
                  break
               fi
            fi
         done
      else
         if (mkdir -p ${__bcpdir}/${__project}/${__table}/fmt && \
             mkdir -p ${__bcpdir}/${__project}/${__table}/data &&
             mkdir -p ${__bcpdir}/${__project}/${__table}/log &&
             mkdir -p ${__bcpdir}/${__project}/${__table}/err)
         then
            log "Project ${__project}, table ${__table} directories created"
            __rc=0
         else
            log "Could not create project ${__project}/${__table} directories" >&2
            __rc=1
         fi
      fi
   else
      log "${__bcpdir} does not exist." >&2
      __rc=1
   fi

   eval $__resultvar="${__rc}"

}
#######################################################################
function checksrcdir() { 

   
   # Expecte dargt - list the args here
   # 1 - loadingdock
   # 2 - hql base
   # 3 - project
   # 4 - table
   # 5 - datadir
   # 6 - use_cli
   # 7 - resultvar - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc
   local __cmd
   local __uri

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=7

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   # local var1="$1"
   # local var2="$2"
   local __loadingdock=$1
   local __hqlbase=$2
   local __project=$3
   local __table=$4
   local __datadir=$5
   local -i __use_cli=$6
   local __resultvar=$7

   # end of function setup

   if [[ "$__table" == "${__datadir}" ]]
   then
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__table}"
   else
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__datadir}"
   fi

   if (( __use_cli == 0 ))
   then
      __cmd="hdfs dfs -test -d s3a://${__uri}"
   else
      __cmd="aws s3 ls s3://${__uri}/ > /dev/null"
   fi

   log "Executing cmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      __rc=0
   fi

   eval $__resultvar="'${__rc}'"
}

#######################################################################
function getdatafilelist() {
   
   # Expected args - list the args here
   # 1 - loadingdock
   # 2 - hqlbase
   # 3 - project
   # 4 - table
   # 5 - datadir
   # 6 - use_cli
   # 7 - resultvar - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=7

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __loadingdock=$1
   local __hqlbase=$2
   local __project=$3
   local __table=$4
   local __datadir=$5
   local -i __use_cli=$6
   local __resultvar=$7

   local -a files
   local -i v=0
   local -i __f=0
   local __uri
   local -i __numfiles

   # end of function setup

   if [[ "${__table}" == "${__datdir}" ]]
   then
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__table}/"
   else
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__datadir}/"
   fi

   if (( __use_cli == 0 ))
   then
      __cmd="hdfs dfs -ls s3a://${__uri}"
   else
      __cmd="aws s3 ls s3://${__uri}"
   fi

   log "Executing cmd: ${__cmd}"

   if (( dry_run == 0 ))
   then

      # hdfs dfs -ls output, field 8 is the actual file name, aws cli it is field 4
      if (( __use_cli == 0 ))
      then
         for f in $(hdfs dfs -ls s3a://${__uri} | gawk -e '{print $8}')
         do
           echo $v $f
           files[(( v++ ))]=$f
         done
      else
         for f in $(aws s3 ls s3://${__uri} | sed -e '/PRE .*\//d' | gawk --assign URI=s3://${__uri} --source '{printf "%s%s\n", URI, $4}')
         do
           echo $v $f
           files[(( v++ ))]=$f
         done
      fi

      __numfiles=${#files[@]}

      if (( __numfiles == 0 ))
      then
         log "No files found to process at ${__uri}" >&2
      else

         log "${__numfiles} files will be processed from ${__uri}"
         log "First few are:"

         if (( __numfiles > 10 )) 
         then
            __f=10
         else
            __f=$(( __numfiles - 1 ))
         fi

         for v in $(seq 0 ${__f})
         do
            printf "Index %-5d: , %s\n" $v ${files[${v}]}
         done
      fi

   fi

   eval $__resultvar="'${files[@]}'"

}

#######################################################################
function cleandatadir  { 
   
   # Args
   # 1 - bcp directory
   # 2 - project
   # 2 - table
   # 4 - resultvar - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=4

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __bcpdir="$1"
   local __project="$2"
   local __table="$3"
   local __resultvar=$4

   # end of function setup

   local -a __filelist

   __filelist=($(ls -1 ${__bcpdir}/${__project}/${__table}/data))

   log "${#__filelist[@]} files will be deleted from ${__bcpdir}/${__project}/${__table}/data"

   __rc=0
   for f in "${__filelist[@]}"
   do
      __cmd="rm ${__bcpdir}/${__project}/${__table}/data/$f"
      log "Executing cmd: ${__cmd}"

      if (( dry_run == 0 ))
      then
         eval "${__cmd}"
         __rc=$?
         if (( __rc != 0 ))
         then
            log "Error deleting ${__bcpdir}/${__project}/${__table}/data/$f" >&2
            __rc=1
            break
         fi
      fi
   done

   eval $__resultvar="'${__rc}'"
}

#######################################################################
function bcpdata {

   # Expected args - list the args here
   #  1 - loadingdock
   #  2 - hqlbase
   #  3 - project
   #  4 - target host
   #  5 - target instance
   #  6 - target database
   #  7 - target table
   #  8 - datadir
   #  9 - target user
   # 10 - target password
   # 11 - hdfs copytolocal batch size
   # 12 - bcp directory
   # 13 - bcp batchsize
   # 14 - bcp packetsize
   # 15 - bcp firstrow
   # 16 - bcp lastrow
   # 17 - bcp hint
   # 18 - parallel bcp jobs
   # 19 - maxerrors
   # 20 - use aws cli
   # 21 - resultvar - variable to return function value
   # 22 - hdfs file list - variable array not counted in __xargs

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc


   

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=19

   # Number of passed args
   local -i __pargs=$#

   if (( __pargs < __xargs ))
	then
	   log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
      log "$@"
	   exit_abnormal
	fi

   local __loadingdock=$1
   local __hqlbase=$2
   local __project=$3
   local __host=$4
   local __instance=$5
   local __database=$6
   local __table=$7
   local __datadir=$8
   local __user=$9
   local __password=${10}
   local -i __batch_size=${11}
   local __bcpdir=${12}
   local -i __batchsize=${13}
   local -i __packetsize=${14}
   local -i __firstrow=${15}
   local -i __lastrow=${16}
   local __hint=${17}
   local __parallel_bcp_jobs=${18}
   local __max_error=${19}
   local -i __use_cli=${20}
   local __resultvar=${21}

   # Change this shift if the number of args changes
   shift 21
   local -a __files=("$@")

   local -i __v=0
   local __uri
   local __parallel_dryrun
   local -a __current_batch
   local __partition=$(date +%Y%m%d%H%M%S)
   local __logdir
   local __errdir
   local __cmd


   # end of function setup

   # setup partition log and err directories
   __logdir=${__bcpdir}/${__project}/${__table}/err/${__partition}
   __errdir=${__bcpdir}/${__project}/${__table}/log/${__partition}
   mkdir ${__logdir}
   mkdir ${__errdir}

   log "Stdout will be written to: ${__logdir}"
   log "Stderr will be written to: ${__errdir}"

   # source data directory
   if [[ "${__table}" == "${__datadir}" ]]
   then
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__table}/"
   else
      __uri="${__loadingdock}/${__hqlbase}/${__project}/${__datadir}/"
   fi

   # If lastrow specified then only use first
   # hdfs file, discard the rest
   if (( __lastrow > 0 ))
   then
      __files=("${__files[@]:0:1}")
   fi

   for i in $(seq 1 ${__batch_size} ${#__files[@]})
   do

      log "Processing ${__batch_size} files starting at ${i}"

      # get array slice
      __current_batch=("${__files[@]:$(( i-1 )):${__batch_size}}")

      # copyToLocal the current batch
      if (( __use_cli == 0 ))
      then
         __cmd="parallel --halt soon,fail=1 --results ${__bcpdir}/${__project}/${__table}/log/${__partition}/copyfrom_${i}.csv hdfs dfs -copyToLocal {} ${__bcpdir}/${__project}/${__table}/data  ::: ${__current_batch[@]}"
      else
         __cmd="parallel --halt soon,fail=1 --results ${__bcpdir}/${__project}/${__table}/log/${__partition}/copyfrom_${i}.csv aws s3 --no-progress cp {} ${__bcpdir}/${__project}/${__table}/data  ::: ${__current_batch[@]}"
      fi

      log "Executing cmd: ${__cmd}"

      if (( dry_run == 0 ))
      then
         eval "${__cmd}" 
      fi

      __rc=$?

      # iconv convert the files encoding if requested
      if [[ ! -z "${iconv_from}" ]]
      then

         # convert the file encoding
         __cmd="parallel iconv --from-code=${iconv_from} --to-code=${iconv_to} -c --output={}.iconv {} ::: ${__bcpdir}/${__project}/${__table}/data/0*_0"

         log "Executing cmd: ${__cmd}"

         if (( dry_run == 0 ))
         then
            eval "${__cmd}" 
         fi

         # move back to original file names
         __cmd="parallel mv {} {.} ::: ${__bcpdir}/${__project}/${__table}/data/*.iconv"

         log "Executing cmd: ${__cmd}"

         if (( dry_run == 0 ))
         then
            eval "${__cmd}" 
         fi

      fi

      # Build the bcp command
      if (( __rc == 0 ))
      then
         if (( __firstrow > 1 ))
         then
            __F="-F ${__firstrow}"
         fi
         if (( __lastrow > 0 ))
         then
            __L="-L ${__lastrow}"
         fi
         if [[ -z "${__hint}" ]]
         then
            __H=""
         else
            __H="-h \""${__hint}"\""
         fi
         # bcp in each file
         __cmd="parallel --halt soon,fail=1 -q --jobs ${__parallel_bcp_jobs} \
            --results ${__bcpdir}/${__project}/${__table}/log/{8}/{5/} \
            bcp dbo.{4} in {2}/{3}/{4}/data/{5/} -e {2}/{3}/{4}/err/{8}/{5/}.err \
            -f  {2}/{3}/{4}/fmt/{4}.fmt -b {6} ${__F} ${__L} ${__H}  -a {7} -S ${__host}\\\\${__instance} -U ${__user} -P '${__password}' -m ${__max_error} \
            -d ${__database} ::: \
            $(date +%Y%m%d_%H%M%S) ::: ${__bcpdir} ::: ${__project} ::: ${__table} ::: ${__current_batch[@]} ::: ${__batchsize} ::: ${__packetsize} ::: ${__partition}"

         log "Executing cmd: ${__cmd}"

         if (( dry_run == 0 ))
         then
            eval "${__cmd}"
         fi

         __rc=$?

         if (( __rc == 0 ))
         then
            # rm the source data files
            __cmd="parallel rm {1}/{2}/{3}/data/{4/} ::: ${__bcpdir} ::: ${__project} ::: ${__table} ::: ${__current_batch[@]}"
            if (( dry_run == 0 ))
            then
               eval "${__cmd}"
            fi
         else
            log "bcp failed, source data not deleted" >2&
            break
         fi

      else
         log "Copy back from hdfs failed" >2&
         break
      fi

   done
   
   # Reports
   if (( __rc == 0 &&  dry_run == 0 ))
   then
      __cmd="gawk -e 'BEGIN {ROWS=0; MS=0}; /Time/ {MS=MS+\$6}; /copied/ {ROWS=ROWS+\$1}; END {print \"Total Rows:\", ROWS, \",Time (sec):\", MS/1000, \",Time (m):\", MS/60000, \",Rows per sec:\", ROWS/(MS/1000)}' ${__bcpdir}/${__project}/${__table}/log/${__partition}/0*"
      log $(eval "${__cmd}")
   fi

   # Check error logs alerting if not empty, else delete
   if (( dry_run == 0 ))
   then
   for f in ${__bcpdir}/${__project}/${__table}/err/${__partition}/*
   do
     log "Checking error file $f"
     if [[ -s ${f} ]]
     then
       log "bcp error file $f may be reporting errors" >2&
       head -10 $f
       __rc=1
       break
     else
       rm $f
     fi
   done
   fi

   # Generate load stats
   if (( __rc == 0 && dry_run == 0))
   then
      gawk 'BEGIN { R=0; F=0; T=0 }; /copied/ {F=F+1; R=R+$1; RC=$1 }; /Clock/ {T=T+$6; printf "%s,%s,%s,%s\n", FILENAME,RC,$6,substr($9,2)}' ${__bcpdir}/${__project}/${__table}/log/${__partition}/0* > ${__bcpdir}/${__project}/${__table}/log/${__partition}/load_stats.csv
   fi

   eval $__resultvar="'${__rc}'"

}

usage() {

cat <<EOF

Usage:
======

This bcpx script is used to export data via the Micrsoft bcp utility
to a target SQL Server

Options:

  -a -- bcp packetsize (optional)
  -d -- data directory, use when data not in table directory (work-in-progress)
  -e -- use DELETE to delete existing rows 
  -E -- use TRUNCATE proc to delete existing rows - mutually exclusive with -e
  -F -- bcp firstrow
  -f -- bcp format file (optional)
  -g -- generate bcp format file only
  -h -- help, displays this usage data
  -H -- hint to be passed to bcp, see bcp doc
  -j -- project name
  -L -- bcp lastrow
  -l -- logfile name (default bcpx.log)
  -m -- bcp maxerrors (optional)
  -n -- no deletion of existing rows in target table
  -p -- number of parallel bcp export jobs, default 1
  -s -- sql yml options file - mandatory
  -t -- target table name - mandatory
  -v -- execute in verbose mode
  -y -- bcp yml options file - mandatory
  -z -- dry run mode, any generated cmds will be displayed not executed

EOF

}

# ########################
# Process cmd line options
# ########################

if [[ $# -eq 0 ]]; then 
   usage
   exit_normal
fi

while getopts ":a:d:eEF:f:ghH:j:L:l:m:np:s:t:vy:z" opt
do
   case $opt in
   a  ) packetsize=$OPTARG
        ;;
   d  ) datadir=$OPTARG
        ;;
   e  ) use_delete=1
        ;;
   E  ) use_truncate=1
        ;;
   F  ) firstrow=$OPTARG
        ;;
   f  ) formatfile=$OPTARG
        using_format=1
        ;;
   g  ) gen_fmt=1
        ;;
   h  ) usage
        exit_normal
        ;;
   H  ) hint=$OPTARG
        ;;
   j  ) project=$OPTARG
        ;;
   L  ) lastrow=$OPTARG
        ;;
   l  ) log_file=$OPTARG
        ;;
   m  ) maxerrors=$OPTARG
        ;;
   n  ) no_delete=1
        ;;
   p  ) pjobs=$OPTARG
        ;;
   s  ) sqlyaml=$OPTARG
        ;;
   t  ) table=$OPTARG
        ;;
   v  ) verbose=1
        ;;
   y  ) bcpyaml=$OPTARG
        ;;
   z  ) dry_run=1
        ;;
   \? ) usage
        exit_normal
        ;;
   :  ) echo "Option -$OPTARG requires an argument" >&2
        exit_abnormal
   esac
done

# Shift the options out of the way
shift $((OPTIND-1))

# #######################
# Validate options     
# #######################
if (( use_delete == 1 && no_delete == 1 ))
then
   log "Cannot specify both -e (use_delete) and -n (no_delete) options" >&2
   exit_abnormal
fi

if (( no_delete == 1 && use_truncate ==1 ))
then
   log "Cannot specify both -E (use_truncate) and -n (no_delete) options" >&2
   exit_abnormal
fi

if (( use_delete == 1 && use_truncate ==1 ))
then
   log "Cannot specify both -e (use_delete) and -E (use_truncate) options" >&2
   exit_abnormal
fi

if (( use_delete == 0 && use_truncate ==0 && no_delete == 0 && gen_fmt == 0 ))
then
   log "Please specify one of the -e (use_delete),  -E (use_truncate), no_delete (-n) or -g (gen_fmt) options" >&2
   exit_abnormal
fi

if (( pjobs <= 0 ))
then
   log "pjobs option must be > zero" >&2
   exit_abnormal
fi

# #######################
# Process yaml options
# #######################
if [[ -z ${bcpyaml} ]]
then
   echo "BCP yaml config file (-y) must be specified" >&2
  exit_abnormal
fi

if [[ -z ${sqlyaml} ]]
then
   echo "SQL yaml config (-s) file must be specified" >&2
  exit_abnormal
fi

# Process the bcp yaml file
if [[ -f ${bcpyaml} ]]
then
   if (( verbose == 1 ))
   then
     echo -e "Yaml options:\n\n$(parse_yaml ${bcpyaml})"
   fi

   eval $(parse_yaml ${bcpyaml})
else
   echo "BCP Yaml options file (-y) ${bcpyaml} does not exist" >&2
   exit_abnormal
fi

# Process the SQL yaml file
if [[ -f ${sqlyaml} ]]
then
   if (( verbose == 1 ))
   then
     echo -e "Yaml options:\n\n$(parse_yaml ${sqlyaml})"
   fi

   eval $(parse_yaml ${sqlyaml})
else
   echo "SQL Yaml options file (-s) ${sqlyml} does not exist" >&2
   exit_abnormal
fi

# ##############################################################
# Defaults
# ##############################################################
# BCP
batchsize=${bcp_batchsize:-1000}
bcpdir=${ec2_bcpdir:?"bcpdir must be specified in bcp yaml file"}
firstrow=${firstrow:-${bcp_firstrow}}
lastrow=${lastrow:-0}
log_file=${log_file:-${default_log_file}}
maxerrors=${maxerrors:-${bcp_maxerrors}}
packetsize=${packetsize:-${bcp_packetsize}}
rowterm=${rowterm:-${bcp_rowterm}}
fieldterm=${fieldterm:-${bcp_fieldterm}}
datadir=${datadir:-${table}}

log "bcp parameters"
log "--------------"
log "  batchsize: ${batchsize}"
log "     bcpdir: ${bcpdir}"
log "  fieldterm: ${fieldterm}"
log "   firstrow: ${firstrow}"
log "    datadir: ${datadir}"
if (( using_format == 1 ))
then
   log "formatfile: ${formatfile}"
fi
log "    lastrow: ${lastrow}"
log "  maxerrors: ${maxerrors}"
log " packetsize: ${packetsize}"
log "    rowterm: ${rowterm}"
log "   parallel: ${pjobs}"

# SQL
database=${database:-${sql_database}}
hint=${hint:-${sql_hint}}
host=${host:-${sql_host}}
port=${port:-${sql_port}}
instance=${instance:-${sql_instance}}
project=${project:-${sql_project}}
password=${password:-${sql_pwd}}
table=${table:?"table name (-t) must be given"}
user=${user:-${sql_user}}
aon=${sql_aon:-0}

table=${table:?"table name (-t) must be given"}
project=${project:?"project must be given as (-j) option of in the SQL yml file"}

if (( sql_aon == 1 ))
then
   if [[ -z ${sql_port} ]]
   then
      printf "SQL yml file %s indicates AON for target %s %s but no port given.\n" ${sqlyml} ${host} ${database}
      exit_abnormal
   fi
fi

log "SQL parameters"
log "--------------"
log "        aon: ${sql_aon}"
log "   database: ${database}"
log "       host: ${host}"
log "       port: ${port}"
log "   instance: ${instance}"
log "    project: ${project}"
log "      table: ${table}"
log "       user: ${user}"
log "       hint: ${hint}"

log "Switches"
log "--------------"
log "     dry_run: ${dry_run}"
log "     verbose: ${verbose}"
log "   no_delete: ${no_delete}"
log "  use_delete: ${use_delete}"
log "use_truncate: ${use_truncate}"

log ""
log "AWS"
log "--------------"
log "     receiving: ${aws_receiving}"
log "   loadingdock: ${aws_loadingdock}"
log "       hqlbase: ${aws_hqlbase}"
log "       use_cli: ${aws_use_cli}"

if [[ ! -z "iconv_from" ]]
then
   log "iconv"
   log "------------------------"
   log "     from: ${iconv_from}"
   log "       to: ${iconv_to}"
   log "     omit: ${iconv_omit}"
fi

if (( verbose ))
then
   log "Invoked from ${DIR}"
fi

# ##############################################################
# Check template function 
# ##############################################################
if (( verbose == 1 ))
then
   template rc
   echo "Call to template returned: $rc"
fi

# ##############################################################
# Check if required cmds exist
# ##############################################################
checkcmd "bcp" rc

if (( rc == 1 ))
then
   exit_abnormal
fi

checkcmd "sqlcmd" rc

if (( rc == 1 ))
then
   exit_abnormal
fi

checkcmd "iconv" rc

if (( rc == 1 ))
then
   exit_abnormal
fi

# ##############################################################
# Check if any file names passed exist
# ##############################################################
if (( using_format == 1 ))
then
   checkfile "${formatfile}" rc
   if (( rc == 1 ))
   then
      exit_abnormal
   fi
fi

# ##############################################################
# Check project directories
# ##############################################################
checkbcpdirs ${bcpdir} ${project} ${table} rc

if (( rc == 0 ))
then
   log "Validated project (${project}) directories"
else
   log "${project} directories validation / creation failed" >&2
   exit_abnormal
fi

# ##############################################################
# Check target table and connection
# ##############################################################
checktable "${host}" "${instance}" "${database}" "${table}" "${user}" "${password}" rc

if (( rc == 0 ))
then
   log "Validated ${database} connection"
else
   log "${database} connection failed, please check the SQL parameter yaml file." >&2
   exit_abnormal
fi

# ##############################################################
# If using truncate check process config config
# ##############################################################
if (( use_truncate == 1 ))
then
   log "Validating connection to process config database"
   checktruncatetable "${truncate_host}" "${truncate_instance}" "${truncate_database}" "${database}" "${table}" "${truncate_user}" "${truncate_pwd}" rc

   if (( rc == 0 ))
   then
      log "Validated ${truncate_database} connection"
   else
      log "${truncate_database} connection failed, please check the error messages." >&2
      exit_abnormal
   fi
fi

# ##############################################################
# Check table format file
# ##############################################################
fetchfmt "${host}" "${instance}" "${database}" "${table}" "${user}" "${password}" "${bcpdir}" "${project}" "${fieldterm}" "${rowterm}" rc

if (( rc == 0 ))
then
   log "Validated / created format file"
else
   log "Format file creation / validation failed." >&2
   exit_abnormal
fi

if (( gen_fmt == 1 ))
then
   log "Generate format only, job finished"
   exit_normal
fi

# ##############################################################
# Check source has data
# ##############################################################
checksrcdir "${aws_loadingdock}" "${aws_hqlbase}" "${project}" "${table}" "${datadir}" ${aws_use_cli} rc

if (( rc == 0 ))
then
   log "Validated data source"
else
   log "Data source validation failed, please check - ${aws_loadingdock}/${aws_hqlbase}/${project}/${table}." >&2
   exit_abnormal
fi

# ##############################################################
# Get list of hdfs file that will be exported
# ##############################################################
getdatafilelist "${aws_loadingdock}" "${aws_hqlbase}" "${project}" "${table}" "${datadir}" ${aws_use_cli} rc

hdfs_files=($rc)

num_files=${#hdfs_files[@]}

if (( num_files == 0 && dry_run == 0))
then
   log "No files to copy for export" >&2
   exit_abnormal
else
   log "${#hdfs_files[@]} files will be copied for loading ${bcpdir}/${project}/${table}/data"
fi


# ##############################################################
# Clean the target data directory
# ##############################################################
cleandatadir "${bcpdir}" "${project}" ${table} rc

if (( rc != 0 ))
then 
   log "Error cleaning data directory" >&2
   exit_abnormal
fi

# ##############################################################
# Clean the target table
# ##############################################################
if (( use_truncate == 1 ))
then
   truncatetable "${truncate_host}" "${truncate_instance}" "${truncate_database}" "${database}" "${table}" "${truncate_user}" "${truncate_pwd}" rc

   if (( rc == 0 ))
   then
      log "Table ${database}.${table} truncated."
   else
      log "Table ${database}.${table} truncation failed."
      exit_abnormal
   fi
fi

if (( use_delete == 1 ))
then
   deletetable "${host}" "${instance}" "${database}" "${table}" "${user}" "${password}" rc

   if (( rc == 0 ))
   then
      log "Table ${database}.${table} contents deleted."
   else
      log "Table ${database}.${table} delete failed."
      exit_abnormal
   fi
fi

# ##############################################################
# Start copy and bcp
# ##############################################################
if (( dry_run == 1 ))
then
  hdfs_files=("dry_run")
fi

bcpdata "${aws_loadingdock}" "${aws_hqlbase}" "${project}" "${host}" "${instance}" "${database}" "${table}" "${datadir}" "${user}" "${password}" ${parallel_hdfs_batch_size} "${bcpdir}" ${batchsize} ${packetsize} ${firstrow} ${lastrow} "${hint}" ${pjobs} ${maxerrors} ${aws_use_cli} rc "${hdfs_files[@]}"

if (( rc == 0 ))
then
   log "Table ${database}.${table} contents loaded."
else
   log "Table ${database}.${table} load failed." >&2
   exit_abnormal
fi

log "End of bcp job"
