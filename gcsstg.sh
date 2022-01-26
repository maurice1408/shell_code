#!/bin/bash

#
# SCRIPT: gcsstg
# AUTHOR: Maurice Hickey
# DATE:   Jan 2021
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
#       the shell script will not execute!
#          
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
  exit 1
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
declare -i batch_id=0
declare -i stage=0

declare -l database
declare -u hint
declare -l project
declare -l table
declare -a data_files
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

   log "${__funcname}: Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      log "${__funcname}: Dry run only returning success"
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

   log "${__funcname}: Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      log "${__funcname}: Dry run mode"
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

   if [[ -f "${__fmtfile}" ]]
   then
      log "${__funcname}: ${__fmtfile} already exists, will not be overwritten"
   else
      __cmd="bcp ${__table} format nul -f ${__fmtfile} -S ${__host}\\\\${__instance} -d ${__database} -U ${__user} -c -P '${__password}' -r '${__rowterm}' -t '${__fieldterm}'"

      log "${__funcname}: Executing cmd: ${__cmd}"

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
   # 1 - source data directory
   # 2 - resultvar - variable to return function value

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
   local -i __xargs=2

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   # local var1="$1"
   # local var2="$2"
   local __directory=$1
   local __resultvar=$2

   # end of function setup

   # Check directory exist
   if [[ -d "${__directory}" ]]
   then
      if (( verbose == 1 ))
      then
	      log "${__funcname}: Validated directory: ${__directory}, exists"
      fi
      __rc=0
   else
      if (( verbose == 1 ))
      then
	      log "${__funcname}: Could not validate directory: ${__directory}"
      fi
      __rc=1
   fi

   eval $__resultvar="'${__rc}'"

}

#######################################################################
function getdatafilelist() {
   
   # Expected args - list the args here
   # 1 - data_source
   # 2 - file_pattern
   # 3 - table
   # 4 - Table prefix
   # 5 - Table suffix
   # 6 - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=6

   # Number of passed args
   local -i __pargs=$#

   if (( __xargs != __pargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
	  exit_abnormal
	fi

   local __directory=$1
   local __pattern=$2
   local __table=$3
   local __prefix=$4
   local __suffix=$5
   local __resultvar=$6

   local -a __files
   local -i v=0
   local __uri

   __table=${__table%%${__suffix}}
   __table=${__table##${__prefix}}

   # end of function setup
   if (( verbose == 1 ))
   then
      log "${__funcname}: validating files for table: ${__table} in directory: ${__directory}"
   fi


   __cmd="ls -1 ${__directory}/${__pattern}*${__table}.csv.zip"

   if (( verbose == 1 ))
   then
     log "${__funcname}: Executing cmd = \"${__cmd}\""
   fi

   for f in $(ls -1 ${__directory}/${__pattern}${__table}.[cC][sS][vV].zip)
   do
     echo $v $f
     __files[(( v++ ))]=$f
   done

   if (( ${#__files[@]} == 0 ))
   then
     log "No files found to process matching ${__directory}/${__pattern}*${__table}.csv.zip" >&2
   fi

   eval $__resultvar="'${__files[@]}'"

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
   #  1 - data_directory
   #  2 - project
   #  3 - target host
   #  4 - target instance
   #  5 - target database
   #  6 - target table
   #  7 - target user
   #  8 - target password
   #  9 - bcp directory
   # 10 - bcp batchsize
   # 11 - bcp packetsize
   # 12 - bcp firstrow
   # 13 - bcp lastrow
   # 14 - bcp hint
   # 15 - maxerrors
   # 16 - data file
   # 17 - resultvar - variable to return function value

   # Function Local variables

   local __funcname=${FUNCNAME[0]}
   local -i __calledfrom=${BASH_LINENO[0]}
   local __rc
   

	if (( verbose == 1 ))
	then
      log "${__funcname}: Called from line: ${__calledfrom}"
	fi

   # Set this local to the number of expected args
   local -i __xargs=17

   # Number of passed args
   local -i __pargs=$#

   if (( __pargs < __xargs ))
	then
	  log "${__funcname}: expected ${__xargs} arguments, received ${__pargs}" >&2
     log "$@"
	  exit_abnormal
	fi

   local __directory=$1
   local __project=$2
   local __host=$3
   local __instance=$4
   local __database=$5
   local __table=$6
   local __user=$7
   local __password=${8}
   local __bcpdir=${9}
   local -i __batchsize=${10}
   local -i __packetsize=${11}
   local -i __firstrow=${12}
   local -i __lastrow=${13}
   local __hint=${14}
   local __max_error=${15}
   local __data_file=${16}
   local __resultvar=${17}

   # Change this shift if the number of args changes
   shift 16
   local -a __data_files=("$@")

   local -i __v=0
   local __partition=$(date +%Y%m%d%H%M%S)
   local __logdir
   local __errdir
   local __cmd


   # end of function setup

   # setup partition log and err directories
   __logdir=${__bcpdir}/${__project}/${__table}/err/${__partition}
   __errdir=${__bcpdir}/${__project}/${__table}/log/${__partition}
   mkdir -p ${__logdir}
   mkdir -p ${__errdir}

   log "Stdout will be written to: ${__logdir}"
   log "Stderr will be written to: ${__errdir}"

   # Unzip data file
   # __cmd="unzip -p '${__directory}/${__data_file}' | gawk --assign=COUNT_FILE=${__bcpdir}/${__project}/${__table}/data/count.txt --file gcs_prep_data.awk > ${__bcpdir}/${__project}/${__table}/data/${__table}.dat"
   __cmd="unzip -p '${__directory}/${__data_file}' | sed -n -E -f /home/hdp_podium/scripts/gcs/gcs_prep_data.sed > ${__bcpdir}/${__project}/${__table}/data/${__table}.dat"

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

       # Build bcp command
       __cmd="bcp dbo.${__table} in ${__bcpdir}/${__project}/${__table}/data/${__table}.dat \
              -e ${__bcpdir}/${__project}/${__table}/err/${__partition}/${__table}.err \
              -f ${__bcpdir}/${__project}/${__table}/fmt/${__table}.fmt \
              -b ${__batchsize} -a ${__packetsize} \
              -S ${__host}\\\\${__instance} -U ${__user} -P '${__password}' -m ${__max_error} \
              -d ${__database} > ${__bcpdir}/${__project}/${__table}/log/${__partition}/${__table}.log"
            

       log "Executing cmd: ${__cmd}"

       if (( dry_run == 0 ))
       then
          eval "${__cmd}"
       fi

       __rc=$?

       if (( __rc == 0 ))
       then
          # rm the source data files
          __cmd="rm ${__bcpdir}/${__project}/${__table}/data/${__table}.dat"
          if (( dry_run == 0 ))
          then
             eval "${__cmd}"
          fi
       else
          log "bcp failed, source data not deleted" >2&
       fi

   else
       log "Unzip of source data failed: ${__directory}/${__data_file}" >2&
       exit_abnormal
   fi

   # Reports
   if (( __rc == 0 &&  dry_run == 0 ))
   then
      __cmd="gawk -e 'BEGIN {ROWS=0; MS=0}; /Time/ {MS=MS+\$6}; /copied/ {ROWS=ROWS+\$1}; END {print \"Total Rows:\", ROWS, \",Time (sec):\", MS/1000, \",Time (m):\", MS/60000, \",Rows per sec:\", ROWS/(MS/1000)}' ${__bcpdir}/${__project}/${__table}/log/${__partition}/${__table}.log"
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
      gawk 'BEGIN { R=0; F=0; T=0 }; /copied/ {F=F+1; R=R+$1; RC=$1 }; /Clock/ {T=T+$6; printf "%s,%s,%s,%s\n", FILENAME,RC,$6,substr($9,2)}' ${__bcpdir}/${__project}/${__table}/log/${__partition}/${__table}.log >> ${__bcpdir}/${__project}/${__table}/log/load_stats.csv
   fi

   eval $__resultvar="'${__rc}'"

}

#######################################################################
function copy_from_stage() {

   
   # Expected args - list the args here
   #  1 - host
   #  2 - instance
   #  3 - database
   #  4 - user
   #  5 - password
   #  6 - source_table
   #  7 - target_table
   #  8 - batch_id
   #  9 - insert or merge (i/m)
   # 10 - batch_run_cd
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
   local __user=$4
   local __pwd=$5
   local __source=$6
   local __target=$7
   local __batchid=$8
   local __insert=$9
   local __batchruncd=${10}
   local __resultvar=${11}

   local __cmd

   # end of function setup
   if (( ${verbose} == 1 ))
   then
      log "Processing source: ${__source}, target: ${__target} insert/merge: ${__insert}"
   fi


   __cmd="sqlcmd -S ${__host}\\\\${__instance} -d ${__database} -U ${__user} -v batch_id=${__batchid} -v src=\"${__source}\" -v tgt=\"${__target}\" -v batchruncd=\"${__batchruncd}\" -v mode=\"${__insert}\" -b -i ${scripts_home}/${scripts_insert_merge}"

   log "${__funcname}: Executing sqlcmd: ${__cmd}"

   if (( dry_run == 0 ))
   then
      __cmd="SQLCMDPASSWORD='${__pwd}' ${__cmd}"
      eval "${__cmd}"
      __rc=$?
   else
      log "${__funcname}: Dry run only returning success"
      __rc=0
   fi

   eval $__resultvar="${__rc}"

}

usage() {

cat <<EOF

Usage:
======

This gcsstg script is used to export data via the Micrsoft bcp utility
to a target GrpGCSStaging SQL Server

Options:

  -a -- bcp packetsize (optional)
  -b -- batch_id - mandatory
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
  -S -- call "proc_load_(insert/merge)_gcs_tables" to copy data from stg to perm tables, default is 0
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
   exit 0
fi

while getopts ":a:b:d:eEF:f:ghH:j:L:l:m:np:s:St:vy:z" opt
do
   case $opt in
   a  ) packetsize=$OPTARG
        ;;
   b  ) batch_id=$OPTARG
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
        exit 0
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
   S  ) stage=1
        ;;
   t  ) table=$OPTARG
        ;;
   v  ) verbose=1
        ;;
   y  ) gcsyaml=$OPTARG
        ;;
   z  ) dry_run=1
        ;;
   \? ) usage
        exit 0
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

if (( no_delete == 1 && use_truncate == 1 ))
then
   log "Cannot specify both -E (use_truncate) and -n (no_delete) options" >&2
   exit_abnormal
fi

if (( use_delete == 1 && use_truncate == 1 ))
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

if (( batch_id == 0 && stage == 1 ))
then
   log "No batch_id specified" >&2
   exit_abnormal
fi

# #######################
# Process yaml options
# #######################
if [[ -z ${gcsyaml} ]]
then
   echo "GCS yaml config file (-y) must be specified" >&2
  exit_abnormal
fi

if [[ -z ${sqlyaml} ]]
then
   echo "SQL yaml config (-s) file must be specified" >&2
  exit_abnormal
fi

# Process the bcp yaml file
if [[ -f ${gcsyaml} ]]
then
   if (( verbose == 1 ))
   then
     echo -e "Yaml options:\n\n$(parse_yaml ${gcsyaml})"
   fi

   eval $(parse_yaml ${gcsyaml})
else
   echo "BCP Yaml options file (-y) ${gcsyaml} does not exist" >&2
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
# GCS Yaml
batchsize=${bcp_batchsize:-1000}
bcpdir=${bcp_bcpdir:?"bcpdir must be specified in gcs yaml file"}
firstrow=${firstrow:-${bcp_firstrow}}
lastrow=${lastrow:-0}
log_file=${log_file:-${default_log_file}}
maxerrors=${maxerrors:-${bcp_maxerrors}}
packetsize=${packetsize:-${bcp_packetsize}}
rowterm=${rowterm:-${bcp_rowterm}}
fieldterm=${fieldterm:-${bcp_fieldterm}}
datadir=${datadir:-${table}}
project=${project:-${gcs_project}}

log "bcp parameters"
log "--------------"
log "  batchsize: ${batchsize}"
log "     bcpdir: ${bcpdir}"
log "  fieldterm: ${fieldterm}"
log "   firstrow: ${firstrow}"
log "     source: ${data_source}"
log "    pattern: ${data_pattern}"
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
instance=${instance:-${sql_instance}}
password=${password:-${sql_pwd}}
table=${table:?"table name (-t) must be given"}
user=${user:-${sql_user}}

table=${table:?"table name (-t) must be given"}

log "SQL parameters"
log "--------------"
log "        database: ${database}"
log "            host: ${host}"
log "        instance: ${instance}"
log "         project: ${project}"
log "           table: ${table}"
log "            user: ${user}"
log "            hint: ${hint}"
log "    scripts home: ${scripts_home}"
log "     batch start: ${scripts_start_batch}"
log "       batch end: ${scripts_end_batch}"
log "    insert/merge: ${scripts_insert_merge}"

log "Switches"
log "--------------"
log "     dry_run: ${dry_run}"
log "       stage: ${stage}"
log "     verbose: ${verbose}"
log "   no_delete: ${no_delete}"
log "  use_delete: ${use_delete}"
log "use_truncate: ${use_truncate}"

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


if (( rc == 0 ))
then
   log "Validated project (${project}) directories"
else
   log "${project} directories validation / creation failed" >&2
   exit_abnormal
fi

# Parse Table name, which should be
# <package name>,<stage table>,<tgt_table>,<Insert/Merge>
package_name=$(echo $table | cut -d "," -f 1)
stg_table=$(echo $table | cut -d "," -f 2)
tgt_table=$(echo $table | cut -d "," -f 3)
insrt_mrg=$(echo $table | cut -d "," -f 4)


log "Parsed table details"
log "--------------"
log "     dry_run: ${dry_run}"
log "     verbose: ${verbose}"
log "    batch_id: ${batch_id}"
log "package_name: ${package_name}"
log "   stg_table: ${stg_table}"
log "   tgt_table: ${tgt_table}"
log "   insrt_mrg: ${insrt_mrg}"


stg_table=${stg_table:?"src_table name (-t) of the form <stg_table>,<tgt_table>,<insert or merge> must be given"}
tgt_table=${tgt_table:?"tgt_table name (-t) of the form <stg_table>,<tgt_table>,<insert or merge> must be given"}
insrt_mrg=${insrt_mrg:?"insert / merge (-t) of the form <stg_table>,<tgt_table>,<insert or merge> must be given"}


# ##############################################################
# Check project directories
# ##############################################################
checkbcpdirs ${bcpdir} ${project} ${stg_table} rc


# ##############################################################
# Check target table and connection
# ##############################################################
checktable "${host}" "${instance}" "${database}" "${stg_table}" "${user}" "${password}" rc

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
   checktruncatetable "${truncate_host}" "${truncate_instance}" "${truncate_database}" "${database}" "${stg_table}" "${truncate_user}" "${truncate_pwd}" rc

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
fetchfmt "${host}" "${instance}" "${database}" "${stg_table}" "${user}" "${password}" "${bcpdir}" "${project}" "${fieldterm}" "${rowterm}" rc

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
   exit 0
fi

# ##############################################################
# Check source has data
# ##############################################################
checksrcdir "${data_source}" rc

if (( rc == 0 ))
then
   log "Validated data source for table - ${stg_table}"
else
   log "Data source validation failed, please check - ${data_source} contains file for table - ${stg_table}." >&2
   exit_abnormal
fi

# ##############################################################
# Get list of files that will be exported
# ##############################################################
getdatafilelist "${data_source}" "${data_pattern}" "${stg_table}" "${data_prefix}" "${data_suffix}" rc

data_files=($rc)

num_files=${#data_files[@]}

if (( num_files == 0 && dry_run == 0))
then
   log "No files to copy for export" >&2
   exit_abnormal
fi

if (( num_files > 1 ))
then
   log "More than one file exists for table ${stg_table}, will not proceed"
   exit_abnormal
else
   log "${data_files[0]} file will be unzipped for loading to ${bcpdir}/${project}/${stg_table}/data"
fi

# ##############################################################
# Clean the target data directory
# ##############################################################
cleandatadir "${bcpdir}" "${project}" ${stg_table} rc

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
   truncatetable "${truncate_host}" "${truncate_instance}" "${truncate_database}" "${database}" "${stg_table}" "${truncate_user}" "${truncate_pwd}" rc

   if (( rc == 0 ))
   then
      log "Table ${database}.${stg_table} truncated."
   else
      log "Table ${database}.${stg_table} truncation failed."
      exit_abnormal
   fi
fi

if (( use_delete == 1 ))
then
   deletetable "${host}" "${instance}" "${database}" "${stg_table}" "${user}" "${password}" rc

   if (( rc == 0 ))
   then
      log "Table ${database}.${stg_table} contents deleted."
   else
      log "Table ${database}.${stg_table} delete failed."
      exit_abnormal
   fi
fi

# ##############################################################
# Start copy and bcp
# ##############################################################

bcpdata "${data_directory}" "${project}" "${host}" "${instance}" "${database}" "${stg_table}" "${user}" "${password}" "${bcpdir}" ${batchsize} ${packetsize} ${firstrow} ${lastrow} "${hint}" ${maxerrors} "${data_files[0]}" rc 

if (( rc == 0 ))
then
   log "Table ${database}.${stg_table} contents loaded."
else
   log "Table ${database}.${stg_table} load had errors." >&2
   exit_abnormal
fi

# ##############################################################
# Call stg to perm stored proc
# ##############################################################
if (( stage == 1 ))
then
   copy_from_stage "${host}" "${instance}" "${database}" "${user}" "${password}" "${stg_table}" "${tgt_table}" ${batch_id} ${insrt_mrg} "GCSCLBCPLD" rc

   if (( rc == 0 ))
   then
      log "Table ${database}.${stg_table} contents processed."
   else
      log "Table ${database}.${stg_table} load failed." >&2
      exit_abnormal
   fi
else
   log "No stage to permanent insert / merge requested"
fi

log "End of gcs job"
