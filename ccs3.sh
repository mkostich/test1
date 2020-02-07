#!/usr/bin/env bash

#SBATCH --job-name=ccs3                                        ## SLURM_JOB_NAME
#SBATCH --export=ALL                                           ## export environment variables
#SBATCH --exclusive                                            ## do not share node with other jobs
#SBATCH --ntasks=1                                             ## total number of tasks
#SBATCH --tasks-per-node=1                                     ## tasks per node

#######################################################################################################################
## metadata helper:

exit_meta() {

  local exit_code=$1
  local exit_msg0=$2
  local exit_msg=${exit_msg0//\'/}

  local arr=("'program_name': '$cmd0'")
  local prog_name=${cmd0/\.sh/}
  local exit_time=$(date)

  local vars=(version author start_time exit_time exit_code exit_msg host_name ip_address os_name os_version \
    cpu_number cpu_model mem_total slurm_version SLURM_JOB_NAME SLURM_JOB_ID task_id singularity_version)
  local n val
  for n in ${vars[*]}; do arr+=("'$n': '${!n}'"); done

  local lnks=(current_dir container in_file out_dir log_dir)
  for n in ${lnks[*]}; do
    val="${!n}"
    [ -n "$val" ] && [ -e "$val" ] && val=$(readlink -f "$val")
    arr+=("'$n': '$val'")
  done

  arr+=("'ccs2.jid': '$ccs_jid', 'ccs3.cpus': '$cpus'")

  [ -z "$container_meta" ] && container_meta='{}'
  arr+=("'container_metadata': $container_meta")

  local o=$(IFS='|'; echo "${arr[*]}")

  local meta_file=$prog_name
  [ -n "$ccs_jid" ] && meta_file="$meta_file.$ccs_jid"
  meta_file="$meta_file.$SLURM_JOB_ID.meta"

  if [ -e "$log_dir" ]; then meta_file=$log_dir/$meta_file
  else meta_file=$current_dir/$meta_file
  fi

  echo "{${o//\|/, }}" >> $meta_file
  [ -n "$exit_msg0" ] && echo "$task_id: $exit_msg0" >&2
  exit $exit_code
}

#######################################################################################################################
## configure:

cmd0='ccs3.sh'
version='20200203a'
author='mitch.kostich@jax.org'

start_time=$(date)
host_name=$(hostname)
ip_address=$(ip route get 1 | awk '{print $NF;exit}')
os_name=$(grep '^NAME=' /etc/os-release)
os_name=${os_name#NAME=}
os_name=${os_name//\"/}
os_version=$(grep '^VERSION=' /etc/os-release)
os_version=${os_version#VERSION=}
os_version=${os_version//\"/}
cpu_number=$(grep -c '^model name' /proc/cpuinfo)
cpu_model=$(grep '^model name' /proc/cpuinfo | head -n1)
cpu_model=${cpu_model#*\: }
mem_total=$(grep 'MemTotal' /proc/meminfo)
mem_total=${mem_total#* }
mem_total=$(echo "$mem_total" | sed 's/^ *//')
slurm_version=$(sbatch --version)
current_dir=$(pwd)

task_id="${cmd0/\.sh/}"
task_id="$task_id.$$"
[ -n "$SLURM_JOB_ID" ] && task_id="$task_id.$SLURM_JOB_ID"
[ -z "$SLURM_JOB_ID" ] && { exit_meta 1 "Error: invoke $0 with slurm srun or sbatch."; }
[ -n "$SLURM_ARRAY_TASK_ID" ] && { exit_meta 1 "Error: invoke $0 without slurm arrays."; }

module load singularity 2>/dev/null
hash singularity 2>/dev/null || { exit_meta 3 "Error: could not load singularity module"; }
singularity_version=$(singularity --version)
singularity_version=${singularity_version##* }

#######################################################################################################################
## process arguments:

[ $# -ne 6 ] && { exit_meta 4 "Error: Usage: $cmd0 <nthreads> <in_file> <ccs_jobid> <container> <out_dir> <log_dir>"; }

cpus=$1; in_file=$2; ccs_jid=$3; container=$4; out_dir=$5; log_dir=$6

[ "$cpus" -eq "$cpus" 2>/dev/null ] && [ "$cpus" -ge 1  ] || \
  { exit_meta 5 "Error: threads must be an integer >= 1"; }
[ "$ccs_jid" = "$ccs_jid" ] && [ "$ccs_jid" -gt 0 ] ||
  { exit_meta 6 "Error: ccs_jobid must be an integer > 0"; }
[ -e "$container" ] && [ -r "$container" ] || \
  { exit_meta 7 "Error: container ($container) not found."; }

cmd="singularity exec $container get_meta_yaml.sh"             ## returns a .yaml string with container metadata
container_meta="$(srun $cmd 2>&1)"

[ -e "$out_dir" ] && [ -d "$out_dir" ] && [ -w "$out_dir" ] || \
  { exit_meta 8 "Error: out_dir ($out_dir) is not a directory that can be written to."; }
[ -e "$log_dir" ] && [ -d "$log_dir" ] && [ -w "$log_dir" ] || \
  { exit_meta 9 "Error: log_dir ($log_dir) is not a directory that can be written to."; }

## out_pre="$out_dir/${in_file/\.bam}.ccs.$SLURM_JOB_ID.$SLURM_ARRAY_TASK_ID"
file_pre=$(basename $in_file)
file_pre="$out_dir/${file_pre/\.bam}.ccs.$ccs_jid"
files=($(ls $file_pre.*.bam 2>/dev/null))
[ "${#files[@]}" -eq 0 ] && { exit_meta 10 "Error: no files with prefix ($file_pre) found."; }
[ "${#files[@]}" -eq 1 ] && { exit_meta 11 "Error: only one ccs file with prefix ($file_pre)."; }
[ -e "${file_pre}.bam" ] && { exit_meta 12 "Error: output file (${file_pre}.bam) already exists."; }

params=("$task_id" "$(hostname)" "$(readlink -f $cmd0)" "$(readlink -f $container)" "$ccs_ver" "$(readlink -f $in_file)" \
  "$ccs_jid" "$(readlink -f $out_dir)" "$(readlink -f $log_dir)" "$cpus")

echo $(IFS='|' ; echo "${params[*]}") >> $log_dir/${cmd0/\.sh}.prm

######################################################################################################################
## run job:

infiles=$(ls ${file_pre}.*.bam*)

cmd="singularity exec $container samtools merge -@$cpus ${file_pre}.bam ${file_pre}.*.bam"
srun $cmd
ecode=$?
echo "$task_id|$ecode|$cmd" >> $log_dir/${cmd0/\.sh}.log
[ "$ecode" -ne 0 ] && { exit_meta 13 "Error: command '$cmd' returned exit code '$ecode'."; }

cmd="singularity exec $container pbindex ${file_pre}.bam"
srun $cmd
ecode=$?
echo "$task_id|$?|$cmd" >> $log_dir/${cmd0/\.sh}.log
[ "$ecode" -ne 0 ] && { exit_meta 14 "Error: command '$cmd' returned exit code '$ecode'."; }

rm $infiles
unset infiles cmd ecode
exit_meta 0 'Completed normally'

## DONE ##############################################################################################################

