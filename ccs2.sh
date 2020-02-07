#!/usr/bin/env bash

#SBATCH --job-name=ccs2                                        ## SLURM_JOB_NAME
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

  local vars=(cmd0 version author start_time exit_time exit_code exit_msg host_name ip_address os_name os_version \
    cpu_number cpu_model mem_total slurm_version \
    SLURM_JOB_NAME SLURM_JOB_ID SLURM_ARRAY_JOB_ID SLURM_ARRAY_TASK_ID SLURM_ARRAY_TASK_COUNT \
    singularity_version task_id)
  local n
  for n in ${vars[*]}; do arr+=("'$n': '${!n}'"); done

  local lnks=(current_dir container in_file out_dir log_dir)
  for n in ${lnks[*]}; do
    val="${!n}"
    [ -n "$val" ] && [ -e "$val" ] && val=$(readlink -f "$val")
    arr+=("'$n': '$val'")
  done

  vars=(cpus min_snr min_length min_passes min_rq)
  for n in ${vars[*]}; do arr+=("'ccs.$n': '${!n}'"); done

  [ -z "$container_meta" ] && container_meta='{}'
  arr+=("'${prog_name}.container_metadata': $container_meta")

  local o=$(IFS='|'; echo "${arr[*]}")

  local meta_file
  if [ -n "$SLURM_ARRAY_JOB_ID" ]; then meta_file="${prog_name}.${SLURM_ARRAY_JOB_ID}.${SLURM_JOB_ID}.meta"
  else meta_file="${prog_name}.${SLURM_JOB_ID}.${SLURM_JOB_ID}.meta"
  fi

  if [ -d "$log_dir" ]; then meta_file=$log_dir/$meta_file;
  else meta_file=$current_dir/$meta_file;
  fi

  echo "{${o//\|/, }}" >> $meta_file
  [ -n "$exit_msg0" ] && echo "$task_id: $exit_msg0" >&2
  exit $exit_code
}

#######################################################################################################################
## configure:

cmd0='ccs2.sh'
version='20200203a'
author='mitch.kostich@jax.org'

min_min_snr=1                                                  ## minimum minimum signal-to-noise ratio for base calls
min_min_length=5                                               ## minimum minimum ccs consensus sequence length
min_min_passes=2                                               ## minimum minimum number of passes required
min_min_rq=0.01                                                ## minimum minimum quality of ccs consenus; in (0, 1).

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
[ -z "$SLURM_JOB_ID" ] && { exit_meta 1 "Error: invoke $cmd0 with slurm srun or sbatch."; }
[ -n "$SLURM_JOB_ID" ] && task_id="$task_id.$SLURM_JOB_ID"
[ -n "$SLURM_ARRAY_TASK_ID" ] && task_id="$task_id.$SLURM_ARRAY_JOB_ID.$SLURM_ARRAY_TASK_ID"

module load singularity 2>/dev/null
hash singularity 2>/dev/null || { exit_meta 3 "Error: could not load singularity module"; }
singularity_version=$(singularity --version)
singularity_version=${singularity_version##* }

#######################################################################################################################
## process arguments:

[ $# -ne 9 ] && \
  { exit_meta 4 "Error: Usage: $cmd0 <cpus_per_node> <in_file> <container> <out_dir> <log_dir> <min_snr> <min_length> <min_passes> <min_rq>"; }

cpus=$1; in_file=$2; container=$3; out_dir=$4; log_dir=$5; min_snr=$6; min_length=$7; min_passes=$8; min_rq=$9;

[ "$cpus" -eq "$cpus" 2>/dev/null ] && [ "$cpus" -gt 0 ] || \
  { exit_meta 5 "cpus_per_node should be an integer greater than 0." >&2; }
[ -e "$in_file" ] && [ -f "$in_file" ] && [ -r "$in_file" ] || \
  { exit_meta 6 "Error: in_file ($in_file) is not a regular file that can be read."; }
[ -e "$container" ] && [ -r "$container" ] || \
  { exit_meta 7 "Error: no container ($container) found."; }

cmd="singularity exec $container get_meta_yaml.sh"             ## returns a .yaml string
container_meta="$(srun $cmd 2>&1)"

[ -e "$out_dir" ] && [ -d "$out_dir" ] && [ -w "$out_dir" ] || \
  { exit_meta 8 "Error: out_dir ($out_dir) is not a directory that can be written to."; }
[ -e "$log_dir" ] && [ -d "$log_dir" ] && [ -w "$log_dir" ] || \
  { exit_meta 9 "Error: log_dir ($log_dir) is not a directory that can be written to."; }

[[ "$min_snr" =~ ^[+-]?[0-9]+\.?$ || "$min_snr" =~ ^[+-]?[0-9]*\.[0-9]+$ ]] || \
  { exit_meta 10 "Error: min_snr must be a number."; }
[ $(bc <<< "$min_snr >= $min_min_snr") -gt 0 ] || \
  { exit_meta 11 "Error: min_snr must be >= $min_min_snr."; }
[ "$min_length" -eq "$min_length" 2>/dev/null ] && [ "$min_length" -ge $min_min_length  ] || \
  { exit_meta 12 "Error: min_length must be an integer >= $min_min_length"; }
[ "$min_passes" -eq "$min_passes" 2>/dev/null ] && [ "$min_passes" -ge $min_min_passes ] || \
  { exit_meta 13 "Error: min_passes must be an integer >= $min_min_passes"; }
[[ "$min_rq" =~ ^[+-]?[0-9]+\.?$ || "$min_rq" =~ ^[+-]?[0-9]*\.[0-9]+$ ]] || \
  { exit_meta 14 "Error: min_rq must be a number."; }
[ $(bc <<< "$min_rq >= $min_min_rq") -gt 0 ] && [ $(bc <<< "$min_rq < 1") -gt 0 ] || \
  { exit_meta 15 "Error: min_rq must be >= $min_min_rq and less than one."; }

params=("$task_id" "$(hostname)" "$(readlink -f $cmd0)" "$(readlink -f $container)" "$cpus" "$(readlink -f $in_file)" \
  "$(readlink -f $out_dir)" "$(readlink -f $log_dir)" "$min_snr" "$min_length" "$min_passes" "$min_rq")

echo $(IFS='|' ; echo "${params[*]}") >> $log_dir/${cmd0/\.sh}.prm
unset params

######################################################################################################################
## run job:

out_pre=$(basename $in_file)

if [ -z "$SLURM_ARRAY_TASK_ID" ]; then
  out_pre="$out_dir/${out_pre/\.bam}.ccs.$SLURM_JOB_ID"
  cmd="singularity exec $container ccs --num-threads $cpus --report-file $out_pre.rpt --min-snr $min_snr \
      --min-length $min_length --min-passes $min_passes --min-rq $min_rq --log-level INFO $in_file $out_pre.bam"
else
  chunks=()
  for ((i=1; i<=$SLURM_ARRAY_TASK_COUNT; ++i)); do chunks+=("$i/$SLURM_ARRAY_TASK_COUNT"); done
  out_pre="$out_dir/${out_pre/\.bam}.ccs.$SLURM_ARRAY_JOB_ID.$SLURM_ARRAY_TASK_ID"
  cmd="singularity exec $container ccs --num-threads $cpus --report-file $out_pre.rpt --min-snr $min_snr \
      --min-length $min_length --min-passes $min_passes --min-rq $min_rq --log-level INFO \
      --chunk ${chunks[$SLURM_ARRAY_TASK_ID]} $in_file $out_pre.bam"
fi

srun $cmd
ecode=$?
echo "$task_id|$ecode|$cmd" >> $log_dir/${cmd0/\.sh}.log
[ "$ecode" -ne 0 ] && { exit_meta 16 "Error: command '$cmd' returned exit code '$ecode'."; }
unset out_pre cmd ecode

exit_meta 0 "Completed normally"

## DONE ##############################################################################################################

