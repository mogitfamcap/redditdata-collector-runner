require 'parseconfig'
require 'fileutils'

require './util.rb'

$incremental_count_file = File.dirname(__FILE__) + '/.incremental_runs_since_full'

def get_incremental_count
  if !File.exist? $incremental_count_file
    return 0
  end

  file = File.open($incremental_count_file, 'rb')
  contents = file.read
  file.close

  contents.to_i
end

def set_incremental_count(count)
  File.open($incremental_count_file, 'w') { |file| file.write(count.to_s) }
end


def collect_dataset(dataset, collector_directory, database_location, logfile, regex, mode)
  command = "ruby #{collector_directory}bin/redditdata-collector collect #{database_location} #{dataset} #{mode} '#{regex}' >> #{logfile} 2>&1"
  Util.log "Running command: #{command}"
  exit_code = system(command)
  Util.log "Exit code: #{exit_code}"
  if exit_code != true then
    raise "Collecting #{dataset} data failed"
  end

end



def run_session
  Util.log 'Session has started'

  config = ParseConfig.new('redditdata-collector-runner.config')
  collector_directory = config['collector_directory']
  data_directory = config['data_directory']
  log_directory = config['log_directory']
  sleep_between_sessions_seconds = config['sleep_between_sessions_seconds']
  regex = config['regex']
  incremental_runs_per_full = config['incremental_runs_per_full'].to_i

  Util.log "collector_directory=#{collector_directory}"
  Util.log "data_directory=#{data_directory}"
  Util.log "log_directory=#{log_directory}"
  Util.log "sleep_between_sessions_seconds=#{sleep_between_sessions_seconds}"
  Util.log "regex=#{regex}"
  Util.log "incremental_runs_per_full=#{incremental_runs_per_full}"

  timestamp = Time.now.strftime('%Y-%m-%d_%H-%M-%S')
  Util.log "timestamp=#{timestamp}"

  static_location = data_directory + 'redditdata.db'
  temp_location = data_directory + 'redditdata.db' + '.' + timestamp + '.temp' + '.db'

  incremental_runs_since_full = get_incremental_count
  mode = (incremental_runs_since_full < incremental_runs_per_full) ? 'incremental' : 'full'

  if File.exist? static_location
    Util.log "Copying #{static_location} to #{temp_location}"
    FileUtils.copy static_location, temp_location
  else
    mode = 'full'
  end

  begin
    collect_dataset('subreddits', collector_directory, temp_location, log_directory + timestamp + '_' + 'subreddits' + '.log', regex, mode)
    collect_dataset('links', collector_directory, temp_location, log_directory + timestamp + '_' + 'links' + '.log', regex, mode)
    collect_dataset('users', collector_directory, temp_location, log_directory + timestamp + '_' + 'users' + '.log', regex, mode)
    collect_dataset('userlinks', collector_directory, temp_location, log_directory + timestamp + '_' + 'userlinks' + '.log', regex, mode)

    Util.log "Copying #{temp_location} to #{static_location}"
    FileUtils.copy temp_location, static_location

    Util.log "Copying #{temp_location} to #{temp_location.sub('.temp', '')}"
    FileUtils.copy temp_location, temp_location.sub('.temp', '')

    Util.log "Removing #{temp_location}"
    FileUtils.rm temp_location

    incremental_runs_since_full = (mode == 'full') ? 0 : incremental_runs_since_full + 1
    Util.log "Setting incremental_runs_since_full to #{incremental_runs_since_full}"
    set_incremental_count incremental_runs_since_full
  rescue StandardError => error
    Util.log 'Session has failed'
    Util.log error.message
  end

  Util.log 'Session has completed'
  sleep sleep_between_sessions_seconds.to_i
end

while true
  run_session
end
