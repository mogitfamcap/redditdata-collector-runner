require 'parseconfig'

require './util.rb'

def collect_dataset(dataset, collector_directory, database_location, logfile, regex)
  command = "ruby #{collector_directory}bin/redditdata-collector #{database_location} #{dataset} full '#{regex}' >> #{logfile} 2>&1"
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

  Util.log "collector_directory=#{collector_directory}"
  Util.log "data_directory=#{data_directory}"
  Util.log "log_directory=#{log_directory}"
  Util.log "sleep_between_sessions_seconds=#{sleep_between_sessions_seconds}"
  Util.log "regex=#{regex}"

  timestamp = Time.now.strftime('%Y-%m-%d_%H-%M-%S')
  Util.log "timestamp=#{timestamp}"

  static_location = data_directory + 'redditdata.db'
  temp_location = data_directory + 'redditdata.db' + '.' + timestamp + '.temp' + '.db'

  if File.exist? static_location
    Util.log "Copying #{static_location} to #{temp_location}"
    FileUtils.copy static_location, temp_location
  end

  begin
    collect_dataset('subreddits', collector_directory, temp_location, log_directory + timestamp + '_' + 'subreddits' + '.log', regex)
    collect_dataset('links', collector_directory, temp_location, log_directory + timestamp + '_' + 'links' + '.log', regex)
    collect_dataset('users', collector_directory, temp_location, log_directory + timestamp + '_' + 'users' + '.log', regex)
    collect_dataset('userlinks', collector_directory, temp_location, log_directory + timestamp + '_' + 'userlinks' + '.log', regex)

    Util.log "Copying #{temp_location} to #{static_location}"
    FileUtils.copy temp_location, static_location

    Util.log "Copying #{temp_location} to #{temp_location.sub('.temp', '')}"
    FileUtils.copy temp_location, temp_location.sub('.temp', '')

    Util.log "Removing #{temp_location}"
    FileUtils.rm temp_location
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
