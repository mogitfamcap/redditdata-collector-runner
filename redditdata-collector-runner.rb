require 'parseconfig'

require './util.rb'

def run_session
  Util.log 'Session has started'

  config = ParseConfig.new('redditdata-collector-runner.config')
  data_directory = config['data_directory']
  log_directory = config['log_directory']
  sleep_between_sessions_seconds = config['sleep_between_sessions_seconds']
  regex = config['regex']

  Util.log "data_directory=#{data_directory}"
  Util.log "log_directory=#{log_directory}"
  Util.log "sleep_between_sessions_seconds=#{sleep_between_sessions_seconds}"
  Util.log "regex=#{regex}"

  timestamp = Time.now.strftime('%Y-%m-%d_%H-%M-%S')
  Util.log "timestamp=#{timestamp}"

  static_location = data_directory + 'redditdata.db'
  temp_location = data_directory + 'redditdata.db' + '.' + timestamp + '.db'

  if File.exist? static_location
    Util.log "Copying #{static_location} to #{temp_location}"
    FileUtils.copy static_location, temp_location
  end

  begin

  rescue StandardError => error
    Util.log error.message
    Util.log error.backtrace
    sleep sleep_between_sessions_seconds.to_i
  end

  Util.log 'Session has completed'
  sleep sleep_between_sessions_seconds.to_i
end

while true
  run_session
end
