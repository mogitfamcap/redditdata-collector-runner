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

  exit 0
end

while true
  run_session
end
