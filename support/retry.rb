#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Retry extend self
  def with_attempts(attempts, *errors)
    errors = [Cassandra::Errors::ValidationError, Cassandra::Errors::ExecutionError] if errors.empty?
    total ||= attempts + 1
    return yield
  rescue *errors => e
    raise e if (attempts -= 1).zero?
    wait = (total - attempts) * 2
    puts "#{e.class.name}: #{e.message}, retrying in #{wait}s..."
    sleep(wait)
    retry
  end
end
