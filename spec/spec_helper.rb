# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

RSpec.configure do |config|
  config.expect_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end
end

require 'dse'
