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

def make_big_float(x)
  [x].pack('G')
end

def make_little_float(x)
  [x].pack('E')
end

def make_big_int32(x)
  [x].pack('L>')
end

def make_little_int32(x)
  [x].pack('L<')
end
