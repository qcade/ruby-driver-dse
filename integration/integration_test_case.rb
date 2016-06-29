# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/../support/ccm.rb'
require File.dirname(__FILE__) + '/../support/retry.rb'
require File.dirname(__FILE__) + '/schema_change_listener.rb'
require 'minitest/unit'
require 'minitest/autorun'
require 'dse'
require 'ansi/code'

class IntegrationTestCase < MiniTest::Unit::TestCase
  @@ccm_cluster = nil

  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(1, 1) unless self == IntegrationTestCase
  end

  def self.after_suite
  end

  def before_setup
    puts ANSI::Code.magenta("\n===== Begin #{__name__} ====")
  end

  def assert_columns(expected_names_and_types, actual_columns)
    assert_equal(expected_names_and_types.size, actual_columns.size)

    expected_names_and_types.zip(actual_columns) do |expected, actual_column|
      assert_equal expected[0], actual_column.name
      assert_equal expected[1], actual_column.type.kind
    end
  end
end

class IntegrationUnit < MiniTest::Unit
  def before_suites
  end

  def after_suites
  end

  def _run_suites(suites, type)
    before_suites
    super(suites, type)
  ensure
    after_suites
  end

  def _run_suite(suite, type)
    suite.before_suite if suite.respond_to?(:before_suite)
    super(suite, type)
  ensure
    suite.after_suite if suite.respond_to?(:after_suite)
  end
end

MiniTest::Unit.runner = IntegrationUnit.new
