#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

Given(/^it is running interactively$/) do
  step 'I run `ruby -I. -rbundler/setup example.rb` interactively'
end

When(/^I type "(.*?)" (\d+) times$/) do |input, count|
  count.to_i.times do
    step "I type \"#{input}\""
  end
end
