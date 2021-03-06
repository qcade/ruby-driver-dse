# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'spec_helper'

module Dse
  module Graph
    # noinspection RubyStringKeysInHashInspection
    describe Options do
      let(:options) { Dse::Graph::Options.new }

      # This bears some explanation. Since we're doing some fun method creation logic in the Options class,
      # we need to test that those methods (getters/setters) work properly. That list may change over time,
      # and we want the test to auto-adjust, so we iterate over the option-names. We could avoid using eval
      # in the test and instead do calls like options.send(attr), but I wanted to emulate *exactly* what a
      # user would type when calling these methods. So, we construct the calls as strings and eval them.
      Dse::Graph::Options::OPTION_NAMES.each do |attr|
        # rubocop:disable Lint/Eval
        it "should allow setting/getting #{attr}" do
          expect(eval("options.#{attr}")).to be_nil
          eval "options.#{attr} = 'val'"
          expect(eval("options.#{attr}")).to eq('val')
        end
      end

      it 'should error out for getting/setting bad attribute' do
        expect { options.foo }.to raise_error NameError
        expect { options.foo = 7 }.to raise_error NameError
      end

      it 'should support initialization with a hash of options' do
        my_options = { graph_name: 'mygraph', junky: 'funky' }
        init_options = Dse::Graph::Options.new(my_options)

        # The constructor will drop junky, but it shouldn't tweak my_options itself; the caller owns that hash.
        expect(my_options).to eq(graph_name: 'mygraph', junky: 'funky')

        # init_options should have graph_name set, but that's it.
        expect(init_options.graph_name).to eq('mygraph')
        expect(init_options.graph_source).to be_nil
        expect(init_options.graph_language).to be_nil
        expect(init_options.graph_read_consistency).to be_nil
        expect(init_options.graph_write_consistency).to be_nil
        expect(init_options.as_payload).to_not include('junky', :junky)
      end

      context :merge do
        it 'should merge from default' do
          other = Dse::Graph::Options.new
          other.graph_source = 'other'
          final = options.merge(other)

          # options should not have changed.
          expect(options.graph_name).to be_nil
          expect(options.graph_source).to be_nil
          expect(options.graph_language).to be_nil
          expect(options.graph_read_consistency).to be_nil
          expect(options.graph_write_consistency).to be_nil

          # other should not have changed.
          expect(other.graph_name).to be_nil
          expect(other.graph_source).to eq('other')
          expect(other.graph_language).to be_nil
          expect(other.graph_read_consistency).to be_nil
          expect(other.graph_write_consistency).to be_nil

          # Since we started with empty-options, final should == other.
          expect(final).to eq(other)
        end

        it 'should merge timeout with base nil' do
          other = Dse::Graph::Options.new(timeout: 7)
          final = options.merge(other)

          # options should not have changed.
          expect(options.timeout).to be_nil

          expect(final.timeout).to eq(other.timeout)
        end

        it 'should merge timeout with base non-nil' do
          other = Dse::Graph::Options.new(timeout: 7)
          options.timeout = 3
          final = options.merge(other)

          expect(final.timeout).to eq(other.timeout)
        end

        it 'should not merge nil timeout' do
          other = Dse::Graph::Options.new
          options.timeout = 3
          final = options.merge(other)

          expect(final.timeout).to eq(options.timeout)
        end

        it 'should treat nil merge as no-op' do
          final = options.merge(nil)

          # options should not have changed.
          expect(options.graph_name).to be_nil
          expect(options.graph_source).to be_nil
          expect(options.graph_language).to be_nil
          expect(options.graph_read_consistency).to be_nil
          expect(options.graph_write_consistency).to be_nil

          # Since we started with empty-options, final should == other.
          expect(final).to be(options)
        end

        it 'should merge and mix' do
          options.graph_name = 'mygraph'
          options.graph_source = 'orig_source'

          other = Dse::Graph::Options.new
          other.graph_source = 'other'
          other.graph_language = 'mylang'
          final = options.merge(other)

          # options should not have changed.
          expect(options.graph_name).to eq('mygraph')
          expect(options.graph_source).to eq('orig_source')
          expect(options.graph_language).to be_nil
          expect(options.graph_read_consistency).to be_nil
          expect(options.graph_write_consistency).to be_nil

          # other should not have changed.
          expect(other.graph_name).to be_nil
          expect(other.graph_source).to eq('other')
          expect(other.graph_language).to eq('mylang')
          expect(other.graph_read_consistency).to be_nil
          expect(other.graph_write_consistency).to be_nil

          # The merge result should have values from 'other' taking precedence
          # over 'options'
          expect(final.graph_name).to eq('mygraph')
          expect(final.graph_source).to eq('other')
          expect(final.graph_language).to eq('mylang')
          expect(final.graph_read_consistency).to be_nil
          expect(final.graph_write_consistency).to be_nil
        end
      end

      context 'merge!' do
        it 'should merge timeout and internal state' do
          other = Dse::Graph::Options.new(timeout: 7)
          options.merge!(other)
          expect(options.timeout).to eq(other.timeout)
          expect(options.instance_variable_get(:@real_options)).to eq(other.instance_variable_get(:@real_options))
        end
      end

      context :set do
        it 'should ignore nil value options' do
          options.set('graph-name', nil)
          expect(options.graph_name).to be_nil
        end

        it 'should set standard options' do
          options.set('graph-name', 'mygraph')
          expect(options.graph_name).to eq('mygraph')
        end

        it 'should set arbitrary options' do
          options.set(:super_cool, 'value')
          expect(options.as_payload).to include('super-cool' => 'value')
        end

        it 'should set timeout (symbol) properly' do
          options.set(:timeout, 7)
          expect(options.as_payload).to include('request-timeout' => [7000].pack('Q>'))
          expect(options.timeout).to eq(7)
        end

        it 'should set timeout (string) properly' do
          options.set('timeout', 7)
          expect(options.as_payload).to include('request-timeout' => [7000].pack('Q>'))
          expect(options.timeout).to eq(7)
        end
      end

      context :delete do
        it 'should delete standard options' do
          options.set('graph-name', 'mygraph')
          options.delete(:graph_name)
          expect(options.graph_name).to be_nil
        end

        it 'should delete arbitrary options' do
          options.set(:super_cool, 'value')
          options.delete('super-cool')
          expect(options.as_payload).to_not include('super-cool')
        end

        it 'should delete timeout' do
          options.timeout = 8
          options.delete(:timeout)
          expect(options.timeout).to be_nil
          expect(options.as_payload).to_not include('request-timeout')

          options.timeout = 8
          options.delete('timeout')
          expect(options.timeout).to be_nil
          expect(options.as_payload).to_not include('request-timeout')
        end
      end

      context 'timeout=' do
        it 'should include request-timeout option if there is a timeout assigned' do
          options = Dse::Graph::Options.new
          options.timeout = 7
          expect(options.as_payload).to eq('request-timeout' => [7000].pack('Q>'),
                                           'graph-source' => 'g',
                                           'graph-language' => 'gremlin-groovy')
        end

        it 'should ignore explicitly provided request-timeout option' do
          options = Dse::Graph::Options.new(request_timeout: 7000)
          expect(options.as_payload).to eq('graph-source' => 'g',
                                           'graph-language' => 'gremlin-groovy')

          options = Dse::Graph::Options.new('request-timeout' => 7000)
          expect(options.as_payload).to eq('graph-source' => 'g',
                                           'graph-language' => 'gremlin-groovy')
        end

        it 'should clear request-timeout if timeout is assigned nil' do
          options = Dse::Graph::Options.new(timeout: 7)

          options.timeout = nil
          expect(options.timeout).to be_nil
          expect(options.as_payload).to_not include('request-timeout')
        end
      end

      context :as_payload do
        it 'should produce a payload only with non-nil entries' do
          expect(options.as_payload).to eq('graph-source' => 'g',
                                           'graph-language' => 'gremlin-groovy')
        end

        it 'should include option attributes mixed with defaults' do
          options.graph_name = 'mygraph'
          options.graph_source = 'a'
          expect(options.as_payload).to eq('graph-name' => 'mygraph',
                                           'graph-source' => 'a',
                                           'graph-language' => 'gremlin-groovy')
        end

        it 'should include request-timeout option if there is a timeout' do
          options = Dse::Graph::Options.new(timeout: 7)
          expect(options.as_payload).to eq('request-timeout' => [7000].pack('Q>'),
                                           'graph-source' => 'g',
                                           'graph-language' => 'gremlin-groovy')
        end
      end
    end
  end
end
