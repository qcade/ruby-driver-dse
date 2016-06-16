# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  module Graph
    describe Options do
      let(:options) { Dse::Graph::Options.new }

      # This bears some explanation. Since we're doing some fun method creation logic in the Options class,
      # we need to test that those methods (getters/setters) work properly. That list may change over time,
      # and we want the test to auto-adjust, so we iterate over the option-names. We could avoid using eval
      # in the test and instead do calls like options.send(attr), but I wanted to emulate *exactly* what a
      # use would type when calling these methods. So, we construct the calls as strings and eval them.
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
      end
    end
  end
end
