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
    describe Duration do
      it 'should produce a valid string representation' do
        expect(Duration.new(1, 2, 3, 4).to_s).to eq('P1DT2H3M4.0S')
        expect(Duration.new(nil, nil, nil, nil).to_s).to eq('P0DT0H0M0.0S')
      end

      context :initialize do
        it 'should coerce nil args to the right types' do
          duration = Duration.new(nil, nil, nil, nil)
          expect(duration.days).to eq(0)
          expect(duration.hours).to eq(0)
          expect(duration.minutes).to eq(0)
          expect(duration.seconds).to eq(0)

          expect(duration.days).to be_a(Fixnum)
          expect(duration.hours).to be_a(Fixnum)
          expect(duration.minutes).to be_a(Fixnum)
          expect(duration.seconds).to be_a(Float)
        end

        it 'should coerce string args to the right types' do
          duration = Duration.new('2', '-3', '4', '5')
          expect(duration.days).to eq(2)
          expect(duration.hours).to eq(-3)
          expect(duration.minutes).to eq(4)
          expect(duration.seconds).to eq(5)

          expect(duration.days).to be_a(Fixnum)
          expect(duration.hours).to be_a(Fixnum)
          expect(duration.minutes).to be_a(Fixnum)
          expect(duration.seconds).to be_a(Float)
        end
      end

      context 'accessors' do
        let (:duration) { Duration.new(0, 0, 0, 0) }

        it 'should coerce days properly' do
          duration.days = '3'
          expect(duration.days).to eq(3)
          expect(duration.days).to be_a(Fixnum)
        end

        it 'should coerce hours properly' do
          duration.hours = '3'
          expect(duration.hours).to eq(3)
          expect(duration.hours).to be_a(Fixnum)
        end

        it 'should coerce minutes properly' do
          duration.minutes = '3'
          expect(duration.minutes).to eq(3)
          expect(duration.minutes).to be_a(Fixnum)
        end

        it 'should coerce seconds properly' do
          duration.seconds = '3'
          expect(duration.seconds).to eq(3)
          expect(duration.seconds).to be_a(Float)
        end
      end

      context :from_dse do
        it 'should error out if string from DSE is not recognized' do
          expect do
            Duration.from_dse(nil)
          end.to raise_error(ArgumentError)

          expect do
            Duration.from_dse(3.5)
          end.to raise_error(ArgumentError)

          expect do
            Duration.from_dse('P')
          end.to raise_error(ArgumentError)
        end

        it 'should handle fully specified positive durations' do
          expect(Duration.from_dse('P2DT3H4M5.6S')).to eq(Duration.new(2, 3, 4, 5.6))
        end

        it 'should handle fully specified negative durations' do
          expect(Duration.from_dse('P-2DT-3H-4M-5.6S')).to eq(Duration.new(-2, -3, -4, -5.6))
        end

        it 'should handle partially specified durations' do
          expect(Duration.from_dse('PT-5S')).to eq(Duration.new(0, 0, 0, -5))
        end
      end
    end
  end
end
