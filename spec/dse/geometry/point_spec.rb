# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'
require 'dse'

module Dse
  module Geometry
    describe Point do
      let(:point) {Point.new(37.5, 21.1)}

      context :constructor do
        let(:fake_float) {double('fake_float')}

        it 'should handle float-like args' do
          expect(fake_float).to receive(:to_f).and_return(99.5)
          p = Point.new(1, fake_float)
          expect(p.x).to eq(1.0)
          expect(p.y).to eq(99.5)
        end

        it 'should error out if args are not float-able' do
          expect do
            Point.new(1, fake_float)
          end.to raise_error(ArgumentError)

          expect do
            Point.new(fake_float, 1)
          end.to raise_error(ArgumentError)
        end
      end

      it 'wkt should work' do
        expect(point.wkt).to eq('POINT (37.5 21.1)')
      end

      context :big_endian do
        let(:one_float) { make_big_float(1.0) }
        let(:two_float) { make_big_float(2.0) }
        let(:type) { make_big_int32(1) }
        let(:bad_type) { make_big_int32(2) }

        it 'should deserialize correctly' do
          test_point = Point.deserialize("\x00" + type + one_float + two_float)
          expect(Point.new(1.0, 2.0)).to eq(test_point)
        end

        it 'should raise an error if type is incorrect' do
          expect do
            Point.deserialize("\x00" + bad_type + one_float + two_float)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end

      context :little_endian do
        let(:one_float) { make_little_float(1.0) }
        let(:two_float) { make_little_float(2.0) }
        let(:type) { make_little_int32(1) }
        let(:bad_type) { make_little_int32(2) }

        it 'should deserialize correctly' do
          test_point = Point.deserialize("\x01" + type + one_float + two_float)
          expect(Point.new(1.0, 2.0)).to eq(test_point)
        end

        it 'should raise an error if type is incorrect' do
          expect do
            Point.deserialize("\x01" + bad_type + one_float + two_float)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end
    end
  end
end
