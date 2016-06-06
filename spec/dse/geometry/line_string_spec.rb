# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'
require 'dse'

module Dse
  module Geometry
    describe LineString do
      let(:line_string) do
        LineString.new([
                         Point.new(37.5, 21.1),
                         Point.new(12.5, 22.1),
                         Point.new(15.5, 1.1)
                       ])
      end

      it 'wkt should work' do
        expect(line_string.wkt).to eq('LINESTRING (37.5 21.1, 12.5 22.1, 15.5 1.1)')
      end

      context :big_endian do
        let(:one_float) { make_big_float(1.0) }
        let(:two_float) { make_big_float(2.0) }
        let(:three_float) { make_big_float(3.0) }
        let(:four_float) { make_big_float(4.0) }
        let(:type) { make_big_int32(2) }
        let(:num_points) { make_big_int32(3) }
        let(:bad_type) { make_big_int32(1) }

        it 'should deserialize correctly' do
          test_line_string = LineString.deserialize("\x00" + type + num_points +
                                                        one_float + two_float +
                                                        three_float + four_float +
                                                        two_float + one_float)
          expect(LineString.new([
                                  Point.new(1.0, 2.0),
                                  Point.new(3.0, 4.0),
                                  Point.new(2, 1)
                                ])).to eq(test_line_string)
        end

        it 'should raise an error if type is incorrect' do
          expect do
            LineString.deserialize("\x00" + bad_type + num_points +
                                       one_float + two_float +
                                       three_float + four_float +
                                       two_float + one_float)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end

      context :little_endian do
        let(:one_float) { make_little_float(1.0) }
        let(:two_float) { make_little_float(2.0) }
        let(:three_float) { make_little_float(3.0) }
        let(:four_float) { make_little_float(4.0) }
        let(:type) { make_little_int32(2) }
        let(:num_points) { make_little_int32(3) }
        let(:bad_type) { make_little_int32(1) }

        it 'should deserialize correctly' do
          test_line_string = LineString.deserialize("\x01" + type + num_points +
                                                        one_float + two_float +
                                                        three_float + four_float +
                                                        two_float + one_float)
          expect(LineString.new([
                                  Point.new(1.0, 2.0),
                                  Point.new(3.0, 4.0),
                                  Point.new(2, 1)
                                ])).to eq(test_line_string)
        end

        it 'should raise an error if type is incorrect' do
          expect do
            LineString.deserialize("\x01" + bad_type + num_points +
                                       one_float + two_float +
                                       three_float + four_float +
                                       two_float + one_float)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end
    end
  end
end
