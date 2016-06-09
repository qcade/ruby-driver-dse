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
        it 'should handle float-like args' do
          p = Point.new(1, 99.5)
          expect(p.x).to eq(1.0)
          expect(p.y).to eq(99.5)
        end

        it 'should error out if two-args form are not float-able' do
          expect { Point.new(1, Object.new) }.to raise_error(ArgumentError)
          expect { Point.new(1, Object.new) }.to raise_error(ArgumentError)
          expect { Point.new(Object.new, 1) }.to raise_error(ArgumentError)

          # We specifically don't want to accept string or nil.
          expect { Point.new('abc', 1) }.to raise_error(ArgumentError)
          expect { Point.new(1, 'abc') }.to raise_error(ArgumentError)
          expect { Point.new(nil, 1) }.to raise_error(ArgumentError)
          expect { Point.new(1, nil) }.to raise_error(ArgumentError)

          # And Nan.
          expect { Point.new(Float::NAN, 1) }.to raise_error(ArgumentError)
          expect { Point.new(1, Float::NAN) }.to raise_error(ArgumentError)
        end

        it 'should error out if one-arg form is not a Point WKT' do
          expect { Point.new(1) }.to raise_error(ArgumentError)
          expect { Point.new(nil) }.to raise_error(ArgumentError)
          expect { Point.new('LINESTRING (1 2, 3 4)') }.to raise_error(ArgumentError)
          expect { Point.new('POINT (1 2, 3 4)') }.to raise_error(ArgumentError)
          expect { Point.new('POINT (a 7)') }.to raise_error(ArgumentError)
        end

        it 'should process correct WKT' do
          expect(Point.new('POINT (3.7 -5)')).to eq(Point.new(3.7, -5.0))
          expect(Point.new('POINT ( 3.7 -5 )')).to eq(Point.new(3.7, -5.0))
          expect(Point.new('POINT( 3.7 -5 )')).to eq(Point.new(3.7, -5.0))
        end
      end

      it '#wkt should work' do
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
