# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'
require 'dse'

module Dse
  module Geometry
    describe Polygon do
      let(:ring1) { LineString.new(Point.new(37, 21), Point.new(12, 22), Point.new(15, 1), Point.new(37, 21)) }
      let(:ring2) { LineString.new(Point.new(3, 2), Point.new(6, 2), Point.new(6, 5), Point.new(3, 2)) }
      let(:ring3) { LineString.new(Point.new(13, 2), Point.new(6, 2), Point.new(6, 5), Point.new(3, 2)) }
      let(:polygon) { Polygon.new(ring1, ring2) }

      context 'simple polygon' do
        let(:exterior_ring) do
          LineString.new(Point.new(37.5, 21.1), Point.new(12.5, 22.1), Point.new(15.5, 1.1), Point.new(37.5, 21.1))
        end
        let(:simple_polygon) { Polygon.new(exterior_ring) }

        it 'should have an exterior ring' do
          expect(simple_polygon.exterior_ring).to be(exterior_ring)
        end

        it 'should not have an interior ring' do
          interior_rings = simple_polygon.interior_rings
          expect(interior_rings).to be_empty
          expect { interior_rings << ring3 }.to raise_error(RuntimeError)
        end

        it '#wkt should work' do
          expect(simple_polygon.wkt).to eq('POLYGON ((37.5 21.1, 12.5 22.1, 15.5 1.1, 37.5 21.1))')
          expect(Polygon.new.wkt).to eq('POLYGON ()')
        end
      end

      context :constructor do
        it 'should handle LineString args' do
          expect(polygon.exterior_ring).to be(ring1)
          expect(polygon.interior_rings).to eq([ring2])
        end

        it 'should handle no args' do
          polygon = Polygon.new
          expect(polygon.exterior_ring).to be_nil
          expect(polygon.interior_rings).to be_empty
        end

        it 'should error out if linear-ring args are not all LineStrings' do
          expect { Polygon.new(ring1, 1) }.to raise_error(ArgumentError)
          expect { Polygon.new('foo', ring1) }.to raise_error(ArgumentError)
          expect { Polygon.new(ring1, ring2, nil) }.to raise_error(ArgumentError)
        end

        it 'should error out if one-arg form is not a WKT' do
          expect { Polygon.new(1) }.to raise_error(ArgumentError)
          expect { Polygon.new(nil) }.to raise_error(ArgumentError)
        end
      end

      it 'should not allow mutation of rings' do
        expect { polygon.interior_rings << ring3 }.to raise_error(RuntimeError)
      end

      context :big_endian do
        let(:one_float) { make_big_float(1.0) }
        let(:two_float) { make_big_float(2.0) }
        let(:four_float) { make_big_float(4.0) }
        let(:ten_float) { make_big_float(10.0) }
        let(:type) { make_big_int32(3) }
        let(:five_int) { make_big_int32(5) }
        let(:two_int) { make_big_int32(2) }
        let(:one_int) { make_big_int32(1) }
        let(:bad_type) { make_big_int32(1) }
        let(:exterior) do
          # A line-string's "meat" is the number of points followed by the points.
          # We're going to do the following four points: (1,1), (10,1), (10,10), (1,10), (1,1)
          five_int +
            one_float + one_float +
            ten_float + one_float +
            ten_float + ten_float +
            one_float + ten_float +
            one_float + one_float
        end
        let(:interior) do
          # Define an interior ring (clockwise) to cut out of the exterior.
          five_int +
            two_float + two_float +
            two_float + four_float +
            four_float + four_float +
            four_float + two_float +
            two_float + two_float
        end

        it 'should deserialize a polygon that only has an exterior ring' do
          test_polygon = Polygon.deserialize("\x00" + type + one_int + exterior)
          expect(Polygon.new(LineString.new(Point.new(1.0, 1.0),
                                            Point.new(10.0, 1.0),
                                            Point.new(10.0, 10.0),
                                            Point.new(1.0, 10.0),
                                            Point.new(1.0, 1.0)
                                           ))).to eq(test_polygon)
          expect(test_polygon.interior_rings).to be_empty
        end

        it 'should deserialize a polygon that has an exterior and interior rings' do
          test_polygon = Polygon.deserialize("\x00" + type + two_int +
                                                 exterior + interior)
          interior_ring = LineString.new(Point.new(2.0, 2.0),
                                         Point.new(2.0, 4.0),
                                         Point.new(4.0, 4.0),
                                         Point.new(4.0, 2.0),
                                         Point.new(2.0, 2.0))
          expect(Polygon.new(LineString.new(Point.new(1.0, 1.0),
                                            Point.new(10.0, 1.0),
                                            Point.new(10.0, 10.0),
                                            Point.new(1.0, 10.0),
                                            Point.new(1.0, 1.0)),
                             interior_ring)).to eq(test_polygon)
          expect(test_polygon.interior_rings).to eq([interior_ring])
        end

        it 'should raise an error if type is incorrect' do
          expect do
            Polygon.deserialize("\x00" + bad_type + one_int + exterior)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end

      context :little_endian do
        let(:one_float) { make_little_float(1.0) }
        let(:two_float) { make_little_float(2.0) }
        let(:four_float) { make_little_float(4.0) }
        let(:ten_float) { make_little_float(10.0) }
        let(:type) { make_little_int32(3) }
        let(:five_int) { make_little_int32(5) }
        let(:two_int) { make_little_int32(2) }
        let(:one_int) { make_little_int32(1) }
        let(:bad_type) { make_little_int32(1) }
        let(:exterior) do
          # A line-string's "meat" is the number of points followed by the points.
          # We're going to do the following four points: (1,1), (10,1), (10,10), (1,10), (1,1)
          five_int +
            one_float + one_float +
            ten_float + one_float +
            ten_float + ten_float +
            one_float + ten_float +
            one_float + one_float
        end
        let(:interior) do
          # Define an interior ring (clockwise) to cut out of the exterior.
          five_int +
            two_float + two_float +
            two_float + four_float +
            four_float + four_float +
            four_float + two_float +
            two_float + two_float
        end

        it 'should deserialize a polygon that only has an exterior ring' do
          test_polygon = Polygon.deserialize("\x01" + type + one_int + exterior)
          expect(Polygon.new(LineString.new(Point.new(1.0, 1.0),
                                            Point.new(10.0, 1.0),
                                            Point.new(10.0, 10.0),
                                            Point.new(1.0, 10.0),
                                            Point.new(1.0, 1.0)))).to eq(test_polygon)
          expect(test_polygon.interior_rings).to be_empty
        end

        it 'should deserialize a polygon that has an exterior and interior rings' do
          test_polygon = Polygon.deserialize("\x01" + type + two_int +
                                                 exterior + interior)
          interior_ring = LineString.new(Point.new(2.0, 2.0),
                                         Point.new(2.0, 4.0),
                                         Point.new(4.0, 4.0),
                                         Point.new(4.0, 2.0),
                                         Point.new(2.0, 2.0))
          expect(Polygon.new(LineString.new(Point.new(1.0, 1.0),
                                            Point.new(10.0, 1.0),
                                            Point.new(10.0, 10.0),
                                            Point.new(1.0, 10.0),
                                            Point.new(1.0, 1.0)),
                             interior_ring)).to eq(test_polygon)
          expect(test_polygon.interior_rings).to eq([interior_ring])
        end

        it 'should raise an error if type is incorrect' do
          expect do
            Polygon.deserialize("\x01" + bad_type + one_int + exterior)
          end.to raise_error(Cassandra::Errors::DecodingError)
        end
      end
    end
  end
end
