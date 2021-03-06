# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

class DatatypeUtils
  def self.primitive_datatypes
    @@primitive_types ||= begin
      primitive_types = ['ascii',
                          'bigint',
                          'blob',
                          'boolean',
                          'decimal',
                          'double',
                          'float',
                          'inet',
                          'int',
                          'text',
                          'timestamp',
                          'timeuuid',
                          'uuid',
                          'varchar',
                          'varint'
      ]

      primitive_types.push('date', 'time', 'smallint', 'tinyint') if CCM.cassandra_version >= '2.2.0'
      primitive_types
    end
  end

  def self.graph_datatypes
    @@graph_datatypes ||= ['bigint',
                           'blob',
                           'boolean',
                           'decimal',
                           'double',
                           'duration',
                           'float',
                           'inet',
                           'int',
                           'text',
                           'timestamp',
                           'uuid',
                           'varint',
                           'smallint',
                           'point',
                           'linestring',
                           'polygon'
    ]
  end

  def self.collection_types
    @@collection_types ||= begin
      collection_types = %w(List Map Set)

      collection_types.push('Tuple') if CCM.cassandra_version >= '2.1.0'
      collection_types
    end
  end

  def self.get_sample(datatype)
    case datatype
    when 'ascii' then 'ascii'
    when 'bigint' then 765438000
    when 'blob' then 'YmxvYg=='
    when 'boolean' then true
    when 'decimal' then ::BigDecimal.new('1313123123.234234234234234234123')
    when 'double' then 3.141592653589793
    when 'duration' then Dse::Graph::Duration.new(2, 3, 4, 1.529)
    when 'float' then 1.25
    when 'inet' then ::IPAddr.new('200.199.198.197')
    when 'int' then 4
    when 'text' then 'text'
    when 'timestamp' then ::Time.at(1358013521, 123000)
    when 'timeuuid' then Cassandra::TimeUuid.new('FE2B4360-28C6-11E2-81C1-0800200C9A66')
    when 'uuid' then Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
    when 'varchar' then 'varchar'
    when 'varint' then 67890656781923123918798273492834712837198237
    when 'date' then Cassandra::Types::Date.new(::Time.at(1358013521).to_date)
    when 'time' then Cassandra::Time.new(1358013521)
    when 'smallint' then 425
    when 'tinyint' then 127
    when 'point' then Dse::Geometry::Point.new(38.0, 21.0)
    when 'linestring' then Dse::Geometry::LineString.new('LINESTRING (30 10, 10 30, 40 40)')
    when 'polygon' then Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                                    (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))')
    else raise 'Missing handling of: ' + datatype
    end
  end

  def self.get_collection_sample(complex_type, datatype)
    case complex_type
    when 'List' then [get_sample(datatype), get_sample(datatype)]
    when 'Set' then Set.new([get_sample(datatype)])
    when 'Map' then
        if datatype == 'blob'
          {get_sample('ascii') => get_sample(datatype)}
        else
          {get_sample(datatype) => get_sample(datatype)}
        end
    when 'Tuple' then Cassandra::Tuple.new(get_sample(datatype))
      else raise 'Missing handling of non-primitive type: ' + complex_type
    end
  end
end
