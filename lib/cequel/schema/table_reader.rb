module Cequel

  module Schema

    class TableReader

      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/

      def self.read(table_data, column_data)
        new(table_data, column_data).read
      end

      def initialize(table_data, column_data)
        @table_data, @column_data = table_data, column_data
        @table = Table.new(table_data['columnfamily_name'].to_sym)
      end
      private_class_method(:new)

      def read
        read_partition_keys
        read_nonpartition_keys
        read_data_columns
        @table
      end

      private

      def read_partition_keys
        types = parse_composite_types(@table_data['key_validator'])
        JSON.parse(@table_data['key_aliases']).zip(types) do |key_alias, type|
          name = key_alias.to_sym
          @table.add_partition_key(key_alias.to_sym, Type.lookup_internal(type))
        end
      end

      def read_nonpartition_keys
        column_aliases = JSON.parse(@table_data['column_aliases'])
        comparators = parse_composite_types(@table_data['comparator'])
        column_aliases.zip(comparators) do |column_alias, type|
          if REVERSED_TYPE_PATTERN =~ type
            type = $1
            clustering_order = :desc
          end
          @table.add_nonpartition_key(
            column_alias.to_sym,
            Type.lookup_internal(type),
            clustering_order
          )
        end
      end

      def read_data_columns
        @column_data.each do |result|
          @table.add_column(
            result['column_name'].to_sym,
            Type.lookup_internal(result['validator']),
            nil
          )
        end
      end

      def parse_composite_types(type_string)
        if COMPOSITE_TYPE_PATTERN =~ type_string
          $1.split(',')
        else
          [type_string]
        end
      end

    end

  end

end
