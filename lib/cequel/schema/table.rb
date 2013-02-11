require 'stringio'

module Cequel

  module Schema

    class Table

      attr_reader :name, :partition_keys, :nonpartition_keys, :clustering_order
      attr_writer :compact_storage

      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/

      def self.read(table_data)
        new(table_data['columnfamily_name'].to_sym).tap do |table|
          table.read_partition_keys(table_data)
          table.read_nonpartition_keys(table_data)
        end
      end

      def initialize(name)
        @name = name
        @partition_keys, @nonpartition_keys, @columns, @properties,
          @clustering_order = [], [], [], [], []
      end

      def read_partition_keys(table_data)
        types = parse_composite_types(table_data['key_validator'])
        JSON.parse(table_data['key_aliases']).zip(types) do |key_alias, type|
          name = key_alias.to_sym
          add_partition_key(key_alias.to_sym, Type.lookup_internal(type))
        end
      end

      def read_nonpartition_keys(table_data)
        column_aliases = JSON.parse(table_data['column_aliases'])
        comparators = parse_composite_types(table_data['comparator'])
        column_aliases.zip(comparators) do |column_alias, type|
          if REVERSED_TYPE_PATTERN =~ type
            type = $1
            clustering_order = :desc
          end
          add_nonpartition_key(
            column_alias.to_sym,
            Type.lookup_internal(type),
            clustering_order
          )
        end
      end

      def add_key(name, type, clustering_order = nil)
        if @partition_keys.empty?
          unless clustering_order.nil?
            raise ArgumentError,
              "Can't set clustering order for partition key #{name}"
          end
          add_partition_key(name, type)
        else
          add_nonpartition_key(name, type, clustering_order)
        end
      end

      def add_partition_key(name, type)
        column = Column.new(name, type)
        @columns << column
        @partition_keys << column
      end

      def add_nonpartition_key(name, type, clustering_order = nil)
        column = Column.new(name, type)
        @columns << column
        @nonpartition_keys << column
        @clustering_order << (clustering_order || :asc)
      end

      def add_column(name, type, index_name)
        index_name = :"#{@name}_#{name}_idx" if index_name == true
        Column.new(name, type, index_name).tap { |column| @columns << column }
      end

      def add_list(name, type)
        @columns << List.new(name, type)
      end

      def add_set(name, type)
        @columns << Set.new(name, type)
      end

      def add_map(name, key_type, value_type)
        @columns << Map.new(name, key_type, value_type)
      end

      def add_property(name, value)
        @properties << TableProperty.new(name, value)
      end

      def create_cql
        create_statement = "CREATE TABLE #{@name} (#{columns_cql}, #{keys_cql})"
        properties = properties_cql
        create_statement << " WITH #{properties}" if properties
        [create_statement, *index_statements]
      end

      private

      def index_statements
        [].tap do |statements|
          @columns.each do |column|
            if column.indexed?
              statements <<
                "CREATE INDEX #{column.index_name} ON #{@name} (#{column.name})"
            end
          end
        end
      end

      def columns_cql
        @columns.map(&:to_cql).join(', ')
      end

      def key_columns_cql
        @keys.map { |key| "#{key.name} #{key.type}" }.join(', ')
      end

      def keys_cql
        partition_cql = @partition_keys.map { |key| key.name }.join(', ')
        if @nonpartition_keys.any?
          nonpartition_cql =
            @nonpartition_keys.map { |key| key.name }.join(', ')
          "PRIMARY KEY ((#{partition_cql}), #{nonpartition_cql})"
        else
          "PRIMARY KEY ((#{partition_cql}))"
        end
      end

      def properties_cql
        properties_fragments = @properties.map { |property| property.to_cql }
        properties_fragments << 'COMPACT STORAGE' if @compact_storage
        if @nonpartition_keys.any?
          clustering_fragment =
            @nonpartition_keys.zip(@clustering_order).
            map { |key, order| "#{key.name} #{order.upcase}" }.join(',')
          properties_fragments << "CLUSTERING ORDER BY (#{clustering_fragment})"
        end
        properties_fragments.join(' AND ') if properties_fragments.any?
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
