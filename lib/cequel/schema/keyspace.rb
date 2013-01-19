module Cequel

  module Schema

    class Keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def create_table(name, &block)
        table = Table.new(name)
        TableDSL.apply(table, &block)
        table.create_cql.each do |statement|
          @keyspace.execute(statement)
        end
      end

      def drop_table(name)
        @keyspace.execute("DROP TABLE #{name}")
      end

    end

  end

end
