require 'stringio'
require 'cequel/table_column'

module Cequel

  class TableDefinition

    attr_reader :name

    def initialize(name)
      @name = name
      @keys = []
      @columns = []
    end

    def add_key(name, type)
      @keys << TableColumn.new(name, type)
    end

    def add_column(name, type)
      @columns << TableColumn.new(name, type)
    end

    def create_cql
      "CREATE TABLE #{@name} (#{key_columns_cql}, #{columns_cql}, #{keys_cql})"
    end

    private

    def columns_cql
      @columns.map { |key| "#{key.name} #{key.type}" }.join(', ')
    end

    def key_columns_cql
      @keys.map { |key| "#{key.name} #{key.type}" }.join(', ')
    end

    def keys_cql
      "PRIMARY KEY (#{@keys.map { |key| key.name }.join(', ')})"
    end

  end

end
