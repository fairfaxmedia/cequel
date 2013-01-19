module Cequel

  module Schema

    class Column

      attr_reader :name, :type, :index_name

      def initialize(name, type, index_name = nil)
        @name, @type, @index_name = name, type, index_name
      end

      def indexed?
        !!@index_name
      end

      def to_cql
        "#{@name} #{@type}"
      end

    end

    class List < Column

      def to_cql
        "#{@name} LIST <#{@type}>"
      end

    end

    class Set < Column

      def to_cql
        "#{@name} SET <#{@type}>"
      end

    end

    class Map < Column

      attr_reader :key_type
      alias_method :value_type, :type

      def initialize(name, key_type, value_type)
        super(name, value_type)
        @key_type = key_type
      end

      def to_cql
        "#{@name} MAP <#{@key_type}, #{@type}>"
      end

    end

  end

end
