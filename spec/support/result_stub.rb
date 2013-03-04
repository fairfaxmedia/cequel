module Cequel

  module SpecSupport

    RowStub = Struct.new(:to_hash)

    class ResultStub
      include Enumerable

      attr_accessor :metadata

      def initialize(rows)
        @rows = rows
        @metadata = {:count => @rows.length }
                    .with_indifferent_access
      end

      def each(&block)
        row = @rows.shift
        yield RowStub.new(row) if row
      end

      def fetch_row
        return each
      end

      alias_method :fetch, :each
    end

  end

end
