module Cequel

  module SpecSupport

    Connection = Object.new

    module Helpers

      def result_stub(*results)
        ResultStub.new(results)
      end

      def count_result_stub(count)
        result = ResultStub.new([])
        result.metadata = {:count => count}.with_indifferent_access
        result
      end

      def connection
        Connection
      end

      def cequel
        @cequel ||= Cequel::Keyspace.new({}).tap do |keyspace|
          keyspace.connection = connection
        end
      end
    end

  end

end
