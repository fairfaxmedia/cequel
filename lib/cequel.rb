require 'active_support/core_ext'
require 'cql'
require 'simple_uuid'
require 'connection_pool'

require 'cequel/batch'
require 'cequel/errors'
require 'cequel/cql_row_specification'
require 'cequel/data_set'
require 'cequel/keyspace'
require 'cequel/row_specification'
require 'cequel/statement'

module Cequel
  def self.connect(configuration = nil)
    Keyspace.new(configuration || {})
  end
end
