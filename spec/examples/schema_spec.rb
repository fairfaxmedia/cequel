require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Schema do

  let(:table) { cequel.schema.read_table(:posts) }

  describe '#create_table' do

    after do
      cequel.schema.drop_table(:posts)
    end

    describe 'with simple skinny table' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create key alias' do
        table.partition_keys.map(&:name).should == [:permalink]
      end

      it 'should set key validator' do
        table.partition_keys.map(&:type).should == [Cequel::Type[:ascii]]
      end

      it 'should set non-key columns' do
        table.columns.find { |column| column.name == :title }.type.
          should == Cequel::Type[:text]
      end
    end

    describe 'with multi-column primary key' do
      before do
        cequel.schema.create_table(:posts) do
          key :blog_subdomain, :ascii
          key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create key alias' do
        column_family('posts')['key_aliases'].
          should == %w(blog_subdomain).to_json
      end

      it 'should set key validator' do
        column_family('posts')['key_validator'].
          should == 'org.apache.cassandra.db.marshal.AsciiType'
      end

      it 'should create non-partition key components' do
        column_family('posts')['column_aliases'].
          should == %w(permalink).to_json
      end

      it 'should set type for non-partition key components' do
        # This will be a composite consisting of the non-partition key types
        # followed by UTF-8 for the logical column name
        column_family('posts')['comparator'].should ==
          'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.AsciiType,org.apache.cassandra.db.marshal.UTF8Type)'
      end
    end

    describe 'with composite partition key' do
      before do
        cequel.schema.create_table(:posts) do
          partition_key :blog_subdomain, :ascii
          partition_key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create all partition key components' do
        column_family('posts')['key_aliases'].
          should == %w(blog_subdomain permalink).to_json
      end

      it 'should set key validators' do
        column_family('posts')['key_validator'].should ==
          'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.AsciiType,org.apache.cassandra.db.marshal.AsciiType)'
      end
    end

    describe 'with composite partition key and non-partition keys' do
      before do
        cequel.schema.create_table(:posts) do
          partition_key :blog_subdomain, :ascii
          partition_key :permalink, :ascii
          key :month, :timestamp
          column :title, :text
        end
      end

      it 'should create all partition key components' do
        column_family('posts')['key_aliases'].
          should == %w(blog_subdomain permalink).to_json
      end

      it 'should set key validators' do
        column_family('posts')['key_validator'].should ==
          'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.AsciiType,org.apache.cassandra.db.marshal.AsciiType)'
      end

      it 'should create non-partition key components' do
        column_family('posts')['column_aliases'].
          should == %w(month).to_json
      end

      it 'should set type for non-partition key components' do
        # This will be a composite consisting of the non-partition key types
        # followed by UTF-8 for the logical column name
        column_family('posts')['comparator'].should ==
          'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.DateType,org.apache.cassandra.db.marshal.UTF8Type)'
      end
    end

    describe 'collection types' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          list :authors, :blob
          set :tags, :text
          map :trackbacks, :timestamp, :ascii
        end
      end

      it 'should create list' do
        column('posts', 'authors')['validator'].should ==
          'org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.BytesType)'
      end

      it 'should create set' do
        column('posts', 'tags')['validator'].should ==
          'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)'
      end

      it 'should create map' do
        column('posts', 'trackbacks')['validator'].should ==
          'org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.DateType,org.apache.cassandra.db.marshal.AsciiType)'
      end
    end

    describe 'storage properties' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          with :comment, 'Blog posts'
          with :compression,
            :sstable_compression => "DeflateCompressor",
            :chunk_length_kb => 64
        end
      end

      it 'should set simple properties' do
        column_family('posts')['comment'].should == 'Blog posts'
      end

      it 'should set map collection properties' do
        column_family('posts')['compression_parameters'].should == {
          'sstable_compression' =>
            'org.apache.cassandra.io.compress.DeflateCompressor',
          'chunk_length_kb' => '64'
        }.to_json
      end
    end

    describe 'compact storage' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          compact_storage
        end
      end

      it 'should have compact storage' do
        # without compact storage, it'll be a single-element CompositeType
        column_family('posts')['comparator'].should ==
          'org.apache.cassandra.db.marshal.UTF8Type'
      end
    end

    describe 'clustering order' do
      before do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text
        end
      end

      it 'should set clustering order' do
        column_family('posts')['comparator'].should ==
          'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UUIDType),org.apache.cassandra.db.marshal.UTF8Type)'
      end
    end

    describe 'indices' do
      it 'should create indices' do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text, :index => true
        end
        column(:posts, :title)['index_name'].should == 'posts_title_idx'
      end

      it 'should create indices with specified name' do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text, :index => :silly_idx
        end
        column(:posts, :title)['index_name'].should == 'silly_idx'
      end
    end

  end

  def column_family(name)
    cequel.execute(<<-CQL, name).first.to_hash
      SELECT * FROM system.schema_columnfamilies
      WHERE keyspace_name = 'cequel_test' AND columnfamily_name = ?
    CQL
  end

  def column(column_family, column_name)
    cequel.execute(<<-CQL, column_family, column_name).first.to_hash
      SELECT * FROM system.schema_columns
      WHERE keyspace_name = 'cequel_test' AND columnfamily_name = ?
        AND column_name = ?
    CQL
  end

  def schema_query(query)
    result = cequel.execute(query)
    result.first.to_hash
  end

  def unpack_composite_column!(map, column, value)
    components = column.scan(/\x00.(.+?)\x00/m).map(&:first)
    last_component = components.pop
    components.each do |component|
      map[component] ||= {}
      map = map[component]
    end
    map[last_component] = value
  end

end
