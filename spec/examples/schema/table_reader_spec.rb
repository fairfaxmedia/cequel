require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableReader do

  after do
    cequel.schema.drop_table(:posts)
  end

  let(:table) { cequel.schema.read_table(:posts) }

  describe 'reading simple key' do
    before do
      cequel.execute("CREATE TABLE posts (permalink text PRIMARY KEY)")
    end

    it 'should read name correctly' do
      table.partition_keys.first.name.should == :permalink
    end

    it 'should read type correctly' do
      table.partition_keys.first.type.should be_a(Cequel::Type::Text)
    end

    it 'should have no nonpartition keys' do
      table.nonpartition_keys.should be_empty
    end
  end # describe 'reading simple key'

  describe 'reading single non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY (blog_subdomain, permalink)
        )
      CQL
    end

    it 'should read partition key name' do
      table.partition_keys.map(&:name).should == [:blog_subdomain]
    end

    it 'should read partition key type' do
      table.partition_keys.map(&:type).should == [Cequel::Type::Text.instance]
    end

    it 'should read non-partition key name' do
      table.nonpartition_keys.map(&:name).should == [:permalink]
    end

    it 'should read non-partition key type' do
      table.nonpartition_keys.map(&:type).
        should == [Cequel::Type::Ascii.instance]
    end

    it 'should default clustering order to asc' do
      table.nonpartition_keys.map(&:clustering_order).should == [:asc]
    end
  end # describe 'reading single non-partition key'

  describe 'reading reverse-ordered non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY (blog_subdomain, permalink)
        )
        WITH CLUSTERING ORDER BY (permalink DESC)
      CQL
    end

    it 'should read non-partition key name' do
      table.nonpartition_keys.map(&:name).should == [:permalink]
    end

    it 'should read non-partition key type' do
      table.nonpartition_keys.map(&:type).
        should == [Cequel::Type::Ascii.instance]
    end

    it 'should recognize reversed clustering order' do
      table.nonpartition_keys.map(&:clustering_order).should == [:desc]
    end
  end # describe 'reading reverse-ordered non-partition key'

  describe 'reading compound non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          author_id uuid,
          PRIMARY KEY (blog_subdomain, permalink, author_id)
        )
        WITH CLUSTERING ORDER BY (permalink DESC, author_id ASC)
      CQL
    end

    it 'should read non-partition key names' do
      table.nonpartition_keys.map(&:name).should == [:permalink, :author_id]
    end

    it 'should read non-partition key types' do
      table.nonpartition_keys.map(&:type).
        should == [Cequel::Type::Ascii.instance, Cequel::Type::Uuid.instance]
    end

    it 'should read heterogeneous clustering orders' do
      table.nonpartition_keys.map(&:clustering_order).should == [:desc, :asc]
    end
  end # describe 'reading compound non-partition key'

  describe 'reading compound partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY ((blog_subdomain, permalink))
        )
      CQL
    end

    it 'should read partition key names' do
      table.partition_keys.map(&:name).should == [:blog_subdomain, :permalink]
    end

    it 'should read partition key types' do
      table.partition_keys.map(&:type).
        should == [Cequel::Type::Text.instance, Cequel::Type::Ascii.instance]
    end

    it 'should have empty nonpartition keys' do
      table.nonpartition_keys.should be_empty
    end

  end # describe 'reading compound partition key'

  describe 'reading compound partition and non-partition keys' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          author_id uuid,
          published_at timestamp,
          PRIMARY KEY ((blog_subdomain, permalink), author_id, published_at)
        )
        WITH CLUSTERING ORDER BY (author_id ASC, published_at DESC)
      CQL
    end

    it 'should read partition key names' do
      table.partition_keys.map(&:name).should == [:blog_subdomain, :permalink]
    end

    it 'should read partition key types' do
      table.partition_keys.map(&:type).
        should == [Cequel::Type::Text.instance, Cequel::Type::Ascii.instance]
    end

    it 'should read non-partition key names' do
      table.nonpartition_keys.map(&:name).
        should == [:author_id, :published_at]
    end

    it 'should read non-partition key types' do
      table.nonpartition_keys.map(&:type).should ==
        [Cequel::Type::Uuid.instance, Cequel::Type::Timestamp.instance]
    end

    it 'should read clustering order' do
      table.nonpartition_keys.map(&:clustering_order).should == [:asc, :desc]
    end

  end # describe 'reading compound partition and non-partition keys'

  describe 'reading data columns' do

    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          title text,
          author_id uuid,
          PRIMARY KEY (blog_subdomain, permalink)
        )
      CQL
    end

    it 'should read names of data columns' do
      table.data_columns.map(&:name).should == [:author_id, :title]
    end

    it 'should read types of data columns' do
      table.data_columns.map(&:type).
        should == [Cequel::Type[:uuid], Cequel::Type[:text]]
    end

    it 'should read index attributes'

  end # describe 'reading data columns'

end
