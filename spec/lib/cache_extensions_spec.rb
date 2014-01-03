require "spec_helper"

module Switchman
  describe CacheExtensions do
    it "should automatically isolate cache keys from different shards" do
      cache = ActiveSupport::Cache::MemoryStore.new
      Rails.stubs(:cache_without_sharding).returns(cache)
      db = DatabaseServer.create(:settings => { :adapter => 'sqlite3' })
      s1 = db.shards.create!
      s2 = db.shards.create!

      s1.activate { Rails.cache }.should == s2.activate { Rails.cache }

      from_1 = s1.activate { Rails.cache.fetch('key') { 1 } }
      from_1.should == 1
      from_2 = s2.activate do
        Rails.cache.fetch('key') { 2 }
      end
      from_2.should == 2

      from_1 = s1.activate { Rails.cache.fetch('key') }
      from_1.should == 1
      from_2 = s2.activate { Rails.cache.fetch('key') }
      from_2.should == 2
    end

    it "should not be assignable" do
      expect{ Rails.cache = :null_store }.to raise_exception(NoMethodError)
    end
  end
end
