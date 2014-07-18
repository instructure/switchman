require "spec_helper"

module Switchman
  describe Rails do
    it "should automatically isolate cache keys from different shards" do
      cache = ::ActiveSupport::Cache::MemoryStore.new
      ::Rails.stubs(:cache).returns(cache)
      db = DatabaseServer.create(:settings => { :adapter => 'sqlite3' })
      s1 = db.shards.create!
      s2 = db.shards.create!

      expect(s1.activate { ::Rails.cache }).to eq s2.activate { ::Rails.cache }

      from_1 = s1.activate { ::Rails.cache.fetch('key') { 1 } }
      expect(from_1).to eq 1
      from_2 = s2.activate do
        ::Rails.cache.fetch('key') { 2 }
      end
      expect(from_2).to eq 2

      from_1 = s1.activate { ::Rails.cache.fetch('key') }
      expect(from_1).to eq 1
      from_2 = s2.activate { ::Rails.cache.fetch('key') }
      expect(from_2).to eq 2
    end

    it "should not be assignable" do
      expect{ ::Rails.cache = :null_store }.to raise_exception(NoMethodError)
    end
  end
end
