# frozen_string_literal: true

require "spec_helper"

module Switchman
  describe Rails do
    it "should automatically isolate cache keys from different shards" do
      cache = ::ActiveSupport::Cache.lookup_store(:memory_store)
      allow(::Rails).to receive(:cache).and_return(cache)
      db = DatabaseServer.create(adapter: 'sqlite3')
      s1 = db.shards.create!(name: 'shard1')
      s2 = db.shards.create!(name: 'shard2')

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
