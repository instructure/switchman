# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe Rails do
    include RSpecHelper

    it 'automaticallies isolate cache keys from different shards' do
      cache = ::ActiveSupport::Cache.lookup_store(:memory_store)
      allow(::Rails).to receive(:cache).and_return(cache)

      expect(@shard1.activate { ::Rails.cache }).to eq(@shard2.activate { ::Rails.cache })

      from1 = @shard1.activate { ::Rails.cache.fetch('key') { 1 } }
      expect(from1).to eq 1
      from2 = @shard2.activate do
        ::Rails.cache.fetch('key') { 2 }
      end
      expect(from2).to eq 2

      from1 = @shard1.activate { ::Rails.cache.fetch('key') }
      expect(from1).to eq 1
      from2 = @shard2.activate { ::Rails.cache.fetch('key') }
      expect(from2).to eq 2
    end

    it 'is not assignable' do
      expect { ::Rails.cache = :null_store }.to raise_exception(NoMethodError)
    end
  end
end
