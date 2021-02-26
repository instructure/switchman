# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe Engine do
    describe '.lookup_stores' do
      it 'links strings to other entries' do
        stores = Engine.lookup_stores('db1' => :memory_store, 'db2' => :memory_store, 'db3' => 'db1')
        expect(stores['db1']).to be_a(::ActiveSupport::Cache::MemoryStore)
        expect(stores['db1']).to equal stores['db3']
        expect(stores['db1']).not_to equal stores['db2']
      end
    end
  end
end
