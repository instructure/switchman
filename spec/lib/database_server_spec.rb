# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe DatabaseServer do
    describe '#create_new_shard' do
      include RSpecHelper

      def maybe_activate(shard, &block)
        shard&.activate(&block)
        yield unless shard
      end

      def create_shard(server)
        new_shard = server.create_new_shard
        expect(new_shard).not_to be_new_record
        expect(new_shard.name).to match(/shard_\d+/)
        # They should share a connection pool
        if server == Shard.default.database_server
          expect(User.connection_pool).to eq(new_shard.activate { User.connection_pool })
          expect(User.connection_pool).to eq Shard.connection_pool
        else
          expect(User.connection_pool).not_to eq(new_shard.activate { User.connection_pool })
        end
        # The tables should be created, ready to use
        new_shard.activate do
          a = User.create!
          expect(a).not_to be_new_record
        end
      ensure
        if new_shard
          new_shard.drop_database
          new_shard.destroy
        end
      end

      let(:exception_class) { Class.new(Exception) }

      it "is able to create a new shard from a db server that doesn't have any shards" do
        # So, it's really the same server, but we want separate connections
        db = DatabaseServer.create(Shard.default.database_server.config)
        begin
          create_shard(db)
        ensure
          db.destroy
        end
      end

      it 'does not use a temp name' do
        db = DatabaseServer.create(adapter: 'postgresql')
        expect(Shard).to receive(:create!) do |hash|
          expect(hash[:name]).to eq 'new_shard'
          expect(hash[:database_server_id]).to eq db.id
          expect(hash[:id]).not_to be_nil
          raise exception_class
        end
        expect { db.create_new_shard(name: 'new_shard') }.to raise_error(exception_class)
      end
    end

    describe '#config' do
      it 'returns subenvs' do
        base_config = { database: 'db',
                        secondary: [nil, { database: 'secondary' }],
                        deploy: { username: 'deploy' } }
        ds = DatabaseServer.new(nil, base_config)
        expect(ds.config).to eq base_config
        expect(ds.config(:secondary)).to eq [{ database: 'db', deploy: base_config[:deploy] },
                                             { database: 'secondary', deploy: base_config[:deploy] }]
        expect(ds.config(:deploy)).to eq({ database: 'db', username: 'deploy', secondary: base_config[:secondary],
                                           deploy: base_config[:deploy] })
      end
    end

    describe '#guard_rail_environment' do
      it 'inherits from GuardRail.environment' do
        ds = DatabaseServer.new
        expect(ds.guard_rail_environment).to eq :primary
        ::GuardRail.activate(:secondary) do
          expect(ds.guard_rail_environment).to eq :secondary
        end
      end

      it 'overrides GuardRail.environment when explicitly set' do
        ds = DatabaseServer.new
        ds.guard!
        expect(ds.guard_rail_environment).to eq :secondary
        ds.unguard do
          expect(ds.guard_rail_environment).to eq :primary
        end
        expect(ds.guard_rail_environment).to eq :secondary
        ::GuardRail.activate(:secondary) do
          expect(ds.guard_rail_environment).to eq :secondary
          ds.unguard do
            expect(ds.guard_rail_environment).to eq :secondary
          end
          expect(ds.guard_rail_environment).to eq :secondary
        end
        expect(ds.guard_rail_environment).to eq :secondary
      end
    end

    describe '#cache_store' do
      before do
        @db = DatabaseServer.new
        @default_store = ::ActiveSupport::Cache.lookup_store(:null_store)
        @db_store = ::ActiveSupport::Cache.lookup_store(:memory_store)
        @original_map = Switchman.config[:cache_map]
        Switchman.config[:cache_map] = { ::Rails.env => @default_store, @db.id => @db_store }
      end

      after do
        Switchman.config[:cache_map] = @original_map
      end

      it 'prefers the cache specific to the database' do
        expect(@db.cache_store.object_id).to eq @db_store.object_id
      end

      it 'fallbacks to Rails.cache_without_sharding if no specific cache' do
        Switchman.config[:cache_map].delete(@db.id)
        expect(@db.cache_store.object_id).to eq @default_store.object_id
      end
    end

    describe '.server_for_new_shard' do
      let(:db1) { DatabaseServer.new('1', adapter: 'postgresql') }
      let(:db2) { DatabaseServer.new('2', open: true, adapter: 'postgresql') }

      it "returns the default server if that's the only one around" do
        allow(DatabaseServer).to receive(:database_servers).and_return({ '1': db1 })
        allow(DatabaseServer).to receive(:find).with(nil).and_return(db1)
        expect(DatabaseServer.server_for_new_shard).to eq db1
      end

      it 'returns on open server' do
        allow(DatabaseServer).to receive(:database_servers).and_return({ '1': db1 })
        db1.config[:open] = true
        expect(DatabaseServer.server_for_new_shard).to eq db1
      end

      it "returns another server if it's the only one open" do
        allow(DatabaseServer).to receive(:database_servers).and_return({ '1': db1, '2': db2 })
        4.times { expect(DatabaseServer.server_for_new_shard).to eq db2 }
        db2.config.delete(:open)
        db1.config[:open] = true
        4.times { expect(DatabaseServer.server_for_new_shard).to eq db1 }
      end

      it 'returns multiple open servers' do
        allow(DatabaseServer).to receive(:database_servers).and_return({ '1': db1, '2': db2 })
        db1.config[:open] = true
        dbs = []
        20.times do
          dbs << DatabaseServer.server_for_new_shard
        end
        expect(dbs).to include(db1)
        expect(dbs).to include(db2)
      end
    end

    describe '#primary_shard' do
      it 'works even without a shards table' do
        expect(Shard.default).to be_a(DefaultShard)
        expect(Shard.default.database_server).not_to receive(:shards)
        expect(Shard.default.database_server.primary_shard).to eq Shard.default
      end
    end
  end
end
