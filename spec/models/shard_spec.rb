# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe Shard do
    include RSpecHelper

    describe '.activate' do
      it 'activates a hash of shard categories' do
        expect(Shard.current).to eq Shard.default
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
        Shard.activate(::ActiveRecord::Base => @shard1, MirrorUniverse => @shard2) do
          expect(Shard.current).to eq @shard1
          expect(Shard.current(MirrorUniverse)).to eq @shard2
        end
        expect(Shard.current).to eq Shard.default
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
      end

      it 'does not allow activating the unsharded category' do
        expect(Shard.current(UnshardedRecord)).to eq Shard.default
        Shard.activate(UnshardedRecord => @shard1) do
          expect(Shard.current(UnshardedRecord)).to eq Shard.default
        end
        expect(Shard.current(UnshardedRecord)).to eq Shard.default
      end
    end

    describe '.destroy' do
      it 'works on created shards' do
        server = DatabaseServer.create(Shard.default.database_server.config)
        shard = server.create_new_shard
        expect { shard.destroy }.not_to raise_error
        expect(Shard.where(id: shard.id)).to be_empty
      end

      it 'works on looked-up shards' do
        server = DatabaseServer.create(Shard.default.database_server.config)
        shard = server.create_new_shard
        expect { Shard.lookup(shard.id).destroy }.not_to raise_error
        expect(Shard.where(id: shard.id)).to be_empty
      end

      it 'fails on the default shard' do
        shard = Shard.default
        expect { shard.destroy }.to raise_error('Cannot destroy the default shard')
      end
    end

    describe '#activate' do
      it 'activates the default category when no args are used' do
        expect(Shard.current).to eq Shard.default
        @shard1.activate do
          expect(Shard.current).to eq @shard1
        end
        expect(Shard.current).to eq Shard.default
      end

      it 'activates other categories' do
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
        @shard1.activate(MirrorUniverse) do
          expect(Shard.current(MirrorUniverse)).to eq @shard1
          expect(Shard.current).to eq Shard.default
        end
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
      end

      it 'activates multiple categories' do
        expect(Shard.current).to eq Shard.default
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
        @shard1.activate(::ActiveRecord::Base, MirrorUniverse) do
          expect(Shard.current).to eq @shard1
          expect(Shard.current(MirrorUniverse)).to eq @shard1
        end
        expect(Shard.current).to eq Shard.default
        expect(Shard.current(MirrorUniverse)).to eq Shard.default
      end
    end

    describe '#shard' do
      it 'returns the default shard if the instance variable is not set' do
        # i.e. the instance var would not be set if we got this back from a cache
        # that was populated pre-sharding
        a = User.new
        expect(a.shard).to eq Shard.default
        a.instance_variable_set(:@shard, nil)
        expect(a.shard).to eq Shard.default
      end
    end

    describe '#drop_database' do
      it 'works' do
        # use a separate connection so we don't commit the transaction
        server = DatabaseServer.create(Shard.default.database_server.config)
        shard = server.create_new_shard
        shard.activate do
          User.create!
          expect(User.count).to eq 1
        end
        shard.drop_database
        shard.activate do
          expect { User.count }.to raise_error(::ActiveRecord::StatementInvalid)
        end
      end

      it 'raises an exception if the shard is the default shard' do
        expect { Shard.default.drop_database }.to raise_error('Cannot drop the database of the default shard')
      end
    end

    describe '.lookup' do
      it 'works with pseudo-ids' do
        expect(Shard.lookup('default')).to eq Shard.default
        expect(Shard.lookup('self')).to eq Shard.current
        @shard1.activate do
          expect(Shard.lookup('default')).to eq Shard.default
          expect(Shard.lookup('self')).to eq Shard.current
        end
      end

      it 'works with string ids' do
        expect(Shard.lookup(Shard.current.id.to_s)).to eq Shard.current
        expect(Shard.lookup(@shard1.id.to_s)).to eq @shard1
      end

      it 'raises an error for non-ids' do
        expect { Shard.lookup('jacob') }.to raise_error(ArgumentError)
      end

      it 'clears the in-process cache when a shard is destroyed' do
        s = Shard.create!(name: 'shard_to_destroy')
        expect(Shard.lookup(s.id)).to eq s
        s.destroy
        expect(Shard.lookup(s.id)).to be_nil
      end
    end

    describe '.preload_cache' do
      it 'works' do
        Shard.clear_cache
        expect(Shard).not_to receive(:find_by)
        Shard.preload_cache
        new_shard1 = Shard.lookup(@shard1.id)
        # same logic object, different instance
        expect(new_shard1).to eq @shard1
        expect(new_shard1).not_to equal @shard1
      end

      it 'preserves existing cached objects' do
        old_shard2 = nil

        @shard1.activate do
          Shard.clear_cache
          old_shard2 = Shard.lookup(@shard2.id)
          Shard.preload_cache
        end

        new_shard1 = Shard.lookup(@shard1.id)
        new_shard2 = Shard.lookup(@shard2.id)
        # exact same object, since it was the current shard we kept it cached
        expect(new_shard1).to equal @shard1
        # exact same object, since it was already in the cache
        expect(new_shard2).to equal old_shard2
      end
    end

    describe '.find_cached' do
      it "doesn't choke when it encounters columns it doesn't know about" do
        attrs = Shard.default.attributes
        # add an extra attribute
        attrs[:dummy_column] = 1
        shard_to_cache = double('shard', attributes: attrs)
        cached_default_shard = Shard.send(:find_cached, 'cache_key') { shard_to_cache }
        # logically equivalent, but a different instance
        expect(cached_default_shard).to eq Shard.default
        expect(cached_default_shard.object_id).not_to eq Shard.default.object_id
      end
    end

    describe '.with_each_shard' do
      describe ':exception' do
        it 'defaults to :raise' do
          expect { Shard.with_each_shard { raise 'error' } }.to raise_error('error')
        end

        it ':ignores' do
          expect(Shard.with_each_shard(exception: :ignore) { raise 'error' }).to eq []
        end

        it ':defers' do
          counter = 0
          expect do
            Shard.with_each_shard(exception: :defer) do
              counter += 1
              raise 'error'
            end
          end.to raise_error('error')
          # called more than once
          expect(counter).to be > 1
        end

        it 'calls a proc' do
          counter = 0
          expect(Shard.with_each_shard(exception: -> { counter += 1 }) { raise 'error' }).to eq []
          # called more than once
          expect(counter).to be > 1
        end
      end

      it 'orders explicit scopes without an explicit order' do
        scope = Shard.where(id: Shard.default)
        expect(scope).to receive(:order).once.and_return(scope)
        Shard.with_each_shard(scope) {}
      end

      it 'does not order explicit scopes that already have an order' do
        scope = Shard.order(:id)
        expect(scope).not_to receive(:order)
        Shard.with_each_shard(scope) {}
      end

      it "doesn't choke if no shards, and parallel" do
        Shard.with_each_shard(Shard.none, parallel: 2) {}
      end

      context 'without transaction' do
        self.use_transactional_tests = false

        it 'does not disconnect' do
          User.connection
          expect(User.connected?).to eq true
          Shard.with_each_shard([Shard.default, @shard2]) {}
          expect(User.connected?).to eq true
        end

        it "does not disconnect when it's the current shard" do
          User.connection
          expect(User.connected?).to eq true
          Shard.with_each_shard([Shard.default]) {}
          expect(User.connected?).to eq true
        end

        it 'does not disconnect for zero shards' do
          User.connection
          expect(User.connected?).to eq true
          Shard.with_each_shard([]) {}
          expect(User.connected?).to eq true
        end

        it "properly re-raises a PG exception that's not dumpable" do
          begin
            Shard.with_each_shard([Shard.default, @shard2], parallel: true) do
              next unless Shard.current == @shard2

              User.connection.execute('die')
            end
          rescue => e
            expect(e.message).to match(/die/)
            expect(e.current_shard).to eq @shard2
            raised = true
          end
          expect(raised).to eq true
        end

        it 'properly re-raises a SystemStackError' do
          begin
            Shard.with_each_shard([Shard.default, @shard2], parallel: true) do
              next unless Shard.current == @shard2

              x = nil
              x = -> { x.call }
              x.call
            end
          rescue SystemStackError => e
            expect(e.backtrace.length).to eq 51
            raised = true
          end
          expect(raised).to eq true
        end
      end

      it 'properly re-raises an autoloaded exception' do
        skip 'Rails 6 (zeitwerk) does not support dynamically changing the autoload path'

        expect(defined?(TestException)).to eq nil
        ::ActiveSupport::Dependencies.autoload_paths << File.expand_path(File.join(__FILE__, '../..'))
        begin
          Shard.with_each_shard([Shard.default, @shard2], parallel: true) do
            raise TestException
          end
        rescue => e
          expect(e.class).to eq TestException
          raised = true
        end
        expect(raised).to eq true
      end

      it "doesn't fork for parallel of 1, with one server" do
        pid = Process.pid
        Shard.with_each_shard([Shard.default, @shard1], parallel: 1) do
          expect(Process.pid).to eq pid
        end
      end

      it "doesn't fork for parallel of 1, with one server, from a Relation" do
        pid = Process.pid
        Shard.with_each_shard(Shard.where(id: Shard.default), parallel: 1) do
          expect(Process.pid).to eq pid
        end
      end

      it 'forks for parallel of 1, with multiple servers' do
        pid = Process.pid
        Shard.with_each_shard([Shard.default, @shard2], parallel: 1) do
          expect(Process.pid).not_to eq pid
        end
      end

      it 'forks for parallel of 1, with multiple servers, from a Relation' do
        pid = Process.pid
        Shard.with_each_shard(Shard.where(id: [Shard.default, @shard2]), parallel: 1) do
          expect(Process.pid).not_to eq pid
        end
      end
    end

    describe '.cached_shards' do
      it 'is a hash rather than array' do
        Shard.instance_variable_set(:@cached_shards, nil)
        expect(Shard.send(:cached_shards)).to be_a(Hash)
        Shard.clear_cache
        expect(Shard.send(:cached_shards)).to be_a(Hash)
      end
    end

    describe '.partition_by_shard' do
      it 'works' do
        ids = [2, 48, Shard::IDS_PER_SHARD * @shard1.id + 6, Shard::IDS_PER_SHARD * @shard1.id + 8, 10, 12]
        results = Shard.partition_by_shard(ids) do |partitioned_ids|
          expect(partitioned_ids.length == 4 || partitioned_ids.length == 2).to eq true
          partitioned_ids.map { |id| id + 1 }
        end

        # could have done either shard first, but we can't sort, because we want to see the shards grouped together
        expect([[3, 49, 11, 13, 7, 9], [7, 9, 3, 49, 11, 13]].include?(results)).to be true
      end

      it 'works for a partition_proc that returns a shard' do
        array = [{ id: 1, shard: @shard1 }, { id: 2, shard: @shard2 }]
        results = Shard.partition_by_shard(array, ->(a) { a[:shard] }) do |objects|
          expect(objects.length).to eq 1
          expect(Shard.current).to eq objects.first[:shard]
          objects.first[:id]
        end
        expect(results.sort).to eq [1, 2]
      end

      it 'supports shortened id syntax, and strings' do
        ids = [@shard1.global_id_for(1), "#{@shard2.id}~2"]
        result = Shard.partition_by_shard(ids) do |partitioned_ids|
          expect(partitioned_ids.length).to eq 1
          expect([@shard1, @shard2].include?(Shard.current)).to eq true
          expect(partitioned_ids.first).to eq 1 if Shard.current == @shard1
          expect(partitioned_ids.first).to eq 2 if Shard.current == @shard2
          partitioned_ids.first
        end
        expect(result.sort).to eq [1, 2]
      end

      it 'partitions unrecognized types unchanged into current shard' do
        expected_shard = Shard.current
        items = [:symbol, Object.new]
        result = Shard.partition_by_shard(items) do |shard_items|
          [Shard.current, shard_items]
        end
        expect(result).to eq [expected_shard, items]
      end

      it 'partitions unrecognized strings unchanged into current shard' do
        expected_shard = Shard.current
        items = ['not an id', 'something other than an id']
        result = Shard.partition_by_shard(items) do |shard_items|
          [Shard.current, shard_items]
        end
        expect(result).to eq [expected_shard, items]
      end

      it 'partitions recognized ids with an invalid shard unchanged into current shard' do
        expected_shard = Shard.current
        bad_shard_id = @shard2.id + 10_000
        items = ["#{bad_shard_id}~1", Shard::IDS_PER_SHARD * bad_shard_id + 1]
        result = Shard.partition_by_shard(items) do |shard_items|
          [Shard.current, shard_items]
        end
        expect(result).to eq [expected_shard, items]
      end
    end

    describe '#name' do
      # just to avoid Rails connecting to non-existent dbs as we temporarily create configs
      self.use_transactional_tests = false

      it 'the default shard should not be marked as dirty after reading its name' do
        s = Shard.default
        expect(s).not_to be_new_record
        s.name
        expect(s).not_to be_changed
      end

      it 'falls back to shard_name in the config if nil' do
        db = DatabaseServer.new('test', adapter: 'postgresql', database: 'canvas', shard_name: 'yoyoyo')
        shard = Shard.new(database_server: db)
        expect(shard.name).to eq 'yoyoyo'
      end

      it 'gets it from the postgres connection if not otherwise specified' do
        db = DatabaseServer.create(adapter: 'postgresql', database: 'notme')
        shard = Shard.new(database_server: db)
        shard.database_server = db
        allow(shard).to receive(:new_record?).and_return(false)
        connection = double(
          open_transactions: 0,
          shard: Shard.default,
          adapter_name: 'PostgreSQL',
          run_callbacks: nil,
          _run_checkin_callbacks: nil,
          owner: Thread.current,
          lock: Mutex.new
        )
        expect(connection).to receive(:current_schemas).once.and_return(%w[canvas public])
        expect(connection).to receive(:shard=).with(shard)
        allow_any_instance_of(::ActiveRecord::ConnectionAdapters::ConnectionPool).to receive(:checkout).and_return(connection)
        begin
          expect(shard.name).to eq 'canvas'
        ensure
          allow_any_instance_of(::ActiveRecord::ConnectionAdapters::ConnectionPool).to receive(:checkout).and_call_original
          shard.activate { ::ActiveRecord::Base.clear_active_connections! }
        end
      end
    end

    describe '.shard_for' do
      it 'works' do
        expect(Shard.shard_for(1)).to eq Shard.default
        expect(Shard.shard_for(1, @shard1)).to eq @shard1
        expect(Shard.shard_for(@shard1.global_id_for(1))).to eq @shard1
        expect(Shard.shard_for(Shard.default.global_id_for(1))).to eq Shard.default
        expect(Shard.shard_for(@shard1.global_id_for(1), @shard1)).to eq @shard1
        expect(Shard.shard_for(Shard.default.global_id_for(1), @shard1)).to eq Shard.default
      end

      it 'works for non-integeral primary key AR objects' do
        user = @shard1.activate { User.new }
        allow(user).to receive(:id).and_return('abc')
        expect(user.id).to eq 'abc'
        expect(user.shard).to eq @shard1
        expect(Shard.shard_for(user)).to eq @shard1
      end
    end

    describe '.local_id_for' do
      it 'recognizes shortened string ids' do
        expected_id = 1
        expected_shard = @shard2
        id, shard = Shard.local_id_for("#{expected_shard.id}~#{expected_id}")
        expect(id).to eq expected_id
        expect(shard).to eq expected_shard
      end

      it 'recognizes global ids' do
        expected_id = 1
        expected_shard = @shard2
        id, shard = Shard.local_id_for(Shard::IDS_PER_SHARD * expected_shard.id + expected_id)
        expect(id).to eq expected_id
        expect(shard).to eq expected_shard
      end

      it 'recognizes local ids with no shard' do
        expected_id = 1
        id, shard = Shard.local_id_for(expected_id)
        expect(id).to eq expected_id
        expect(shard).to be_nil
      end

      it 'returns nil for unrecognized input' do
        id, shard = Shard.local_id_for('not an id')
        expect(id).to be_nil
        expect(shard).to be_nil
      end

      it 'returns nil for ids with bad shard values' do
        bad_shard_id = @shard2.id + 10_000
        id, shard = Shard.local_id_for("#{bad_shard_id}~1")
        expect(id).to be_nil
        expect(shard).to be_nil
      end
    end

    context 'with id translation' do
      before do
        @local_id = 1
        @global_id = Shard::IDS_PER_SHARD * @shard1.id + @local_id
      end

      describe '.integral_id' do
        it 'returns recognized ids' do
          expect(Shard.integral_id_for(@local_id)).to eq @local_id
          expect(Shard.integral_id_for(@local_id.to_s)).to eq @local_id
          expect(Shard.integral_id_for(@global_id)).to eq @global_id
          expect(Shard.integral_id_for(@global_id.to_s)).to eq @global_id
          expect(Shard.integral_id_for("#{@shard1.id}~#{@local_id}")).to eq @global_id
          expect(Shard.integral_id_for("-#{@local_id}")).to eq(-1 * @local_id)
          expect(Shard.integral_id_for("-#{@global_id}")).to eq(-1 * @global_id)
          expect(Shard.integral_id_for("#{@shard1.id}~-#{@local_id}")).to eq(@global_id * -1)
        end

        it "works even for shards that don't exist" do
          shard = Shard.create!(name: 'unique')
          shard.destroy
          global_id = shard.global_id_for(1)
          expect(Shard.integral_id_for(global_id)).to eq global_id
          expect(Shard.integral_id_for(global_id.to_s)).to eq global_id
          expect(Shard.integral_id_for("#{shard.id}~1")).to eq global_id
        end

        it 'returns nil for unrecognized ids' do
          expect(Shard.integral_id_for('not an id')).to eq nil
        end
      end

      describe '.local_id_for' do
        it 'returns id without shard for local id' do
          expect(Shard.local_id_for(@local_id)).to eq [@local_id, nil]
        end

        it 'returns id with shard for global id' do
          expect(Shard.local_id_for(@global_id)).to eq [@local_id, @shard1]
        end

        it "returns nil for shards that don't exist" do
          shard = Shard.create!(name: 'unique')
          shard.destroy
          expect(Shard.local_id_for(shard.global_id_for(1))).to eq [nil, nil]
        end

        it 'returns nil for unrecognized ids' do
          expect(Shard.local_id_for('not an id')).to eq [nil, nil]
        end

        it 'handles negative IDs' do
          negative_local_id = @local_id * -1
          negative_global_id = @global_id * -1
          expect(Shard.local_id_for(negative_local_id)).to eq [negative_local_id, nil]
          expect(Shard.local_id_for(negative_global_id)).to eq [negative_local_id, @shard1]
        end
      end

      describe '.relative_id_for' do
        it 'returns recognized ids relative to the target shard' do
          expect(Shard.relative_id_for(@local_id, @shard1, @shard2)).to eq @global_id
          expect(Shard.relative_id_for(@local_id, @shard2, @shard2)).to eq @local_id
          expect(Shard.relative_id_for(@global_id, @shard1, @shard2)).to eq @global_id
          expect(Shard.relative_id_for(@global_id, @shard2, @shard2)).to eq @global_id
        end

        it 'processes negative ids' do
          negative_local_id = @local_id * -1
          negative_global_id = @global_id * -1
          expect(Shard.relative_id_for(negative_local_id, @shard1, @shard2)).to eq negative_global_id
          expect(Shard.relative_id_for(negative_local_id, @shard2, @shard2)).to eq negative_local_id
          expect(Shard.relative_id_for(negative_global_id, @shard1, @shard2)).to eq negative_global_id
          expect(Shard.relative_id_for(negative_global_id, @shard2, @shard2)).to eq negative_global_id
        end

        it 'returns the nil for unrecognized ids' do
          expect(Shard.relative_id_for('not an id', @shard1, @shard2)).to eq nil
        end

        it 'returns an integral form of an id when it refers to a non-existent shard' do
          expect(Shard.relative_id_for("#{@shard2.id + 1}~1", @shard1,
                                       @shard2)).to eq Shard.new(id: @shard2.id + 1).global_id_for(1)
        end
      end

      describe '.short_id_for' do
        it 'returns shorted strings for global ids' do
          expect(Shard.short_id_for(@local_id)).to eq @local_id
          expect(Shard.short_id_for(@local_id.to_s)).to eq @local_id
          expect(Shard.short_id_for(@global_id)).to eq "#{@shard1.id}~#{@local_id}"
        end

        it 'returns the original id for unrecognized ids' do
          expect(Shard.short_id_for('not an id')).to eq 'not an id'
        end

        it 'maintains sign of input' do
          negative_local_id = @local_id * -1
          negative_global_id = @global_id * -1
          expect(Shard.short_id_for(negative_local_id)).to eq negative_local_id
          expect(Shard.short_id_for(negative_local_id.to_s)).to eq negative_local_id
          expect(Shard.short_id_for(negative_global_id)).to eq "#{@shard1.id}~#{negative_local_id}"
        end
      end

      describe '.global_id_for' do
        it 'returns the provided id if already global' do
          local_id = 5
          Shard.with_each_shard do
            global_id = Shard.current.global_id_for(local_id)
            expect(Shard.global_id_for(global_id)).to eq global_id
          end
        end

        it 'treats local ids as local to the current shard' do
          local_id = 5
          Shard.with_each_shard do
            next if Shard.current == Shard.default

            expect(Shard.shard_for(Shard.global_id_for(local_id))).to eq Shard.current
          end
        end

        it 'globalizes with sign intact' do
          local_id = -5
          global_id = (Shard::IDS_PER_SHARD * @shard1.id * -1) + local_id
          expect(Shard.global_id_for(local_id, @shard1)).to eq global_id
          expect(Shard.global_id_for(global_id, @shard1)).to eq global_id
        end
      end
    end

    describe '.default' do
      after do
        allow(Shard).to receive(:where).and_call_original
        Shard.default(reload: true)
      end

      it 'returns the cached value if default is already set' do
        Shard.instance_variable_set(:@default, DefaultShard.instance)
        expect(Shard.default).to eq(DefaultShard.instance)
      end

      it 'loads a default value if cached value is nil' do
        Shard.instance_variable_set(:@default, nil)
        expect(Shard.default).to be_a(Switchman::Shard)
      end

      it "reloads the default shard even when it's set when the reload arg present" do
        Shard.instance_variable_set(:@default, DefaultShard.instance)
        expect(Shard.default(reload: true)).to be_a(Switchman::Shard)
      end

      context 'when using reload with_fallback' do
        it 'replaces DefaultShard instance if cached' do
          Shard.instance_variable_set(:@default, DefaultShard.instance)
          expect(Shard.default(reload: true, with_fallback: true)).to be_a(Switchman::Shard)
        end

        it 'replaces a Shard instance if replacement query successful' do
          non_default = Shard.where(default: false).first
          actual_default = Shard.where(default: true).first
          expect(non_default).not_to be_nil
          expect(actual_default).not_to be_nil
          Shard.instance_variable_set(:@default, non_default)
          allow(Shard).to receive(:where).with(default: true).and_return(double(take: actual_default))
          new_default = Shard.default(reload: true, with_fallback: true)
          expect(new_default).to eq(actual_default)
        end

        it 'uses the default shard instance when fallback is off' do
          non_default = Shard.where(default: false).first
          Shard.instance_variable_set(:@default, non_default)
          Switchman.cache.clear
          allow(Shard).to receive(:where).with(default: true).and_raise(PG::UnableToSend)
          new_default = Shard.default(reload: true, with_fallback: false)
          expect(new_default).to eq(DefaultShard.instance)
        end

        it 'falls back to existing default shard if replacement query fails' do
          non_default = Shard.where(default: false).first
          Shard.instance_variable_set(:@default, non_default)
          Switchman.cache.clear
          allow(Shard).to receive(:where).with(default: true).and_raise(PG::UnableToSend)
          new_default = Shard.default(reload: true, with_fallback: true)
          expect(new_default).to eq(non_default)
        end

        it 'respects a false reload even with fallback' do
          Shard.instance_variable_set(:@default, DefaultShard.instance)
          expect(Shard.default(reload: false, with_fallback: true)).to eq(DefaultShard.instance)
        end
      end

      it "doesn't forget current shard activations when reloading" do
        @shard1.activate do
          Shard.default(reload: true)
          expect(Shard.current).to eq @shard1
        end
      end
    end

    describe '.determine_max_procs' do
      context 'with no info on cpu_count' do
        before do
          allow(::Switchman::Environment).to receive(:cpu_count).and_return(nil)
        end

        it 'returns the option if valid' do
          expect(Shard.determine_max_procs(5)).to eq(5)
        end

        it 'makes the option an integer' do
          expect(Shard.determine_max_procs('8')).to eq(8)
        end

        it 'returns nil if the option is nil' do
          expect(Shard.determine_max_procs(nil)).to be_nil
        end

        it 'is nil if the option is 0' do
          expect(Shard.determine_max_procs('0')).to be_nil
        end

        it 'is nil if the option is nonsense' do
          expect(Shard.determine_max_procs('asdf')).to be_nil
        end

        it 'ignores parallel input if max procs specified' do
          expect(Shard.determine_max_procs('4', 3)).to eq(4)
        end
      end

      context 'with a cpu_count' do
        before do
          allow(::Switchman::Environment).to receive(:cpu_count).and_return(8)
        end

        it 'returns the option if valid' do
          expect(Shard.determine_max_procs('4')).to eq(4)
        end

        it 'returns 2x cores if the option is nil and no parallel input' do
          expect(Shard.determine_max_procs(nil)).to eq(16)
        end

        it 'uses the parallel input as a multiplier if provided' do
          expect(Shard.determine_max_procs(nil, 3)).to eq(24)
        end

        it 'maxes at 1 if parallel is 0' do
          expect(Shard.determine_max_procs(nil, 0)).to eq(1)
        end

        it 'maxes at 1 if parallel is nil' do
          expect(Shard.determine_max_procs(nil, nil)).to eq(1)
        end

        it 'is nil if the option is 0' do
          expect(Shard.determine_max_procs('0')).to eq(nil)
        end

        it 'is nil if the cpu count is 0' do
          allow(::Switchman::Environment).to receive(:cpu_count).and_return(0)
          expect(Shard.determine_max_procs(nil)).to eq(nil)
        end
      end
    end
  end

  describe 'Failed shard creation' do
    include RSpecHelper

    it 'ends in a consistent state on default shard database server' do
      ::ActiveRecord::Base.transaction do
        Shard.default.database_server.create_new_shard(name: 'bad_shard')
        raise ::ActiveRecord::Rollback
      end

      # Simulate activesupport connection teardown
      ::ActiveRecord::Base.connection_pool.disable_query_cache!
    end

    it 'ends in a consistent state on non-default shard database server' do
      ::ActiveRecord::Base.transaction do
        Shard.third.database_server.create_new_shard(name: 'bad_shard')
        raise ::ActiveRecord::Rollback
      end

      # Simulate activesupport connection teardown
      ::ActiveRecord::Base.connection_pool.disable_query_cache!
    end
  end
end
