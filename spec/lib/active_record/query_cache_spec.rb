require "spec_helper"

module Switchman
  module ActiveRecord
    describe QueryCache do
      include RSpecHelper

      if ::Rails.version >= '5.0.1'
        after do
          ::ActiveRecord::Base.connection_pool.disable_query_cache!
        end

        it "should isolate queries to multiple shards on the same server" do
          expect(::ActiveRecord::Base.connection_pool.query_cache_enabled).to eq false
          ::ActiveRecord::Base.connection_pool.enable_query_cache!

          @shard1.activate do
            expect(User.connection.query_cache_enabled).to eq true
            User.create!
            User.create!
          end
          @shard3.activate do
            expect(User.connection.query_cache_enabled).to eq true
            User.create!
          end
          expect(@shard1.activate { User.all.to_a }).not_to eq @shard3.activate { User.all.to_a }
          @shard1.activate { User.connection.expects(:select).never }
          expect(@shard1.activate { User.all.to_a }).not_to eq @shard3.activate { User.all.to_a }
        end

        it "doesn't break logging with binds" do
          ::Rails.logger.expects(:error).never
          User.connection.cache do
            User.where(id: 1).take
            User.where(id: 1).take
          end
        end
      else
        # call with two blocks. the first will run on a new thread, pausing at
        # the site of a "cc.call". the second will run on the original thread
        # while the first is paused. the first will then finish.
        def threaded(blk1, blk2)
          mutex = Mutex.new
          ready1 = ConditionVariable.new
          ready2 = ConditionVariable.new

          cc = lambda do
            mutex.synchronize do
              ready1.signal
              ready2.wait(mutex)
            end
          end

          thread = Thread.new do
            blk1.call(cc)
            ::ActiveRecord::Base.connection_pool.release_connection
          end

          mutex.synchronize do
            ready1.wait(mutex)
            blk2.call
            ready2.signal
          end

          thread.join
        end

        before do
          User.connection.clear_query_cache
          @orig_enabled = User.connection.query_cache_enabled
          @orig_cache = User.connection.query_cache.dup
        end

        after do
          User.connection.query_cache_enabled = @enabled
          User.connection.instance_variable_set(:@query_cache, @orig_cache)
        end

        it "should isolate queries to multiple shards on the same server" do
          @shard1.activate do
            User.create!
            User.create!
          end
          @shard3.activate do
            User.create!
          end
          expect(@shard1.activate { User.all.to_a }).not_to eq @shard3.activate { User.all.to_a }
        end

        describe "query_cache_enabled" do
          it "should be shared across shards on the same server" do
            @shard1.activate{ User.connection.query_cache_enabled = true }
            @shard3.activate{ User.connection.query_cache_enabled = false }
            @shard1.activate{ expect(User.connection.query_cache_enabled).to eq false }
          end

          it "should be shared across servers" do
            @shard1.activate{ User.connection.query_cache_enabled = true }
            @shard2.activate{ User.connection.query_cache_enabled = false }
            @shard1.activate{ expect(User.connection.query_cache_enabled).to eq false }
          end

          it "should be distinct across threads" do
            User.connection.query_cache_enabled = true
            threaded(
              lambda{ |cc| User.connection.query_cache_enabled = false; cc.call },
              lambda{ expect(User.connection.query_cache_enabled).to eq true })
          end
        end

        describe "enable_query_cache!" do
          it "should only enable for the current thread" do
            User.connection.query_cache_enabled = false
            threaded(
              lambda{ |cc|
                User.connection.query_cache_enabled = false
                User.connection.enable_query_cache!
                expect(User.connection.query_cache_enabled).to eq true
                cc.call
              },
              lambda{ expect(User.connection.query_cache_enabled).to eq false })
          end
        end

        describe "disable_query_cache!" do
          it "should only enable for the current thread" do
            User.connection.query_cache_enabled = true
            threaded(
              lambda{ |cc|
                User.connection.query_cache_enabled = true
                User.connection.disable_query_cache!
                expect(User.connection.query_cache_enabled).to eq false
                cc.call
              },
              lambda{ expect(User.connection.query_cache_enabled).to eq true })
          end
        end

        describe "cache" do
          it "should only enable for the current thread" do
            # check that query_cache_enabled stays false on this thread while
            # another thread is in a cache{} block
            User.connection.disable_query_cache!
            threaded(
              lambda{ |cc| User.connection.cache{ cc.call } },
              lambda{ expect(User.connection.query_cache_enabled).to eq false })
          end

          it "should only enable for the duration of the block" do
            User.connection.disable_query_cache!
            User.connection.cache do
              expect(User.connection.query_cache_enabled).to eq true
            end
            expect(User.connection.query_cache_enabled).to eq false
          end

          it "should clear query cache if disabling query cache after block" do
            User.connection.disable_query_cache!
            User.connection.cache do
              User.connection.query_cache[:key][:binds] = :value
            end
            expect(User.connection.query_cache[:key][:binds]).to be_nil
          end

          it "should not clear query cache if the cache was already enabled" do
            User.connection.enable_query_cache!
            User.connection.cache do
              User.connection.query_cache[:key][:binds] = :value
            end
            expect(User.connection.query_cache[:key][:binds]).to eq :value
          end
        end

        describe "uncached" do
          it "should only disable for the current thread" do
            # check that query_cache_enabled stays true on this thread while
            # another thread is in an uncached{} block
            User.connection.enable_query_cache!
            threaded(
              lambda{ |cc| User.connection.uncached{ cc.call } },
              lambda{ expect(User.connection.query_cache_enabled).to eq true })
          end

          it "should only disable for the duration of the block" do
            User.connection.enable_query_cache!
            User.connection.uncached do
              expect(User.connection.query_cache_enabled).to eq false
            end
            expect(User.connection.query_cache_enabled).to eq true
          end

          it "should not clear query cache" do
            User.connection.query_cache[:key][:binds] = :value
            User.connection.uncached{}
            expect(User.connection.query_cache[:key][:binds]).to eq :value
          end
        end

        describe "select_all" do
          it "should cache when query cache enabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            expect(User.connection.query_cache).not_to be_empty
          end

          it "should not cache when query cache disabled" do
            User.connection.disable_query_cache!
            User.all.to_a
            expect(User.connection.query_cache).to be_empty
          end

          it "should not cache when query cache disabled but other thread's enabled" do
            User.connection.disable_query_cache!
            threaded(
              lambda{ |cc| User.connection.cache{ cc.call } },
              lambda{ User.all.to_a; expect(User.connection.query_cache).to be_empty })
          end

          it "should not cache when query is locked" do
            User.connection.enable_query_cache!
            User.lock.to_a
            expect(User.connection.query_cache).to be_empty
          end
        end

        describe "insert" do
          it "should clear thread's query cache if enabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.create!
            expect(User.connection.query_cache).to be_empty
          end

          it "should not clear thread's query cache if disabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.connection.disable_query_cache!
            User.create!
            expect(User.connection.query_cache).not_to be_empty
          end

          it "should not clear thread's query cache if disabled but other thread's enabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.connection.disable_query_cache!
            threaded(
              lambda{ |cc| User.create!; cc.call },
              lambda{ expect(User.connection.query_cache).not_to be_empty })
          end

          it "should not clear other thread's query cache" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.connection.disable_query_cache!
            threaded(
              lambda{ |cc| User.create!; cc.call },
              lambda{ expect(User.connection.query_cache).not_to be_empty })
          end
        end

        describe "update" do
          before do
            User.create!
          end

          it "should clear thread's query cache if enabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.update_all(updated_at: Time.now)
            expect(User.connection.query_cache).to be_empty
          end

          it "should not clear thread's query cache if disabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.connection.disable_query_cache!
            User.update_all(updated_at: Time.now)
            expect(User.connection.query_cache).not_to be_empty
          end

          context "non-transactional" do
            unless ::ActiveRecord::Base.connection_pool.spec.config[:adapter_name] == 'PostgreSQL'
              self.use_transactional_fixtures = false

              after do
                User.delete_all
              end
            end

            it "should not clear thread's query cache if disabled but other thread's enabled" do
              User.connection.enable_query_cache!
              User.all.to_a
              User.connection.disable_query_cache!
              threaded(
                lambda{ |cc| User.update_all(updated_at: Time.now); cc.call },
                lambda{ expect(User.connection.query_cache).not_to be_empty })
            end

            it "should not clear other thread's query cache" do
              User.connection.enable_query_cache!
              User.all.to_a
              User.connection.disable_query_cache!
              threaded(
                lambda{ |cc| User.update_all(updated_at: Time.now); cc.call },
                lambda{ expect(User.connection.query_cache).not_to be_empty })
            end
          end
        end

        describe "delete" do
          before do
            User.create!
          end

          it "should clear thread's query cache if enabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.delete_all
            expect(User.connection.query_cache).to be_empty
          end

          it "should not clear thread's query cache if disabled" do
            User.connection.enable_query_cache!
            User.all.to_a
            User.connection.disable_query_cache!
            User.delete_all
            expect(User.connection.query_cache).not_to be_empty
          end

          context "non-transactional" do
            unless ::ActiveRecord::Base.connection_pool.spec.config[:adapter_name] == 'PostgreSQL'
              self.use_transactional_fixtures = false

              after do
                User.delete_all
              end
            end

            it "should not clear thread's query cache if disabled but other thread's enabled" do
              User.connection.enable_query_cache!
              User.all.to_a
              User.connection.disable_query_cache!
              threaded(
                lambda{ |cc| User.delete_all; cc.call },
                lambda{ expect(User.connection.query_cache).not_to be_empty })
            end

            it "should not clear other thread's query cache" do
              User.connection.enable_query_cache!
              User.all.to_a
              User.connection.disable_query_cache!
              threaded(
                lambda{ |cc| User.delete_all; cc.call },
                lambda{ expect(User.connection.query_cache).not_to be_empty })
            end

            it "should clear cache for all connections" do
              u = User.create!(name: 'a')
              User.connection.cache do
                expect(u.reload.name).to eq 'a'
                ::Shackles.activate(:slave) do
                  expect(u.reload.name).to eq 'a'
                end
                expect(u.reload.name).to eq 'a'
                u.name = 'b'
                u.save!
                expect(u.reload.name).to eq 'b'
                ::Shackles.activate(:slave) do
                  expect(u.reload.name).to eq 'b'
                end
              end
            end
          end
        end
      end
    end
  end
end
