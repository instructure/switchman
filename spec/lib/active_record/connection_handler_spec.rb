require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionHandler do
      include RSpecHelper

      it "should use different proxies for different categories" do
        expect(Shard.connection_pool).not_to eq User.connection_pool
      end

      it "should share underlying pools for different categories on the same shard" do
        expect(Shard.connection_pool.current_pool).to eq User.connection_pool.current_pool
      end

      it "should insert sharding for connections established after initialization" do
        expect(User.connection_pool).to eq ::ActiveRecord::Base.connection_pool
        begin
          config = { :adapter => 'sqlite3', :database => ':memory:', :something_unique_in_the_spec => true }
          User.establish_connection(config)
          expect(User.connection_pool).not_to eq ::ActiveRecord::Base.connection_pool
          expect(User.connection_pool.spec.config).to eq config
          expect(User.connection_pool).to be_is_a(ConnectionPoolProxy)
          @shard2.activate do
            expect(User.connection_pool.spec.config).to eq ::ActiveRecord::Base.connection_pool.spec.config
            expect(User.connection_pool.spec.config).not_to eq config
          end
        ensure
          User.remove_connection
          expect(User.connection_pool).to eq ::ActiveRecord::Base.connection_pool
          expect(User.connection_pool).to be_is_a(ConnectionPoolProxy)
        end
      end

      it "should set up separate pools for different categories" do
        expect(User.connection_pool).not_to eq MirrorUser.connection_pool
        mu = MirrorUser.create!
        expect(MirrorUser.find(mu.local_id)).to eq mu
        # didn't activate the :mirror_universe category
        @shard1.activate { expect(MirrorUser.find(mu.local_id)).to eq mu }
        @shard1.activate(:mirror_universe) { expect(MirrorUser.where(id: mu.local_id).first).to eq nil }
      end
    end
  end
end
