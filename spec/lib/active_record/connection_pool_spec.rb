require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionPool do
      it "should be able to access another shard on a db server after the 'primary' shard is gone" do
        pending 'A "real" database"' unless Shard.default.database_server.shareable?
        # separate connections
        server = DatabaseServer.create(:config => Shard.default.database_server.config.dup)
        s1 = server.shards.create!(:name => 'non_existent_shard') # don't actually create any schema
        s2 = server.shards.create! # inherit's the default shard's config, which is functional
        s1.activate do
          lambda { User.count }.should raise_exception
        end
        # the config for s1 should not be the permanent default for all new
        # connections now
        s2.activate do
          lambda { User.count }.should_not raise_exception
        end
      end
    end
  end
end
