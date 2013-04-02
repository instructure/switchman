require "spec_helper"

module Switchman
  module ActiveRecord
    describe QueryCache do
      include RSpecHelper

      it "should isolate queries to multiple shards on the same server" do
        @shard1.activate do
          User.create!
          User.create!
        end
        @shard3.activate do
          User.create!
        end
        @shard1.activate { User.all }.should_not == @shard3.activate { User.all }
      end
    end
  end
end
