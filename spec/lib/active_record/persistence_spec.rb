require "spec_helper"

module Switchman
  module ActiveRecord
    describe FinderMethods do
      include RSpecHelper

      describe "#touch" do
        it "should touch on the correct shard" do
          user = @shard1.activate { User.create! }

          User.connection.expects(:update).never

          # expects an update
          @shard1.activate do
            User.connection.expects(:update).once.with do |arel, name, binds|
              # does not match the global id
              # but does match the local_id
              binds.first.value == user.local_id
            end
          end

          user.touch
        end
      end
    end
  end
end
