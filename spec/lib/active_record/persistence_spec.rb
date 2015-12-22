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
              if ::Rails.version < '4.2'
                arel.to_sql.match(/#{user.local_id}/) && !arel.to_sql.match(/#{user.global_id}/)
              else
                binds.first.last == user.local_id
              end
            end
          end

          user.touch
        end
      end
    end
  end
end
