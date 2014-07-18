require "spec_helper"

module Switchman
  module ActiveRecord
    describe AbstractAdapter do
      include RSpecHelper

      it "should update the connection's last_query_at on query" do
        conn = @shard1.activate{ User.connection }
        Time.stubs(:now).returns(conn.last_query_at + 1.minute)
        @shard1.activate{ User.create! }
        expect(conn.last_query_at).to eq Time.now
      end
    end
  end
end
