# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe AbstractAdapter do
      include RSpecHelper

      it "should update the connection's last_query_at on query" do
        conn = @shard1.activate{ User.connection }
        allow(Time).to receive(:now).and_return(conn.last_query_at + 1.minute)
        @shard1.activate{ User.create! }
        expect(conn.last_query_at).to eq Time.now
      end

      context "non-transactional" do
        self.use_transactional_tests = false

        after do
          ::ActiveRecord::Base.clear_all_connections!
        end

        it "doesn't get confused if another env is active when creating the SchemaMigration class" do
          # this doesn't manifest itself in test normally
          allow(::Rails.env).to receive(:test?).and_return(false)
          ::GuardRail.activate(:deploy) do
            # clean slate
            ::ActiveRecord::Base.clear_all_connections!
            # the first thing accessed is an unsharded model
            ::Switchman::Shard.connection
            # now talk to a sharded model
            name1 = ::ActiveRecord::Base.connection.schema_migration.connection_specification_name
            name2 = @shard2.activate { ::ActiveRecord::Base.connection.schema_migration.connection_specification_name }
            expect(name1.to_sym).to eq :primary
            expect(name2.to_sym).to eq :primary
          end
        end
      end
    end
  end
end
