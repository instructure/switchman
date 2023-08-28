# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionHandler do
      include RSpecHelper

      describe "#resolve_pool_config" do
        it "shares the schema reflection", if: ::Rails.version >= "7.1" do
          server_reflection = ::ActiveRecord::Base.establish_connection.pool_config.schema_reflection
          server2_reflection = @shard2.activate do
            ::ActiveRecord::Base.establish_connection.pool_config.schema_reflection
          end
          expect(server_reflection).to be(server2_reflection)
        end
      end
    end
  end
end
