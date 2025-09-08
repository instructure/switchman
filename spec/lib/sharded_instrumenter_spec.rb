# frozen_string_literal: true

require "spec_helper"

module Switchman
  class TestLogSubscriber < ::ActiveRecord::LogSubscriber; end

  describe ShardedInstrumenter do
    include RSpecHelper

    before do
      TestLogSubscriber.attach_to(:active_record)
    end

    it "logs shard info with queries" do
      expect_any_instance_of(TestLogSubscriber).to receive(:sql).with(
        an_object_satisfying do |event|
          expect(event.payload[:shard]).to eq({
                                                database_server_id: @shard1.database_server.id,
                                                id: @shard1.id,
                                                env: :primary
                                              })
        end
      ).at_least(:once)

      @shard1.activate { User.first }
    end
  end
end
