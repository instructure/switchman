# frozen_string_literal: true

require "spec_helper"

module Switchman
  describe StandardError do
    include RSpecHelper

    it "keeps track of active shard when an exception is raised" do
      begin
        @shard2.activate do
          raise "hi"
        end
      rescue => e
        expect(e.current_shard).to eq @shard2
      end
    end
  end
end
