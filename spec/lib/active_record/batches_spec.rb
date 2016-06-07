require "spec_helper"

module Switchman
  module ActiveRecord
    describe Batches do
      include RSpecHelper

      describe "#find_in_batches" do
        it "doesn't form invalid queries with qualified_names" do
          User.connection.stubs(:use_qualified_names?).returns(true)
          User.shard(@shard1).find_in_batches {}
        end
      end
    end
  end
end
