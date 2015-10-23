require "spec_helper"

module Switchman
  module ActiveRecord
    describe ModelSchema do
      include RSpecHelper

      after do
        User.reset_table_name
      end

      it "caches per-shard" do
        User.reset_table_name
        User.connection.expects(:quote_table_name).returns("1")
        expect(User.quoted_table_name).to eq "1"
        User.connection.expects(:quote_table_name).returns("2")
        expect(User.quoted_table_name).to eq "1"
        @shard1.activate do
          expect(User.quoted_table_name).to eq "2"
        end
        expect(User.quoted_table_name).to eq "1"
      end
    end
  end
end
