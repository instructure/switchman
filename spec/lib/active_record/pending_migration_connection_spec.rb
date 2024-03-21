# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    if ::Rails.version > "7.3.1"
      describe PendingMigrationConnection do
        context "with Rails > 7.3.1" do
          describe "#current_role" do
            it "runs successfully" do
              expect(::ActiveRecord::PendingMigrationConnection.current_role).to eq(::ActiveRecord::Base.current_role)
            end
          end

          describe "#current_switchman_shard" do
            it "runs successfully" do
              expect(
                ::ActiveRecord::PendingMigrationConnection.current_switchman_shard
              ).to eq(::ActiveRecord::Base.current_switchman_shard)
            end
          end
        end
      end
    end
  end
end
