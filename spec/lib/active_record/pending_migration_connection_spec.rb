# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe PendingMigrationConnection do
      describe "pending migration check" do
        it "runs successfully" do
          ::ActiveRecord::Migration.check_pending!
        end
      end
    end
  end
end
