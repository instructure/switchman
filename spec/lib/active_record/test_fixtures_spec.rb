# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe TestFixtures do
      # Rails fires :active_record_fixtures from inside
      # ActiveRecord::TestFixtures' `included` block, so hooking it prepends to
      # the including class rather than the module, which reverses precedence
      # against the host app's own fixture overrides.
      it "is prepended to ActiveRecord::TestFixtures itself" do
        expect(::ActiveRecord::TestFixtures.ancestors).to include(TestFixtures)
      end

      it "sits behind an including class's own overrides, not in front of them" do
        klass = Class.new do
          def self.name = "SomeTestCase"
          include ::ActiveRecord::TestFixtures
        end

        switchman_index = klass.ancestors.index(TestFixtures)
        rails_index = klass.ancestors.index(::ActiveRecord::TestFixtures)

        # the class's own methods win, then ours, then Rails'
        expect(klass.ancestors.index(klass)).to be < switchman_index
        expect(switchman_index).to be < rails_index
      end
    end
  end
end
