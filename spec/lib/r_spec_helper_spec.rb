# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe RSpecHelper do
    context 'when unsharded' do
      it "doesn't make shards accessible" do
        # by virtue of including RSpecHelper somewhere, test shards will
        # always already be set up by this point (though not accessible),
        # but only if we are running a sharding spec
        expect(Shard.count).to eq 0
        expect(Shard.default).to be_a(DefaultShard)
      end

      it "doesn't set up sharding at all if no sharded specs are run" do
        run_groups = RSpec.world.filtered_examples.select { |_k, v| v.present? }.map(&:first)
        pending 'run without other sharding specs' if run_groups.any? { |group| RSpecHelper.included_in?(group) }

        expect(RSpecHelper.class_variable_defined?(:@@default_shard)).to eq false
        expect(RSpecHelper.class_variable_get(:@@shard1)).to be_nil
      end

      it 'sets up sharding but hides it if other sharding specs are run' do
        run_groups = RSpec.world.filtered_examples.select { |_k, v| v.present? }.map(&:first)
        pending 'run alongside sharding specs' unless run_groups.any? { |group| RSpecHelper.included_in?(group) }

        expect(RSpecHelper.class_variable_get(:@@default_shard)).to be_a(Shard)
        expect(RSpecHelper.class_variable_get(:@@shard1)).to be_a(Shard)
      end
    end

    context 'with sharding' do
      # strategically place these before we include the module
      before(:all) do
        expect(Shard.default).to be_a(DefaultShard)
      end

      after(:all) do
        expect(Shard.default).to be_a(DefaultShard)
      end

      include RSpecHelper

      it 'makes the default shard a real shard' do
        expect(Shard.default).to be_a(Shard)
      end
    end
  end
end
