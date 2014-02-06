require "spec_helper"

module Switchman
  describe RSpecHelper do
    # strategically place these before we include the module
    before(:all) do
      Shard.default.should be_a(DefaultShard)
    end

    after(:all) do
      Shard.default.should be_a(DefaultShard)
    end

    include RSpecHelper

    it "should make the default shard a real shard" do
      Shard.default.should be_a(Shard)
    end
  end
end
