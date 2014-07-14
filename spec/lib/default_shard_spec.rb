require "spec_helper"

module Switchman
  describe DefaultShard do
    context "sharding" do
      include RSpecHelper

      it "should be equivalent to a real default shard" do
        Shard.default.should be_is_a(Shard)
        DefaultShard.send(:new).should == Shard.default
      end
    end

    it "all defaultshards should be equivalent to each other" do
      DefaultShard.send(:new).should == DefaultShard.send(:new)
    end
  end
end