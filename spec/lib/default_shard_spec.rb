require "spec_helper"

module Switchman
  describe DefaultShard do
    before(:all) do
      @user = User.create!
    end

    after(:all) do
      @user.destroy
    end

    context "sharding" do
      include RSpecHelper

      it "should be equivalent to a real default shard" do
        Shard.default.should be_is_a(Shard)
        @user.shard.should be_is_a(DefaultShard)
        @user.shard.should == Shard.default
        Shard.default.should == @user.shard
      end
    end
  end
end