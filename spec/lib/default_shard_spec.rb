require "spec_helper"

module Switchman
  describe DefaultShard do
    context "sharding" do
      include RSpecHelper

      it "should be equivalent to a real default shard" do
        expect(Shard.default).to be_is_a(Shard)
        expect(DefaultShard.send(:new)).to eq Shard.default
      end
    end

    it "all defaultshards should be equivalent to each other" do
      expect(DefaultShard.send(:new)).to eq DefaultShard.send(:new)
    end
  end
end