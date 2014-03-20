require "spec_helper"

module Switchman
  module ActiveRecord
    describe SpawnMethods do
      include RSpecHelper

      describe "#merge" do
        it "should merge shard_value for multiple explicits" do
          result = User.shard([@shard1, @shard2]).merge(User.shard([Shard.default, @shard1]))
          result.shard_value.should == @shard1
          result.shard_source_value.should == :explicit
        end

        it "should merge shard_value relations for multiple explicits" do
          result = User.shard(Shard.where("id IN (?)", [@shard1, @shard2])).merge(User.shard(Shard.where(id: [Shard.default, @shard1])))
          (::ActiveRecord::Relation === result.shard_value).should be_true
          result.shard_value.to_a.should == [@shard1]
          result.shard_source_value.should == :explicit
        end

        it "should ignore implicit shard value lhs" do
          scope = ::Rails.version < '4' ? User.scoped : User.all
          result = scope.merge(User.shard(@shard1))
          result.shard_value.should == @shard1
          result.shard_source_value.should == :explicit
        end

        it "should ignore implicit shard value rhs" do
          result = User.shard(@shard1).merge(::Rails.version < '4' ? User.scoped : User.all)
          result.shard_value.should == @shard1
          result.shard_source_value.should == :explicit
        end

        it "should take lhs shard_value for double implicit" do
          scope1 = @shard2.activate { ::Rails.version < '4' ? User.scoped : User.all }
          result = scope1.merge(::Rails.version < '4' ? User.scoped : User.all)
          result.shard_value.should == @shard2
          result.shard_source_value.should == :implicit
        end
      end
    end
  end
end
