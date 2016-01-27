require "spec_helper"

module Switchman
  module ActiveRecord
    describe SpawnMethods do
      include RSpecHelper

      describe "#merge" do
        it "should merge shard_value for multiple explicits" do
          result = User.shard([@shard1, @shard2]).merge(User.shard([Shard.default, @shard1]))
          expect(result.shard_value).to eq @shard1
          expect(result.shard_source_value).to eq :explicit
        end

        it "should merge shard_value relations for multiple explicits" do
          result = User.shard(Shard.where("id IN (?)", [@shard1, @shard2])).merge(User.shard(Shard.where(id: [Shard.default, @shard1])))
          expect(::ActiveRecord::Relation === result.shard_value).to eq true
          expect(result.shard_value.to_a).to eq [@shard1]
          expect(result.shard_source_value).to eq :explicit
        end

        it "should ignore implicit shard value lhs" do
          scope = User.all
          result = scope.merge(User.shard(@shard1))
          expect(result.shard_value).to eq @shard1
          expect(result.shard_source_value).to eq :explicit
        end

        it "should ignore implicit shard value rhs" do
          result = User.shard(@shard1).merge(User.all)
          expect(result.shard_value).to eq @shard1
          expect(result.shard_source_value).to eq :explicit
        end

        it "should take lhs shard_value for double implicit" do
          scope1 = @shard2.activate { User.all }
          result = scope1.merge(User.all)
          expect(result.shard_value).to eq @shard2
          expect(result.shard_source_value).to eq :implicit
        end
      end
    end
  end
end
