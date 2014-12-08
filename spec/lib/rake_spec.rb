require "spec_helper"

require 'rake' if ::Rails.version < '4'
Dummy::Application.load_tasks

module Switchman
  describe Rake do
    include RSpecHelper

    describe '.shard_scope' do
      before do
        @s1, @s2, @s3 = [Shard.default, @shard1, @shard2].sort
      end

      def shard_scope(shard_ids)
        Rake.send(:shard_scope, (::Rails.version >= '4' ? Shard.all : Shard.scoped), shard_ids)
      end

      it "should work for default shard" do
        expect(shard_scope("default").to_a).to eq [Shard.default]
      end

      it "should exclude default shard" do
        scope = shard_scope("-default")
        # the query is negative
        expect(scope.to_sql).to be_include(Shard.default.id.to_s)
        shards = scope.to_a
        expect(shards).to_not be_empty
        expect(shards).to_not be_include(Shard.default)
      end

      it "should no-op exclude the default shard" do
        scope = shard_scope("default,-default")
        expect(scope.to_a).to be_empty
      end

      it "should work for a half-open range" do
        expect(shard_scope("#{@s1.id}...#{@s2.id}").to_a).to eq [@s1]
      end

      it "should work for a half-closed range" do
        expect(shard_scope("#{@s1.id}..#{@s2.id}").order(:id).to_a).to eq [@s1, @s2]
      end

      it "should remove from a range" do
        expect(shard_scope("#{@s1.id}..#{@s2.id},-#{@s1.id}").to_a).to eq [@s2]
      end

      it "should work for multiple ranges" do
        expect(shard_scope("#{@s1.id}...#{@s1.id + 1},#{@s3.id}...#{@s3.id + 1}").order(:id).to_a).to eq [@s1, @s3]
      end

      it "should work for beginning infinite range" do
        expect(shard_scope("..#{@s2.id}").order(:id).to_a).to eq [@s1, @s2]
      end

      it "should work for ending infinite range" do
        expect(shard_scope("#{@s2.id}..").order(:id).to_a).to eq [@s2, @s3]
      end

      it "should default include all if you're only excluding" do
        scope = shard_scope("-#{@s1.id}").to_a
        expect(scope.length).to eq (Shard.count - 1)
        expect(scope).to_not be_include(@s1)
      end

      it "should not forget about ranges when specific ids cancel out" do
        scope = shard_scope("#{@s1.id},#{@s2.id}..#{@s2.id},-#{@s1.id}").to_a
        expect(scope).to eq [@s2]
      end
    end
  end
end
