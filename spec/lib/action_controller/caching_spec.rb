require "spec_helper"

module Switchman
  module ActionController
    describe Caching do
      include RSpecHelper

      shared_examples_for "sharded cache store" do
        describe "cache_store" do
          before do
            DatabaseServer.all.each do |ds|
              ds.instance_variable_set(:@cache_store, stub("cache_store #{ds.id}"))
            end
          end

          after do
            DatabaseServer.all.each do |ds|
              ds.instance_variable_set(:@cache_store, nil)
            end
          end

          it "should share the cache across shards on the same server" do
            @shard1.activate{ subject.cache_store }.
              should == @shard3.activate{ subject.cache_store }
          end

          it "should not share the cache across shards on different servers" do
            @shard1.activate{ subject.cache_store }.
              should_not == @shard2.activate{ subject.cache_store }
          end
        end

        it "should not be assignable" do
          expect{ subject.cache_store = :memory_store }.to raise_exception(NoMethodError)
        end
      end

      describe "ActionController::Base class" do
        subject { ::ActionController::Base }
        it_should_behave_like "sharded cache store"
      end

      describe "ActionController::Base instance" do
        subject { ::ActionController::Base.new }
        it_should_behave_like "sharded cache store"
      end
    end
  end
end
