# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActionController
    describe Caching do
      include RSpecHelper

      shared_examples_for 'sharded cache store' do
        describe 'cache_store' do
          before do
            DatabaseServer.all.each do |ds|
              ds.instance_variable_set(:@cache_store, double("cache_store #{ds.id}"))
            end
          end

          after do
            DatabaseServer.all.each do |ds|
              ds.instance_variable_set(:@cache_store, nil)
            end
          end

          it 'shares the cache across shards on the same server' do
            expect(Shard.default.activate { subject.cache_store }).
              to eq(@shard1.activate { subject.cache_store })
          end

          it 'does not share the cache across shards on different servers' do
            expect(@shard1.activate { subject.cache_store }).
              not_to eq(@shard2.activate { subject.cache_store })
          end
        end

        it 'is not assignable' do
          expect { subject.cache_store = :memory_store }.to raise_exception(NoMethodError)
        end
      end

      describe 'ActionController::Base class' do
        subject { ::ActionController::Base }

        it_behaves_like 'sharded cache store'
      end

      describe 'ActionController::Base instance' do
        subject { ::ActionController::Base.new }

        it_behaves_like 'sharded cache store'
      end
    end
  end
end
