# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActiveRecord
    describe StatementCache do
      include RSpecHelper

      before do
        @user1 = User.create!(name: 'user1')

        @appendage1 = @user1.appendages.create!
      end

      it 'executes a query with a where value of an object' do
        cache = ::ActiveRecord::StatementCache.create(Appendage.connection) do
          Appendage.where(id: @appendage1.id, user_id: @user1)
        end

        expect(cache.execute([], Appendage.connection)).to include @appendage1
      end

      it 'executes a query with a where value of a non-object' do
        cache = ::ActiveRecord::StatementCache.create(User.connection) do
          User.where(name: 'user1')
        end

        expect(cache.execute([], User.connection)).to include @user1
      end

      it 'calls the block with the result' do
        cache = ::ActiveRecord::StatementCache.create(User.connection) do
          User.where(name: 'user1')
        end

        expect { |block| cache.execute([], User.connection, &block) }.to yield_with_args(@user1)
      end
    end
  end
end
