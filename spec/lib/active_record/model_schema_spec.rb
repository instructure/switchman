# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActiveRecord
    describe ModelSchema do
      include RSpecHelper

      after do
        User.reset_table_name
        User.instance_variable_set(:@quoted_table_name, nil)
      end

      it 'caches per-shard' do
        User.reset_table_name
        User.instance_variable_set(:@quoted_table_name, nil)
        expect(User.connection).to receive(:quote_table_name).and_return('1')
        expect(User.quoted_table_name).to eq '1'
        expect(User.connection).to receive(:quote_table_name).and_return('2')
        expect(User.quoted_table_name).to eq '1'
        @shard1.activate do
          expect(User.quoted_table_name).to eq '2'
        end
        expect(User.quoted_table_name).to eq '1'
      end
    end
  end
end
