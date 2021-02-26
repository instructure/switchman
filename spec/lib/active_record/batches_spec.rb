# frozen_string_literal: true

require 'spec_helper'

describe ::ActiveRecord::Batches do
  include Switchman::RSpecHelper

  describe '#find_in_batches' do
    it "doesn't form invalid queries with qualified_names" do
      User.shard(@shard1).find_in_batches {}
    end
  end
end
