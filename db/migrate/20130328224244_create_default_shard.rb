# frozen_string_literal: true

class CreateDefaultShard < ActiveRecord::Migration[4.2]
  def up
    return if Switchman::Shard.default.is_a?(Switchman::Shard)

    Switchman::Shard.reset_column_information
    Switchman::Shard.create!(default: true)
    Switchman::Shard.default(reload: true)
  end
end
