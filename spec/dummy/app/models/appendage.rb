# frozen_string_literal: true

class Appendage < ActiveRecord::Base
  belongs_to :user, required: false
  has_many :digits

  has_many :features, as: :owner

  scope :has_no_value, -> { where(value: nil) }
  scope :has_value, -> { where("appendages.value IS NOT NULL") }

  attr_writer :should_test_scoping, :associated_shards
  attr_reader :all_appendages

  after_save :test_scoping

  def test_scoping
    @all_appendages = Appendage.all.to_a if @should_test_scoping
  end

  class << self
    attr_accessor :associated_shards_map
  end

  def self.associated_shards_for(global_id)
    associated_shards_map[global_id] if global_id && associated_shards_map
  end

  def associated_shards
    self.class.associated_shards_for(global_id) || @associated_shards || [shard]
  end
end
