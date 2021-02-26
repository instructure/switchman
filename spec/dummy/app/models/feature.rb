# frozen_string_literal: true

class Feature < ActiveRecord::Base
  belongs_to :owner, polymorphic: true, required: false
end
