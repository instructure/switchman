# frozen_string_literal: true

class Feature < ApplicationRecord
  belongs_to :owner, polymorphic: true, optional: true
end
