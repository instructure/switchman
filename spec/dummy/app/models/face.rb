# frozen_string_literal: true

class Face < ApplicationRecord
  belongs_to :user, optional: true
end
