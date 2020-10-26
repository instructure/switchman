# frozen_string_literal: true

class Face < ActiveRecord::Base
  belongs_to :user, :required => false
end