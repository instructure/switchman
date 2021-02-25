# frozen_string_literal: true

class MirrorUser < MirrorUniverse
  has_one :user
  belongs_to :belongs_to_user, class_name: :User, :required => false
end
