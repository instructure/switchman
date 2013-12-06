class Feature < ActiveRecord::Base
  belongs_to :owner, :polymorphic => true

  attr_accessible :owner, :value
end