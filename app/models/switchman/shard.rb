module Switchman
  class Shard < ActiveRecord::Base
    attr_accessible :name, :database_server, :default
  end
end
