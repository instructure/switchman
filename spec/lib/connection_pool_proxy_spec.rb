require "spec_helper"

module Switchman
  describe ConnectionPoolProxy do
    it "should not share connections for sqlite shards on the same db" do
      @db = DatabaseServer.create(:config => { :adapter => 'sqlite3', :database => ':memory:' })
      @sqlite_shard1 = @db.shards.create!
      @sqlite_shard2 = @db.shards.create!
      ::ActiveRecord::Base.connection.should_not == @sqlite_shard2.activate { ::ActiveRecord::Base.connection }
      @sqlite_shard1.activate { ::ActiveRecord::Base.connection }.should_not == @sqlite_shard2.activate { ::ActiveRecord::Base.connection }
    end
  end
end
