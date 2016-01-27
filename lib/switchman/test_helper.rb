module Switchman
  module TestHelper
    class << self
      def recreate_persistent_test_shards(dont_create: false)
        # recreate the default shard (it got buhleted)
        if Shard.default(true).is_a?(DefaultShard)
          begin
            Shard.create!(default: true)
          rescue
            raise unless dont_create
            # database doesn't exist yet, presumably cause we're creating it right now
            return [nil, nil]
          end
          Shard.default(true)
        end

        # can't auto-create a new shard on the default shard's db server if the
        # default shard is split across multiple db servers
        if ::ActiveRecord::Base.connection_handler.connection_pool_list.length > 1
          server1 = DatabaseServer.create(:config => Shard.default.database_server.config)
        else
          server1 = Shard.default.database_server
        end
        server2 = DatabaseServer.create(:config => Shard.default.database_server.config)

        if server1 == Shard.default.database_server && server1.config[:shard1] && server1.config[:shard2]
          # look for the shards in the db already
          shard1 = find_existing_test_shard(server1, server1.config[:shard1])
          shard2 = find_existing_test_shard(server2, server1.config[:shard2])

          shard1 ||= server1.shards.build
          shard1.name = server1.config[:shard1]
          shard1.save! if shard1.changed?
          shard2 ||= server2.shards.build
          shard2.name = server1.config[:shard2]
          shard2.save! if shard2.changed?

          recreate_shards = shard1.activate { ::ActiveRecord::Base.connection.tables == [] }
          if recreate_shards
            if dont_create
              shard1.destroy
              shard2.destroy
              return [nil, nil]
            end

            shard1.drop_database rescue nil
            shard1.destroy
            shard2.drop_database rescue nil
            shard2.destroy
            shard1 = server1.create_new_shard(:name => server1.config[:shard1])
            shard2 = server2.create_new_shard(:name => server1.config[:shard2])
          end
          [shard1, shard2]
        else
          [server1, server2]
        end
      end

      private
      def find_existing_test_shard(server, name)
        if server == Shard.default.database_server
          server.shards.where(name: name).first
        else
          shard = Shard.where("database_server_id IS NOT NULL AND name=?", name).first
          # if somehow databases got created in a different order, change the shard to match
          shard.database_server = server if shard
          shard
        end
      end
    end
  end
end