class Switchman::DatabaseServer
  attr_accessor :id, :config

  class << self
    def all
      database_servers.values
    end

    def find(id_or_all)
      return self.all if id_or_all == :all
      return id_or_all.map { |id| self.database_servers[id || Rails.env] }.compact.uniq if id_or_all.is_a?(Array)
      database_servers[id_or_all || Rails.env]
    end

    def create(settings = {})
      raise "database servers should be set up in database.yml" unless Rails.env.test?
      id = 1
      while database_servers[id.to_s]; id += 1; end
      server = DatabaseServer.new({ :id => id.to_s }.merge(settings))
      server.instance_variable_set(:@fake, true)
      raise "database server #{server.id} already exists" if self.database_servers[server.id]
      database_servers[server.id] = server
    end

    def server_for_new_shard
      servers = all.select { |s| s.config[:open] }
      return find(nil) if servers.empty?
      servers[rand(servers.length)]
    end

    private
    def database_servers
      unless @database_servers
        @database_servers = {}.with_indifferent_access
        ActiveRecord::Base.configurations.each do |(id, config)|
          @database_servers[id] = Switchman::DatabaseServer.new(:id => id, :config => config)
        end
      end
      @database_servers
    end
  end

  def initialize(settings = {})
    self.id = settings[:id]
    self.config = (settings[:config] || {}).deep_symbolize_keys
  end

  def destroy
    raise "database servers should be set up in database.yml" unless Rails.env.test?
    self.class.database_servers.delete(self.id) if self.id
  end

  def fake?
    @fake
  end

  def shareable?
    username = self.config[:username]
    @shareable = self.config[:adapter] != 'sqlite3' && username !~ /%?\{[a-zA-Z0-9_]+\}/
  end
end
