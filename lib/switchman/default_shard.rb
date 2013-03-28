require_dependency 'switchman/database_server'

class Switchman::DefaultShard
  def id; 'default'; end
  def activate(*categories); yield; end
  def activate!(*categories); end
  def default?; true; end
  def relative_id_for(local_id, target = nil); local_id; end
  def global_id_for(local_id); local_id; end
  def database_server_id; nil; end
  def database_server; ::Switchman::DatabaseServer.find(nil); end
  def name; @name; end
  def description; Rails.env; end
  # The default's shard is always the default shard
  def shard; self; end
  def _dump(depth)
    ''
  end
  def self._load(str)
    Shard.default
  end
end
