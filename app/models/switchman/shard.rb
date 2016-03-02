# ActiveRecord::Base _must_ be loaded prior to the file that defines Shard
# Because Switchman hooks into ActiveSupport.on_load(:active_record) and
# reference it again (technically it gets referenced from inside
# ConnectionHandler#establish_connection, when AR's on_load(:active_record)
# hook gets called
ActiveRecord::Base
# in case it was referenced _before_ the on_load hook is added, then
# we need to make sure we define the class someone wanted (in which case
# AR's on_load hook won't be called either yet, and establish_connection
# will be safe)
require 'switchman/shard_internal'
