module Switchman
  module ActiveRecord
    module LogSubscriber
      def self.included(klass)
        klass.send(:remove_method, :sql)
      end

      # sadly, have to completely replace this
      def sql(event)
        self.class.runtime += event.duration
        return unless logger.debug?

        payload = event.payload

        return if 'SCHEMA' == payload[:name]

        name  = '%s (%.1fms)' % [payload[:name], event.duration]
        sql   = payload[:sql].squeeze(' ')
        binds = nil
        connection = ObjectSpace._id2ref(payload[:connection_id])

        unless (payload[:binds] || []).empty?
          binds = "  " + payload[:binds].map { |col,v|
            if col
              [col.name, v]
            else
              [nil, v]
            end
          }.inspect
        end

        if odd?
          name = color(name, self.class::CYAN, true)
          sql  = color(sql, nil, true)
        else
          name = color(name, self.class::MAGENTA, true)
        end

        debug "  #{name}  #{sql}#{binds}  [shard #{connection.shard.id} #{::Shackles.environment}]"
      end
    end
  end
end
