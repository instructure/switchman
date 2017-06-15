module Switchman
  module ActiveRecord
    module LogSubscriber
      # sadly, have to completely replace this
      def sql(event)
        self.class.runtime += event.duration
        return unless logger.debug?

        payload = event.payload

        return if 'SCHEMA'.freeze == payload[:name]

        name  = '%s (%.1fms)'.freeze % [payload[:name], event.duration]
        sql   = payload[:sql].squeeze(' '.freeze)
        binds = nil
        shard = payload[:shard]
        shard = "  [#{shard[:database_server_id]}:#{shard[:id]} #{shard[:env]}]" if shard

        unless (payload[:binds] || []).empty?
          if ::Rails.version < '5'
            binds = "  " + payload[:binds].map { |col,v|
              render_bind(col, v)
            }.inspect
          elsif ::Rails.version < '5.0.3'
            binds = "  " + payload[:binds].map { |attr| render_bind(attr) }.inspect
          else
            casted_params = type_casted_binds(payload[:binds], payload[:type_casted_binds])
            binds = "  " + payload[:binds].zip(casted_params).map { |attr, value|
              render_bind(attr, value)
            }.inspect
          end
        end

        if ::Rails.version >= '5'
          name = colorize_payload_name(name, payload[:name])
          sql  = color(sql, sql_color(sql), true)
        else
          if odd?
            name = color(name, self.class::CYAN, true)
            sql  = color(sql, nil, true)
          else
            name = color(name, self.class::MAGENTA, true)
          end
        end

        debug "  #{name}  #{sql}#{binds}#{shard}"
      end
    end
  end
end
