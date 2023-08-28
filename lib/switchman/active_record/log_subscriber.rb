# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module LogSubscriber
      # sadly, have to completely replace this
      def sql(event)
        if ::Rails.version < "7.1"
          self.class.runtime += event.duration
          return unless logger.debug?
        end

        payload = event.payload

        return if ::ActiveRecord::LogSubscriber::IGNORE_PAYLOAD_NAMES.include?(payload[:name])

        name  = "#{payload[:name]} (#{event.duration.round(1)}ms)"
        name  = "CACHE #{name}" if payload[:cached]
        sql   = payload[:sql].squeeze(" ")
        binds = nil
        shard = payload[:shard]
        shard = "  [#{shard[:database_server_id]}:#{shard[:id]} #{shard[:env]}]" if shard

        unless (payload[:binds] || []).empty?
          casted_params = type_casted_binds(payload[:type_casted_binds])
          binds = "  " + payload[:binds].zip(casted_params).map do |attr, value|
            render_bind(attr, value)
          end.inspect
        end

        name = colorize_payload_name(name, payload[:name])
        sql  = if ::Rails.version < "7.1"
                 color(sql, sql_color(sql), true)
               else
                 color(sql, sql_color(sql), bold: true)
               end

        debug "  #{name}  #{sql}#{binds}#{shard}"
      end
    end
  end
end
