module Switchman
  module Arel
    module Visitors
      module PostgreSQL
        # the only difference is to remove caching (which only applies to Arel < 6.0/AR < 4.2)
        def quote_table_name name
          return name if ::Arel::Nodes::SqlLiteral === name
          @connection.quote_table_name(name)
        end
      end
    end
  end
end
