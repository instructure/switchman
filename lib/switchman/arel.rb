module Switchman
  module Arel
    module Visitors
      module ToSql
        def visit_Arel_Nodes_TableAlias *args
          if ::Rails.version < '4.2'
            o = args.shift
            "#{visit o.relation, *args} #{quote_local_table_name o.name}"
          else
            o, collector = args
            collector = visit o.relation, collector
            collector << " "
            collector << quote_local_table_name(o.name)
          end
        end

        def visit_Arel_Attributes_Attribute *args
          o = args.first
          self.last_column = column_for(o) if ::Rails.version < '4.0'.freeze
          join_name = o.relation.table_alias || o.relation.name
          result = "#{quote_local_table_name join_name}.#{quote_column_name o.name}"
          unless ::Rails.version < '4.2'.freeze
            result = args.last << result
          end
          result
        end

        def quote_local_table_name name
          return name if ::Arel::Nodes::SqlLiteral === name
          @connection.quote_local_table_name(name)
        end
      end

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
