# frozen_string_literal: true

module Switchman
  module Arel
    module Table
      def klass
        @klass || ::ActiveRecord::Base
      end
    end

    module Visitors
      module ToSql
        # rubocop:disable Naming/MethodName
        # rubocop:disable Naming/MethodParameterName

        def visit_Arel_Nodes_Cte(o, collector)
          collector << quote_local_table_name(o.name)
          collector << " AS "

          case o.materialized
          when true
            collector << "MATERIALIZED "
          when false
            collector << "NOT MATERIALIZED "
          end

          visit o.relation, collector
        end

        def visit_Arel_Nodes_TableAlias(o, collector)
          collector = visit o.relation, collector
          collector << " "
          collector << quote_local_table_name(o.name)
        end

        def visit_Arel_Attributes_Attribute(o, collector)
          join_name = o.relation.table_alias || o.relation.name
          collector << quote_local_table_name(join_name) << "." << quote_column_name(o.name)
        end

        # rubocop:enable Naming/MethodName
        # rubocop:enable Naming/MethodParameterName

        def quote_local_table_name(name)
          return name if ::Arel::Nodes::SqlLiteral === name

          @connection.quote_local_table_name(name)
        end
      end
    end
  end
end
