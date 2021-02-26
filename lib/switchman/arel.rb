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

        def visit_Arel_Nodes_TableAlias(*args)
          o, collector = args
          collector = visit o.relation, collector
          collector << ' '
          collector << quote_local_table_name(o.name)
        end

        def visit_Arel_Attributes_Attribute(*args)
          o = args.first
          join_name = o.relation.table_alias || o.relation.name
          result = "#{quote_local_table_name join_name}.#{quote_column_name o.name}"
          args.last << result
        end

        # rubocop:enable Naming/MethodName

        def quote_local_table_name(name)
          return name if ::Arel::Nodes::SqlLiteral === name

          @connection.quote_local_table_name(name)
        end
      end
    end
  end
end
