module Switchman
  module ActiveRecord
    module PostgreSQLAdapter
      def self.included(klass)
        klass::NATIVE_DATABASE_TYPES[:primary_key] = "bigserial primary key".freeze
        klass.send(:remove_method, :quote_table_name) if ::Rails.version < '4' && klass.instance_method(:quote_table_name).owner == klass
      end

      def current_schemas
        select_values("SELECT * FROM unnest(current_schemas(false))")
      end

      def quote_table_name name
        if ::Rails.version < '4.2'
          schema, name_part = extract_pg_identifier_from_name(name.to_s)

          if !name_part && @config[:use_qualified_names] && shard.name
            schema, name_part = shard.name, schema
          end

          unless name_part
            quote_column_name(schema)
          else
            table_name, name_part = extract_pg_identifier_from_name(name_part)
            "#{quote_column_name(schema)}.#{quote_column_name(table_name)}"
          end
        else
          name = Utils.extract_schema_qualified_name(name.to_s)
          if !name.schema && @config[:use_qualified_names]
            name.instance_variable_set(:@schema, shard.name)
          end
          name.quoted
        end
      end
    end
  end
end
