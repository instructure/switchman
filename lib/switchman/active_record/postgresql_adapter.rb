module Switchman
  module ActiveRecord
    module PostgreSQLAdapter
      def self.prepended(klass)
        klass::NATIVE_DATABASE_TYPES[:primary_key] = "bigserial primary key".freeze
      end

      # copy/paste; use quote_local_table_name
      def create_database(name, options = {})
        options = { encoding: 'utf8' }.merge!(options.symbolize_keys)

        option_string = options.sum do |key, value|
          case key
            when :owner
              " OWNER = \"#{value}\""
            when :template
              " TEMPLATE = \"#{value}\""
            when :encoding
              " ENCODING = '#{value}'"
            when :collation
              " LC_COLLATE = '#{value}'"
            when :ctype
              " LC_CTYPE = '#{value}'"
            when :tablespace
              " TABLESPACE = \"#{value}\""
            when :connection_limit
              " CONNECTION LIMIT = #{value}"
            else
              ""
          end
        end

        execute "CREATE DATABASE #{quote_local_table_name(name)}#{option_string}"
      end

      # copy/paste; use quote_local_table_name
      def drop_database(name) #:nodoc:
        execute "DROP DATABASE IF EXISTS #{quote_local_table_name(name)}"
      end

      def current_schemas
        select_values("SELECT * FROM unnest(current_schemas(false))")
      end

      def use_qualified_names?
        @config[:use_qualified_names]
      end

      def tables(name = nil)
        schema = shard.name if use_qualified_names?

        query(<<-SQL, 'SCHEMA').map { |row| row[0] }
          SELECT tablename
          FROM pg_tables
          WHERE schemaname = #{schema ? "'#{schema}'" : 'ANY (current_schemas(false))'}
        SQL
      end

      def table_exists?(name)
        if ::Rails.version < '4.2'
          schema, table = ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::Utils.extract_schema_and_table(name.to_s)
          return false unless table
          schema ||= shard.name if use_qualified_names?

          binds = [[nil, table]]
          binds << [nil, schema] if schema

          exec_query(<<-SQL, 'SCHEMA').rows.first[0].to_i > 0
              SELECT COUNT(*)
              FROM pg_class c
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind in ('v','r')
              AND c.relname = '#{table.gsub(/(^"|"$)/,'')}'
              AND n.nspname = #{schema ? "'#{schema}'" : 'ANY (current_schemas(false))'}
          SQL
        else
          name =  ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(name.to_s)
          return false unless name.identifier
          if !name.schema && use_qualified_names?
            name.instance_variable_set(:@schema, shard.name)
          end

          exec_query(<<-SQL, 'SCHEMA').rows.first[0].to_i > 0
              SELECT COUNT(*)
              FROM pg_class c
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind IN ('r','v','m') -- (r)elation/table, (v)iew, (m)aterialized view
              AND c.relname = '#{name.identifier}'
              AND n.nspname = #{name.schema ? "'#{name.schema}'" : 'ANY (current_schemas(false))'}
          SQL
        end
      end

      def indexes(table_name)
        schema = shard.name if use_qualified_names?

        result = query(<<-SQL, 'SCHEMA')
           SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid
           FROM pg_class t
           INNER JOIN pg_index d ON t.oid = d.indrelid
           INNER JOIN pg_class i ON d.indexrelid = i.oid
           WHERE i.relkind = 'i'
             AND d.indisprimary = 'f'
             AND t.relname = '#{table_name}'
             AND i.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = #{schema ? "'#{schema}'" : 'ANY (current_schemas(false))'} )
          ORDER BY i.relname
        SQL


        result.map do |row|
          index_name = row[0]
          unique = row[1] == 't'
          indkey = row[2].split(" ")
          inddef = row[3]
          oid = row[4]

          columns = Hash[query(<<-SQL, "SCHEMA")]
          SELECT a.attnum, a.attname
          FROM pg_attribute a
          WHERE a.attrelid = #{oid}
          AND a.attnum IN (#{indkey.join(",")})
          SQL

          column_names = columns.stringify_keys.values_at(*indkey).compact

          unless column_names.empty?
            # add info on sort order for columns (only desc order is explicitly specified, asc is the default)
            desc_order_columns = inddef.scan(/(\w+) DESC/).flatten
            orders = desc_order_columns.any? ? Hash[desc_order_columns.map {|order_column| [order_column, :desc]}] : {}
            where = inddef.scan(/WHERE (.+)$/).flatten[0]
            using = inddef.scan(/USING (.+?) /).flatten[0].to_sym

            ::ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, index_name, unique, column_names, [], orders, where, nil, using)
          end
        end.compact
      end

      def index_name_exists?(table_name, index_name, default)
        schema = shard.name if use_qualified_names?

        exec_query(<<-SQL, 'SCHEMA').rows.first[0].to_i > 0
            SELECT COUNT(*)
            FROM pg_class t
            INNER JOIN pg_index d ON t.oid = d.indrelid
            INNER JOIN pg_class i ON d.indexrelid = i.oid
            WHERE i.relkind = 'i'
              AND i.relname = '#{index_name}'
              AND t.relname = '#{table_name}'
              AND i.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = #{schema ? "'#{schema}'" : 'ANY (current_schemas(false))'} )
        SQL
      end

      def quote_local_table_name(name)
        # postgres quotes tables and columns the same; just pass through
        # (differs from quote_table_name below by no logic to explicitly
        # qualify the table)
        quote_column_name(name)
      end

      def quote_table_name name
        if ::Rails.version < '4.2'.freeze
          schema, name_part = extract_pg_identifier_from_name(name.to_s)

          if !name_part && use_qualified_names? && shard.name
            schema, name_part = shard.name, schema
          end

          unless name_part
            quote_column_name(schema)
          else
            table_name, name_part = extract_pg_identifier_from_name(name_part)
            "#{quote_column_name(schema)}.#{quote_column_name(table_name)}"
          end
        else

          name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(name.to_s)
          if !name.schema && use_qualified_names?
            name.instance_variable_set(:@schema, shard.name)
          end
          name.quoted
        end
      end
    end
  end
end
