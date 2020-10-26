# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module PostgreSQLAdapter
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

      def tables(name = nil)
        query(<<-SQL, 'SCHEMA').map { |row| row[0] }
          SELECT tablename
          FROM pg_tables
          WHERE schemaname = '#{shard.name}'
        SQL
      end

      def extract_schema_qualified_name(string)
        name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(string.to_s)
        if string && !name.schema
          name.instance_variable_set(:@schema, shard.name)
        end
        [name.schema, name.identifier]
      end

      def view_exists?(name)
        name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(name.to_s)
        return false unless name.identifier
        if !name.schema
          name.instance_variable_set(:@schema, shard.name)
        end

        select_values(<<-SQL, 'SCHEMA').any?
          SELECT c.relname
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relkind IN ('v','m') -- (v)iew, (m)aterialized view
          AND c.relname = '#{name.identifier}'
          AND n.nspname = '#{shard.name}'
        SQL
      end

      def indexes(table_name)
        result = query(<<-SQL, 'SCHEMA')
           SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid
           FROM pg_class t
           INNER JOIN pg_index d ON t.oid = d.indrelid
           INNER JOIN pg_class i ON d.indexrelid = i.oid
           WHERE i.relkind = 'i'
             AND d.indisprimary = 'f'
             AND t.relname = '#{table_name}'
             AND i.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = '#{shard.name}' )
          ORDER BY i.relname
        SQL


        result.map do |row|
          index_name = row[0]
          unique = row[1] == true || row[1] == 't'
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

            if ::Rails.version >= "5.2"
              ::ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, index_name, unique, column_names, orders: orders, where: where, using: using)
            else
              ::ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, index_name, unique, column_names, [], orders, where, nil, using)
            end
          end
        end.compact
      end

      def index_name_exists?(table_name, index_name, _default = nil)
        exec_query(<<-SQL, 'SCHEMA').rows.first[0].to_i > 0
            SELECT COUNT(*)
            FROM pg_class t
            INNER JOIN pg_index d ON t.oid = d.indrelid
            INNER JOIN pg_class i ON d.indexrelid = i.oid
            WHERE i.relkind = 'i'
              AND i.relname = '#{index_name}'
              AND t.relname = '#{table_name}'
              AND i.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = '#{shard.name}' )
        SQL
      end

      def quote_local_table_name(name)
        # postgres quotes tables and columns the same; just pass through
        # (differs from quote_table_name_with_shard below by no logic to
        # explicitly qualify the table)
        quote_column_name(name)
      end

      def quote_table_name(name)
        return quote_local_table_name(name) if @use_local_table_name
        name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(name.to_s)
        if !name.schema
          name.instance_variable_set(:@schema, shard.name)
        end
        name.quoted
      end

      def with_local_table_name(enable = true)
        old_value = @use_local_table_name
        @use_local_table_name = enable
        yield
      ensure
        @use_local_table_name = old_value
      end

      def foreign_keys(table_name)

        # mostly copy-pasted from AR - only change is to the nspname condition for qualified names support
        fk_info = select_all <<-SQL.strip_heredoc
          SELECT t2.oid::regclass::text AS to_table, a1.attname AS column, a2.attname AS primary_key, c.conname AS name, c.confupdtype AS on_update, c.confdeltype AS on_delete
          FROM pg_constraint c
          JOIN pg_class t1 ON c.conrelid = t1.oid
          JOIN pg_class t2 ON c.confrelid = t2.oid
          JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid
          JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid
          JOIN pg_namespace t3 ON c.connamespace = t3.oid
          WHERE c.contype = 'f'
            AND t1.relname = #{quote(table_name)}
            AND t3.nspname = '#{shard.name}'
          ORDER BY c.conname
        SQL

        fk_info.map do |row|
          options = {
            column: row['column'],
            name: row['name'],
            primary_key: row['primary_key']
          }

          options[:on_delete] = extract_foreign_key_action(row['on_delete'])
          options[:on_update] = extract_foreign_key_action(row['on_update'])

          # strip the schema name from to_table if it matches
          to_table = row['to_table']
          to_table_qualified_name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(to_table)
          if to_table_qualified_name.schema == shard.name
            to_table = to_table_qualified_name.identifier
          end

          ::ActiveRecord::ConnectionAdapters::ForeignKeyDefinition.new(table_name, to_table, options)
        end
      end

      def add_index_options(_table_name, _column_name, **)
        index_name, index_type, index_columns, index_options, algorithm, using = super
        algorithm = nil if DatabaseServer.creating_new_shard && algorithm == "CONCURRENTLY"
        [index_name, index_type, index_columns, index_options, algorithm, using]
      end

      def rename_table(table_name, new_name)
        clear_cache!
        execute "ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_local_table_name(new_name)}"
        pk, seq = pk_and_sequence_for(new_name)
        if pk
          idx = "#{table_name}_pkey"
          new_idx = "#{new_name}_pkey"
          execute "ALTER INDEX #{quote_table_name(idx)} RENAME TO #{quote_local_table_name(new_idx)}"
          if seq && seq.identifier == "#{table_name}_#{pk}_seq"
            new_seq = "#{new_name}_#{pk}_seq"
            execute "ALTER TABLE #{seq.quoted} RENAME TO #{quote_local_table_name(new_seq)}"
          end
        end
        rename_table_indexes(table_name, new_name)
      end

      def rename_index(table_name, old_name, new_name)
        validate_index_length!(table_name, new_name)

        execute "ALTER INDEX #{quote_table_name(old_name)} RENAME TO #{quote_local_table_name(new_name)}"
      end

      def columns(*)
        with_local_table_name(false) { super }
      end
    end
  end
end
