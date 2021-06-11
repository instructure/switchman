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
            ''
          end
        end

        execute "CREATE DATABASE #{quote_local_table_name(name)}#{option_string}"
      end

      # copy/paste; use quote_local_table_name
      def drop_database(name) #:nodoc:
        execute "DROP DATABASE IF EXISTS #{quote_local_table_name(name)}"
      end

      def current_schemas
        select_values('SELECT * FROM unnest(current_schemas(false))')
      end

      def extract_schema_qualified_name(string)
        name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(string.to_s)
        name.instance_variable_set(:@schema, shard.name) if string && !name.schema
        [name.schema, name.identifier]
      end

      # significant change: use the shard name if no explicit schema
      def quoted_scope(name = nil, type: nil)
        schema, name = extract_schema_qualified_name(name)
        type = \
          case type # rubocop:disable Style/HashLikeCase
          when 'BASE TABLE'
            "'r','p'"
          when 'VIEW'
            "'v','m'"
          when 'FOREIGN TABLE'
            "'f'"
          end
        scope = {}
        scope[:schema] = quote(schema || shard.name)
        scope[:name] = quote(name) if name
        scope[:type] = type if type
        scope
      end

      def foreign_keys(table_name)
        super.each do |fk|
          to_table_qualified_name = ::ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(fk.to_table)
          fk.to_table = to_table_qualified_name.identifier if to_table_qualified_name.schema == shard.name
        end
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
        name.instance_variable_set(:@schema, shard.name) unless name.schema
        name.quoted
      end

      def with_global_table_name(&block)
        with_local_table_name(false, &block)
      end

      def with_local_table_name(enable = true) # rubocop:disable Style/OptionalBooleanParameter
        old_value = @use_local_table_name
        @use_local_table_name = enable
        yield
      ensure
        @use_local_table_name = old_value
      end

      def add_index_options(_table_name, _column_name, **)
        index, algorithm, if_not_exists = super
        algorithm = nil if DatabaseServer.creating_new_shard && algorithm == 'CONCURRENTLY'
        [index, algorithm, if_not_exists]
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
