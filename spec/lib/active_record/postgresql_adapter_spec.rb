# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe PostgreSQLAdapter do
      include RSpecHelper

      before do
        skip "requires PostgreSQL" unless ::ActiveRecord::Base.connection.adapter_name == "PostgreSQL"
      end

      describe "#quote_table_name" do
        before do
          shard = double(name: "bob")
          allow(::ActiveRecord::Base.connection).to receive(:shard).and_return(shard)
        end

        after do
          allow(::ActiveRecord::Base.connection).to receive(:shard).and_call_original
        end

        it "adds schema if not included" do
          expect(::ActiveRecord::Base.connection.quote_table_name("table")).to eq '"bob"."table"'
        end

        it "does not add schema if already included" do
          expect(::ActiveRecord::Base.connection.quote_table_name("schema.table")).to eq '"schema"."table"'
        end

        it "does not add schema under with_local_table_name" do
          ::ActiveRecord::Base.connection.with_local_table_name do
            expect(::ActiveRecord::Base.connection.quote_table_name("table")).to eq '"table"'
          end
          expect(::ActiveRecord::Base.connection.quote_table_name("table")).to eq '"bob"."table"'
        end
      end

      context "with table aliases" do
        it "qualifies tables, but not aliases or columns" do
          # preload schema metadata
          User.primary_key

          expect(User.joins(:parent).where(id: 1).to_sql).to include(<<~SQL.squish)
            * FROM "public"."users"
            INNER JOIN "public"."users" "parents_users"
              ON "parents_users"."id" = "users"."parent_id"
            WHERE "users"."id" = 1
          SQL
        end

        it "qualifies tables, but not aliases with IN" do
          # preload schema metadata
          User.primary_key

          expect(User.joins(:parent).where(id: [1, 2]).to_sql).to include(<<~SQL.squish)
            * FROM "public"."users"
            INNER JOIN "public"."users" "parents_users"
              ON "parents_users"."id" = "users"."parent_id"
            WHERE "users"."id" IN (1, 2)
          SQL
        end
      end

      describe "#indexes" do
        it "successfully lists indexes" do
          expect(::ActiveRecord::Base.connection.indexes(:users).length).not_to eq 0
        end

        it "identifies unique indexes" do
          conn = ::ActiveRecord::Base.connection

          conn.create_table :unique_index_test do |t|
            t.string :foo
          end
          conn.add_index :unique_index_test, [:foo], unique: true

          index = ::ActiveRecord::Base.connection.indexes(:unique_index_test).first
          expect(index.unique).to be(true)
        end
      end

      describe "#foreign_keys" do
        it "returns non-qualified to_table with qualified names" do
          search_path = User.connection.schema_search_path
          User.connection.schema_search_path = "''"
          expect(User.connection.foreign_keys(:users).first.to_table).to eq "users"
        ensure
          Face.connection.schema_search_path = search_path
        end
      end

      describe "#rename_table" do
        it "doesn't have problems with qualified names" do
          conn = ::ActiveRecord::Base.connection

          conn.create_table :rename_table_test do |t|
            t.integer :bob
            t.integer :joe
          end
          conn.add_index :rename_table_test, %i[bob joe]

          conn.rename_table(:rename_table_test, :rename_table_test2)
        end
      end

      describe "#columns" do
        it "properly quotes, even when nested within a with_local_table_names call" do
          search_path = User.connection.schema_search_path
          User.connection.schema_search_path = "''"
          User.connection.with_local_table_name do
            expect { User.connection.columns("users") }.not_to raise_error
          end
        ensure
          User.connection.schema_search_path = search_path
        end
      end
    end
  end
end
