require "spec_helper"

module Switchman
  module ActiveRecord
    describe PostgreSQLAdapter do
      before do
        skip "requires PostgreSQL" unless ::ActiveRecord::Base.connection.adapter_name == 'PostgreSQL'
      end

      describe '#quote_table_name' do
        before do
          shard = mock()
          shard.stubs(:name).returns('bob')
          ::ActiveRecord::Base.connection.stubs(:use_qualified_names?).returns(true)
          ::ActiveRecord::Base.connection.stubs(:shard).returns(shard)
        end

        it 'should add schema if not included' do
          expect(::ActiveRecord::Base.connection.quote_table_name('table')).to eq '"bob"."table"'
        end

        it 'should not add schema if already included' do
          expect(::ActiveRecord::Base.connection.quote_table_name('schema.table')).to eq '"schema"."table"'
        end

        it 'should not add schema under with_local_table_name' do
          ::ActiveRecord::Base.connection.with_local_table_name do
            expect(::ActiveRecord::Base.connection.quote_table_name('table')).to eq '"table"'
          end
          expect(::ActiveRecord::Base.connection.quote_table_name('table')).to eq '"bob"."table"'
        end
      end

      context "table aliases" do
        it "qualifies tables, but not aliases or columns" do
          # preload schema metadata
          User.primary_key
          shard = mock()
          shard.stubs(:name).returns('bob')
          ::ActiveRecord::Base.connection.stubs(:use_qualified_names?).returns(true)
          ::ActiveRecord::Base.connection.stubs(:shard).returns(shard)
          ::ActiveRecord::Base.connection.stubs(:columns).returns([])

          expect(User.joins(:parent).where(id: 1).to_sql).to be_include %{* FROM "bob"."users" INNER JOIN "bob"."users" "parents_users" ON "parents_users"."id" = "users"."parent_id" WHERE "users"."id" = 1}
        end
      end

      context 'indexes' do
        it "successfully lists indexes" do
          shard = mock()
          shard.stubs(:name).returns(nil)
          ::ActiveRecord::Base.connection.stubs(:use_qualified_names?).returns(true)
          ::ActiveRecord::Base.connection.stubs(:shard).returns(shard)

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

      describe 'foreign_keys' do
        it 'returns non-qualified to_table with qualified names' do
          begin
            search_path = User.connection.schema_search_path
            User.connection.schema_search_path = "''"
            ::ActiveRecord::Base.connection.stubs(:use_qualified_names?).returns(true)
            expect(User.connection.foreign_keys(:users).first.to_table).to eq 'users'
          ensure
            Face.connection.schema_search_path = search_path
          end
        end
      end

      describe '#rename_table' do
        it "doesn't have problems with qualified names" do
          conn = ::ActiveRecord::Base.connection
          conn.stubs(:use_qualified_names?).returns(true)

          conn.create_table :rename_table_test do |t|
            t.integer :bob
            t.integer :joe
          end
          conn.add_index :rename_table_test, [:bob, :joe]

          conn.rename_table(:rename_table_test, :rename_table_test2)
        end
      end
    end
  end
end
