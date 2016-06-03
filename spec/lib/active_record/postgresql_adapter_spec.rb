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
      end

      context "table aliases" do
        it "qualifies tables, but not aliases or columns" do
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
      end
    end
  end
end
