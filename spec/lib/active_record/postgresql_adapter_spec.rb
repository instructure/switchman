require "spec_helper"

module Switchman
  module ActiveRecord
    describe PostgreSQLAdapter do
      describe '#quote_table_name' do
        before do
          skip "requires PostgreSQL" unless ::ActiveRecord::Base.connection.adapter_name == 'PostgreSQL'
          @config = ::ActiveRecord::Base.connection.instance_variable_get(:@config)
          @prior_use_qualified_names = @config[:use_qualified_names]
          @config[:use_qualified_names] = true
          shard = mock()
          shard.stubs(:name).returns('bob')
          ::ActiveRecord::Base.connection.stubs(:shard).returns(shard)
        end

        after do
          @config[:use_qualified_names] = @prior_use_qualified_names
        end

        it 'should add schema if not included' do
          expect(::ActiveRecord::Base.connection.quote_table_name('table')).to eq '"bob"."table"'
        end

        it 'should not add schema if already included' do
          expect(::ActiveRecord::Base.connection.quote_table_name('schema.table')).to eq '"schema"."table"'
        end
      end
    end
  end
end
