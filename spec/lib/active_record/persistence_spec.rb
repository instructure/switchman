require "spec_helper"

module Switchman
  module ActiveRecord
    describe FinderMethods do
      include RSpecHelper

      class SqlMatches < Mocha::ParameterMatchers::Base
        def initialize(parent)
          @parent = parent
        end

        def matches?(available_parameters)
          param = available_parameters.shift
          return false unless param.respond_to?(:to_sql)
          @parent.matches?([param.to_sql])
        end

        def mocha_inspect
          "sql_matches(#{@parent.mocha_inspect})"
        end
      end

      describe "#touch" do
        it "should touch on the correct shard" do
          user = @shard1.activate { User.create! }

          User.connection.expects(:update).never
          # does not match the global id
          argument = Not(SqlMatches.new(regexp_matches(/#{user.global_id.to_s}/)))
          # but does match UPDATE <stuff> local_id
          argument &= SqlMatches.new(regexp_matches(/UPDATE.*#{user.local_id}/))
          # expects an update
          @shard1.activate { User.connection.expects(:update).with(argument, anything, anything).once }

          user.touch
        end
      end
    end
  end
end
