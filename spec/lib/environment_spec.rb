# frozen_string_literal: true

require 'etc'
require "spec_helper"

module Switchman
  describe Environment do
    describe ".cpu_count" do
      it "shells out for the cpu count" do
        if Etc.respond_to?(:nprocessors)
          expect(Environment.cpu_count).to eq(Etc.nprocessors)
        else
          expect(Environment.cpu_count("echo 42")).to eq(42)
        end
      end

      it "return 0 if processor counter doesn't exist" do
        if Etc.respond_to?(:nprocessors)
          Etc.stubs(:nprocessors).returns(0)
          expect(Environment.cpu_count).to eq(0)
        else
          expect(Environment.cpu_count("nonsense_nproc")).to eq(0)
        end
      end
    end
  end
end
