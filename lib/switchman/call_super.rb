# frozen_string_literal: true

module Switchman
  module CallSuper
    def super_method_above(method_name, above_module)
      method = method(method_name)
      last_owner = method.owner
      while method.owner != above_module
        method = method.super_method
        raise "Could not find super method ``#{method_name}' for #{self.class}" if method.owner == last_owner
      end
      method.super_method
    end

    def call_super(method, above_module, *args, &block)
      super_method_above(method, above_module).call(*args, &block)
    end
  end
end
