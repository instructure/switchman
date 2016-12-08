module Switchman
  module CallSuper
    def super_method_above(method_name, above_module)
      method = method(method_name)
      while method.owner != above_module
        method = method.super_method
      end
      method.super_method
    end

    def call_super(method, above_module, *args, &block)
      super_method_above(method, above_module).call(*args, &block)
    end
  end
end
