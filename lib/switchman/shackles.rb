module Switchman
  module Shackles
    module ClassMethods
      def ensure_handler
        raise "This should not be called with switchman installed"
      end

      # drops the save_handler and ensure_handler calls from the vanilla
      # Shackles' implementation.
      def activate!(environment)
        environment ||= :master
        activated_environments << environment
        old_environment = self.environment
        @environment = environment
        old_environment
      end

      # since activate! really is just a variable swap now, it's safe to use in
      # the ensure block, simplifying the implementation
      def activate(environment)
        old_environment = activate!(environment)
        yield
      ensure
        activate!(old_environment)
      end
    end

    def self.included(klass)
      klass.extend(ClassMethods)
      klass.singleton_class.send(:remove_method, :ensure_handler)
      klass.singleton_class.send(:remove_method, :activate!)
      klass.singleton_class.send(:remove_method, :activate)
    end
  end
end
