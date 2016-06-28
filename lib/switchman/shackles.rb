module Switchman
  module Shackles
    module ClassMethods
      def self.prepended(klass)
        klass.send(:remove_method, :ensure_handler)
      end

      # drops the save_handler and ensure_handler calls from the vanilla
      # Shackles' implementation.
      def activate!(environment)
        environment ||= :master
        activated_environments << environment
        old_environment = self.environment
        Thread.current[:shackles_environment] = environment
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
  end
end
