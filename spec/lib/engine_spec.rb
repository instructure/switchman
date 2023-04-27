# frozen_string_literal: true

require "spec_helper"

module Switchman
  describe Engine do
    include RSpecHelper

    it "registers initializers in the correct order" do
      order = ::Rails.application.initializers.map(&:name)

      # These are part of Rail::Railtie - we never want to supersede these
      indexes = []
      indexes.push(order.index(:load_environment_hook))
      indexes.push(order.index(:load_active_support))
      indexes.push(order.index(:set_eager_load))
      indexes.push(order.index(:initialize_logger))
      indexes.push(order.index(:initialize_cache))
      indexes.push(order.index((::Rails.version < "7.0") ? :initialize_dependency_mechanism : :setup_once_autoloader))
      indexes.push(order.index(:bootstrap_hook))
      indexes.push(order.index(:set_secrets_root))

      indexes_max = indexes.max

      # Our initializers will and should always be after the second :set_autoload_paths
      # The first one is the application switchman is installed in, the second is switchman itself
      # followed by any other engines registered
      lower_bound = order.each_index.select { |i| order[i] == :set_autoload_paths }.second
      sm_arp = order.index("switchman.active_record_patch")
      sm_ep = order.index("switchman.error_patch")
      sm_ic = order.index("switchman.initialize_cache")

      expect(indexes_max).to be < lower_bound

      expect(lower_bound).to be < sm_arp # switchman.active_record_patch is first
      expect(sm_ep).to eq(sm_arp + 1) # switchman.error_patch is second
      expect(sm_ic).to eq(sm_ep + 1) # switchman.initialize_cache is third
    end
  end
end
