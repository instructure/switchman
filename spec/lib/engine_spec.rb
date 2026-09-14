# frozen_string_literal: true

require "spec_helper"

module Switchman
  describe Engine do
    include RSpecHelper

    it "registers initializers in the correct order" do
      order = ::Rails.application.initializers.tsort_each.map(&:name)

      sm_arp = order.index("switchman.active_record_patch")
      ar_init_db = order.index("active_record.initialize_database")
      sm_ep = order.index("switchman.error_patch")
      as_init_tz = order.index("active_support.initialize_time_zone")
      sm_ic = order.index("switchman.initialize_cache")

      # check relative ordering
      expect(sm_arp).to be < ar_init_db
      expect(sm_ic).to be > ar_init_db
      expect(sm_ic).to be > as_init_tz
      expect(sm_ep).to be > sm_ic
    end
  end
end
