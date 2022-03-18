# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module DatabaseConfigurations
      private

      # key difference: assumes a hybrid two-tier structure; each third tier
      # is implicitly named, and their config is constructing by merging into
      # its parent
      def build_configs(configs)
        return configs.configurations if configs.is_a?(DatabaseConfigurations)
        return configs if configs.is_a?(Array)

        db_configs = configs.flat_map do |env_name, config|
          roles = config.keys.select { |k| config[k].is_a?(Hash) }
          base_config = config.except(*roles)

          name = "#{env_name}/primary"
          name = 'primary' if env_name == default_env
          base_db = build_db_config_from_raw_config(env_name, name, base_config)
          [base_db] + roles.map do |role|
            build_db_config_from_raw_config(env_name, "#{env_name}/#{role}",
                                            base_config.merge(config[role]))
          end
        end

        db_configs << environment_url_config(default_env, 'primary', {}) unless db_configs.find(&:for_current_env?)

        merge_db_environment_variables(default_env, db_configs.compact)
      end
    end
  end
end
