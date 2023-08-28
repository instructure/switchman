# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module DatabaseConfigurations
      # key difference: For each env name, ensure only one writable config is returned
      # since all should point to the same data, even if multiple are writable
      # (Picks 'primary' since it is guaranteed to exist and switchman handles activating
      # deploy through other means)
      if ::Rails.version < "7.1"
        def configs_for(include_replicas: false, name: nil, **)
          res = super
          if name && !include_replicas
            return nil unless name.end_with?("primary")
          elsif !include_replicas
            return res.select { |config| config.name.end_with?("primary") }
          end
          res
        end
      else
        def configs_for(include_hidden: false, name: nil, **)
          res = super
          if name && !include_hidden
            return nil unless name.end_with?("primary")
          elsif !include_hidden
            return res.select { |config| config.name.end_with?("primary") }
          end
          res
        end
      end

      private

      # key difference: assumes a hybrid two-tier structure; each third tier
      # is implicitly named, and their config is constructing by merging into
      # its parent
      def build_configs(configs)
        return configs.configurations if configs.is_a?(DatabaseConfigurations)
        return configs if configs.is_a?(Array)

        db_configs = configs.flat_map do |env_name, config|
          if config.is_a?(Hash)
            # It would be nice to do the auto-fallback that we want here, but we haven't
            # actually done that for years (or maybe ever) and it will be a big lift to get working
            roles = config.keys.select do |k|
              config[k].is_a?(Hash) || (config[k].is_a?(Array) && config[k].all?(Hash))
            end
            base_config = config.except(*roles)
          else
            base_config = config
            roles = []
          end

          name = "#{env_name}/primary"
          name = "primary" if env_name == default_env
          base_db = build_db_config_from_raw_config(env_name, name, base_config)
          [base_db] + roles.map do |role|
            build_db_config_from_raw_config(
              env_name,
              "#{env_name}/#{role}",
              base_config.merge(config[role].is_a?(Array) ? config[role].first : config[role])
            )
          end
        end

        db_configs << environment_url_config(default_env, "primary", {}) unless db_configs.find(&:for_current_env?)

        merge_db_environment_variables(default_env, db_configs.compact)
      end
    end
  end
end
