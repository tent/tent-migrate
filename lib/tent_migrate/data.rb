require 'redis'
require 'yajl'

module TentMigrate
  module Data
    def self.redis_client
      @redis_client ||= Redis.new(:url => ENV['REDIS_URL'])
    end

    def self.parse_json(string)
      Yajl::Parser.parse(string) if string
    end

    def self.encode_json(data)
      Yajl::Encoder.encode(data)
    end

    module User
      def self.export_app_key(entity)
        "#{entity}-export_app"
      end

      def self.import_app_key(entity)
        "#{entity}-import_app"
      end

      def self.get_export_app(entity)
        Data.parse_json(Data.redis_client.get(export_app_key(entity)))
      end

      def self.get_import_app(entity)
        Data.parse_json(Data.redis_client.get(import_app_key(entity)))
      end

      def self.set_export_app_from_auth_hash(auth_hash)
        entity = auth_hash.uid
        key = export_app_key(entity)
        set_app_from_auth_hash(key, auth_hash)
        key
      end

      def self.set_export_app_from_auth_hash(auth_hash)
        entity = auth_hash.uid
        key = import_app_key(entity)
        set_app_from_auth_hash(key, auth_hash)
        key
      end

      def self.set_app_from_auth_hash(key, auth_hash)
        app = auth_hash.extra.raw_info.app
        app = %w( id mac_key_id mac_key mac_algorithm ).inject({}) { |memo, k| memo[k] = app[k] }
        app['auth'] = auth_hash.extra.credentials
        app['servers'] = extract_servers_from_auth_hash(auth_hash)
        Data.redis_client.set(key, Data.encode_json(app))
      end

      def self.extract_servers_from_auth_hash(auth_hash)
        profile = auth_hash.extra.raw_info.profile
        core_profile = (profile.find { |k,v|
          k =~ %r{\Ahttps://tent.io/types/info/core/}
        } || [{}]).last
        core_profile['servers']
      end

      def self.set_job_export_app(job_key, app_key)
        Data.redis_client.set(job_key + '-export', app_key)
      end

      def self.set_job_import_app(job_key, app_key)
        Data.redis_client.set(job_key + '-import', app_key)
      end

      def self.get_job_export_app(job_key)
        export_app_key = Data.redis_client.get(job_key + '-export')
        return unless export_app_key
        Data.parse_json(Data.redis_client.get(export_app_key))
      end

      def self.get_job_import_app(job_key)
        import_app_key = Data.redis_client.get(job_key + '-import')
        return unless import_app_key
        Data.parse_json(Data.redis_client.get(import_app_key))
      end

      def self.delete_job(job_key)
        export_app_key = Data.redis_client.get(job_key + '-export')
        import_app_key = Data.redis_client.get(job_key + '-import')
        Data.redis_client.del(job_key + '-export')
        Data.redis_client.del(job_key + '-import')
        Data.redis_client.del(export_app_key)
        Data.redis_client.del(import_app_key)
      end
    end
  end
end
