require 'redis'
require 'yajl'

module TentMigrate
  module Data
    class << self
      def redis_client
        @redis_client ||= Redis.new(:url => ENV['REDIS_URL'])
      end

      def parse_json(string)
        Yajl::Parser.parse(string) if string
      end

      def encode_json(data)
        Yajl::Encoder.encode(data)
      end

      def export_app_key(entity)
        "#{entity}-export_app"
      end

      def import_app_key(entity)
        "#{entity}-import_app"
      end

      def get_export_app(entity)
        parse_json(redis_client.get(export_app_key(entity)))
      end

      def get_import_app(entity)
        parse_json(redis_client.get(import_app_key(entity)))
      end

      def set_export_app_from_auth_hash(auth_hash)
        entity = auth_hash.uid
        key = export_app_key(entity)
        set_app_from_auth_hash(key, auth_hash)
        key
      end

      def set_import_app_from_auth_hash(auth_hash)
        entity = auth_hash.uid
        key = import_app_key(entity)
        set_app_from_auth_hash(key, auth_hash)
        key
      end

      def set_app_from_auth_hash(key, auth_hash)
        app = auth_hash.extra.raw_info.app.to_hash
        app = %w( id mac_key_id mac_key mac_algorithm ).inject({}) { |memo, k| memo[k] = app[k]; memo }
        app['auth'] = auth_hash.extra.credentials
        core_profile = extract_core_profile_from_auth_hash(auth_hash)
        app['servers'] = core_profile['servers']
        app['entity'] = core_profile['entity']
        redis_client.set(key, encode_json(app))
      end

      def extract_core_profile_from_auth_hash(auth_hash)
        profile = auth_hash.extra.raw_info.profile
        core_profile = (profile.find { |k,v|
          k =~ %r{\Ahttps://tent.io/types/info/core/}
        } || [{}]).last
      end

      def set_job_export_app(job_key, app_key)
        redis_client.set(job_key + '-export', app_key)
      end

      def set_job_import_app(job_key, app_key)
        redis_client.set(job_key + '-import', app_key)
      end

      def get_job_export_app(job_key)
        export_app_key = redis_client.get(job_key + '-export')
        return unless export_app_key
        parse_json(redis_client.get(export_app_key))
      end

      def get_job_import_app(job_key)
        import_app_key = redis_client.get(job_key + '-import')
        return unless import_app_key
        parse_json(redis_client.get(import_app_key))
      end

      def set_job_stat(job_key, stat_key, stat)
        redis_client.hset("#{job_key}-stats", stat_key, stat)
      end

      def increment_job_stat(job_key, stat_key, amount)
        redis_client.hincrby("#{job_key}-stats", stat_key, amount)
      end

      def get_job_stats(job_key)
        redis_client.hgetall("#{job_key}-stats")
      end

      def expire_job_data(job_key)
        expires = 86400 * 2
        export_app_key = redis_client.get(job_key + '-export')
        import_app_key = redis_client.get(job_key + '-import')
        [job_key, "#{job_key}-export", "#{job_key}-import", export_app_key, import_app_key, "#{job_key}-stats"].each do |key|
          redis_client.expire(key, expires)
        end
      end

      def delete_job(job_key)
        export_app_key = redis_client.get(job_key + '-export')
        import_app_key = redis_client.get(job_key + '-import')
        redis_client.del(job_key + '-export')
        redis_client.del(job_key + '-import')
        redis_client.del(job_key + '-stats')
        redis_client.del(export_app_key)
        redis_client.del(import_app_key)
      end
    end
  end
end
