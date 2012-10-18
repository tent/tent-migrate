require 'tent-migrate/data'
require 'girl_friday'
require 'connection_pool'
require 'tent-client'

module TentMigrate
  module Worker
    GIRL_FRIDAY_CONFIG = {
      :store => GirlFriday::Store::Redis,
      :store_config => { :pool => ConnectionPool.new(:size => 3) {Redis.connect(:url => ENV['REDIS_URL'])} }
    }

    MIGRATE_QUEUE = GirlFriday::WorkQueue.new(:migrate, GIRL_FRIDAY_CONFIG) do |job_key|
      Migration.perform(job_key)
    end

    def self.expire_job_data(job_key)
      Data.expire_job_data(job_key)
    end

    class Migration
      PER_PAGE = 200.0

      class Error < StandardError
      end

      def self.perform(job_key)
        new(job_key).perform
      end

      attr_reader :job_key

      def initialize(job_key)
        @job_key = job_key
      end

      def export_app
        @export_app ||= Data.get_job_export_app(job_key)
      end

      def import_app
        @import_app ||= Data.get_job_import_app(job_key)
      end

      def export_client
        @export_client ||= TentClient.new(export_app['servers'], export_app['auth'].inject({}) { |memo, (k,v)| memo[k.to_sym] = v; memo })
      end

      def import_client
        @import_client ||= TentClient.new(import_app['servers'], import_app['auth'].inject({}) { |memo, (k,v)| memo[k.to_sym] = v; memo })
      end

      def perform
        migrate_profile
        migrate_posts
        migrate_apps

        Worker.expire_job_data(job_key)
      end

      private

      def error_response?(res)
        ((400...500).to_a - [404, 409]).include?(res)
      end

      def migrate_apps
        apps = export_apps
        Data.increment_job_stat(job_key, 'exported_apps_count', apps.size)
        apps.each do |app|
          import_app_with_authorizations(app)
        end
      end

      def export_apps
        res = export_client.app.list
        raise Error.new(res.body) if error_response?(res)
        res.body
      end

      def import_app_with_authorizations(app)
        res = import_client.app.create(app)
        raise Error.new(res.body) if error_response?(res)
        if error_response?(res)
          app_id = app['id']
          (app['authorizations'] || []).each do |app_authorization|
            res = import_client.app.authorization.create(app_id, app_authorization)
            raise Error.new(res.body) if error_response?(res)
          end
        end
        Data.increment_job_stat(job_key, 'imported_apps_count', 1)
      end

      def import_post(post)
        case post['type']
        when %r{\Ahttps://tent.io/types/post/group/}
          import_group_post(post)
        when %r{\Ahttps://tent.io/types/post/following/}
          import_following_post(post)
        when %r{\Ahttps://tent.io/types/post/follower/}
          import_follower_post(post)
        when %r{\Ahttps://tent.io/types/post/profile/}
          # ignore
        when %r{\Ahttps://tent.io/types/post/delete/}
          import_delete_post(post)
        else
          import_standard_post(post)
        end
      end

      def import_group_post(post)
        case post['action']
        when 'create'
          res = export_client.group.get(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          group = res.body
          import_group(group)
        when 'delete'
          res = export_client.group.delete(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          Data.increment_job_stat(job_key, 'imported_groups_count', -1)
        end
      end

      def import_group(group)
        res = import_client.group.create(group)
        raise Error.new(res.body) if error_response?(res)
        Data.increment_job_stat(job_key, 'imported_groups_count', 1)
        res
      end

      def import_following_post(post)
        case post['action']
        when 'create'
          res = export_client.following.get(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          following = res.body
          import_following(following)
        when 'delete'
          res = import_client.following.delete(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          Data.increment_job_stat(job_key, 'imported_followings_count', -1)
        end
      end

      def import_following(following)
        res = import_client.following.create(following['entity'], following)
        raise Error.new(res.body) if error_response?(res)
        Data.increment_job_stat(job_key, 'imported_followings_count', 1)
      end

      def import_follower_post(post)
        case post['action']
        when 'create'
          res = export_client.follower.get(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          follower = res.body
          import_follower(follower)
        when 'delete'
          res = import_client.follower.delete(post['id'])
          return if res.status == 404
          raise Error.new(res.body) if error_response?(res)
          Data.increment_job_stat(job_key, 'imported_followers_count', -1)
        end
      end

      def import_follower(follower)
        res = import_client.follower.create(follower)
        raise Error.new(res.body) if error_response?(res)
        Data.increment_job_stat(job_key, 'imported_followers_count', 1)
      end

      def import_delete_post(post)
        res = import_client.post.delete(post['id'])
        raise Error.new(res.body) if error_response?(res)
        Data.increment_job_stat(job_key, 'imported_posts_count', -1)
      end

      def import_standard_post(post)
        post_versions = export_post_versions(post['id']) # TODO: handle more than 200 post versions
        if post_versions
          post_versions.sort_by { |p| p['version'] * -1 }.map do |post_version|
            res = import_client.post.create(post_version)
            Data.increment_job_stat(job_key, 'imported_posts_count', 1) if error_response?(res)
            res
          end
        else
          res = import_client.post.create(post)
          Data.increment_job_stat(job_key, 'imported_posts_count', 1) if error_response?(res)
          [res]
        end
      end

      def get_first_post
        res = export_client.post.list(
          :since_time => 0,
          :limit => 1,
          :reverse => false
        )
        res.body.first if error_response?(res)
      end

      def export_posts(params)
        res = export_client.post.list(params)
        res.body if error_response?(res)
      end

      def export_post_versions(post_id, params={})
        res = export_client.post.version.list(post_id, params)
        res.body if error_response?(res)
      end

      def migrate_posts
        first_post = get_first_post
        return unless first_post
        import_post(first_post)
        params = {
          :since_id => first_post['id'],
          :limit => PER_PAGE.to_i,
          :reverse => false
        }
        while (posts = export_posts(params)) && posts.size > 0
          params[:since_id] = posts.last['id']

          Data.increment_job_stat(job_key, 'exported_posts_count', posts.size)

          posts.each { |post| import_post(post) }
        end
      end

      def add_export_server_to_profile(key, export_core_profile)
        export_core_profile['servers'].concat(import_app['servers'])
        export_client.profile.update(key, export_core_profile)
      end

      def export_profile
        res = export_client.profile.get
        raise Error.new(res.body) if error_response?(res)
        res.body
      end

      def import_profile(type, data)
        res = import_client.profile.update(type, data)
        raise Error.new(res.body) if error_response?(res)
        Data.increment_job_stat(job_key, "imported_profile_infos_count", 1)
      end

      def migrate_profile
        profile = export_profile
        core_profile_key, core_profile = profile.find { |type, data| type =~ %r{\Ahttps://tent.io/types/info/core/} }
        raise Error.new('core_profile missing') unless core_profile_key && core_profile
        add_export_server_to_profile(core_profile_key, core_profile)

        Data.set_job_stat(job_key, "profile_infos_count", profile.keys.size)
        profile.each_pair do |type, data|
          import_profile(type, data)
        end
      end
    end
  end
end
