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
        Data.set_job_stat(job_key, 'started_at', Time.now.to_i)
        new(job_key).perform
        Data.set_job_stat(job_key, 'finished_at', Time.now.to_i)
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
        migrate_groups
        migrate_followers
        migrate_followings
        migrate_posts
        migrate_apps

        Worker.expire_job_data(job_key)
      rescue => e
        log_exception(e)
      end

      def log_exception(e)
        Data.log_job_exception(job_key, e)
      end

      def log_exceptions(&block)
        begin
          yield
        rescue => e
          log_exception(e)
        end
      end

      private

      def error_response?(res)
        ((400...500).to_a - [404, 409]).include?(res.status)
      end

      def migrate_apps
        apps = export_apps
        Data.increment_job_stat(job_key, 'exported_apps_count', apps.size)
        apps.each do |app|
          log_exceptions do
            import_app_with_authorizations(app)
          end
        end
      end

      def export_apps
        res = export_client.app.list
        raise Error.new(res.body) if error_response?(res)
        return unless res.success?
        res.body
      end

      def import_app_with_authorizations(app)
        authorizations = app.delete('authorizations')
        res = import_client.app.create(app)
        return if res.status == 422 # app already exists
        raise Error.new(res.body) if error_response?(res)
        return unless res.success?
        app_id = app['id']
        (authorizations || []).each do |app_authorization|
          log_exceptions do
            res = import_client.app.authorization.create(app_id, app_authorization)
            raise Error.new(res.body) if error_response?(res)
          end
        end
        Data.increment_job_stat(job_key, 'imported_apps_count', 1)
      end

      def migrate_groups
        paginate(lambda { |group|
          Data.increment_job_stat(job_key, 'exported_groups_count', 1)
          import_group(group)
        }) do |params|
          res = export_client.group.list(params)
          raise Error.new(res.body) if error_response?(res)
          res.body
        end
      end

      def import_group(group)
        p ['import_group', group]
        res = import_client.group.create(group)
        raise Error.new("#{res.body}\n#{group.inspect}") if error_response?(res)
        return unless res.success?
        Data.increment_job_stat(job_key, 'imported_groups_count', 1)
        res
      end

      def migrate_followers
        paginate(lambda { |follower|
          Data.increment_job_stat(job_key, 'exported_followers_count', 1)
          import_follower(follower)
        }) do |params|
          res = export_client.follower.list(params)
          raise Error.new(res.body) if error_response?(res)
          return unless res.success?
          res.body
        end
      end

      def import_follower(follower)
        p ['import_follower', follower]
        res = import_client.follower.create(follower)
        raise Error.new("#{res.body}\n#{follower.inspect}") if error_response?(res)
        return unless res.success?
        Data.increment_job_stat(job_key, 'imported_followers_count', 1)
      end

      def migrate_followings
        paginate(lambda { |following|
          Data.increment_job_stat(job_key, 'exported_followings_count', 1)
          import_following(following)
        }) do |params|
          res = export_client.following.list(params)
          raise Error.new(res.body) if error_response?(res)
          return unless res.success?
          res.body
        end
      end

      def import_following(following)
        p ['import_following', following]
        res = import_client.following.create(following['entity'], following)
        raise Error.new("#{res.body}\n#{following.inspect}") if error_response?(res)
        return unless res.success?
        Data.increment_job_stat(job_key, 'imported_followings_count', 1)
      end

      def import_post(post)
        case post['type']
        when %r{\Ahttps://tent.io/types/post/group/}
        when %r{\Ahttps://tent.io/types/post/following/}
        when %r{\Ahttps://tent.io/types/post/follower/}
        when %r{\Ahttps://tent.io/types/post/profile/}
          # ignore
        when %r{\Ahttps://tent.io/types/post/delete/}
          # ignore, deleted posts don't get fetched
        else
          Data.increment_job_stat(job_key, 'exported_posts_count', 1)
          Data.job_stat_set_add(job_key, 'exported_post_ids', post['id'])
          p ['import_post', post['type']]
          import_standard_post(post)
        end
      end

      def update_post_entity(post)
        if post['entity'] == export_app['entity']
          post['entity'] = import_app['entity']
        end

        # repost
        if post['content'] && post['content']['entity'] == export_app['entity']
          post['content']['entity'] = import_app['entity']
        end

        post['mentions'].to_a.each do |mention|
          next unless mention['entity'] == export_app['entity']
          mention['entity'] = import_app['entity']
        end

        post
      end

      def import_standard_post(post)
        post_versions = export_post_versions(post['id']) # TODO: handle more than 200 post versions
        if post_versions && post_versions.kind_of?(Array)
          post_versions.sort_by { |p| p['version'] * -1 }.map do |post_version|
            res = import_client.post.create(update_post_entity(post_version))
            raise Error.new(res.body) if error_response?(res)
          end
          Data.job_stat_set_add(job_key, 'imported_post_ids', post['id'])
          Data.increment_job_stat(job_key, 'imported_posts_count', 1)
        else
          res = import_client.post.create(update_post_entity(post))
          raise Error.new([res.body, post].join("\n")) if error_response?(res)
          return unless res.success?
          Data.increment_job_stat(job_key, 'imported_posts_count', 1)
          Data.job_stat_set_add(job_key, 'imported_post_ids', post['id'])
        end
      end

      def export_post_versions(post_id, params={})
        res = export_client.post.version.list(post_id, params)
        raise Error.new(res.body) if error_response?(res)
        return unless res.success?
        res.body
      end

      def paginate(process_item, params={}, &block)
        params = {
          :secrets => true,
          :limit => PER_PAGE.to_i,
          :reverse => false
        }.merge(params)

        loop do
          items = nil
          log_exceptions { items = yield(params) }
          break if !items || items.size == 0
          params[:since_id] = items.last['id']

          items.each do |item|
            log_exceptions { process_item.call(item) }
          end
        end
      end

      def migrate_posts
        paginate(lambda { |post| import_post(post) }) do |params|
          res = export_client.post.list(params)
          raise Error.new(res.body) if error_response?(res)
          return unless res.success?
          res.body
        end
      end

      def export_profile
        res = export_client.profile.get
        raise Error.new(res.body) if error_response?(res)
        return unless res.success?
        res.body
      end

      def import_profile(type, data)
        res = import_client.profile.update(type, data)
        raise Error.new(res.body) if error_response?(res)
        return unless res.success?
        Data.increment_job_stat(job_key, "imported_profile_infos_count", 1)
      end

      def migrate_profile
        profile = export_profile
        profile.each_pair do |type, data|
          next if type =~ %r{\Ahttps://tent.io/types/info/core/}
          Data.set_job_stat(job_key, "profile_infos_count", 1)
          import_profile(type, data)
        end
      end
    end
  end
end
