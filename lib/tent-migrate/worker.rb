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
        @export_client ||= TentClient.new(export_app['servers'], export_app['auth'])
      end

      def import_client
        @import_client ||= TentClient.new(import_app['servers'], import_app['auth'])
      end

      def perform
        migrate_apps # apps + authorizations
        migrate_followers # followers
        migrate_followings # followings
        migrate_groups # groups
        migrate_posts # posts + permissions
        migrate_profile

        Worker.expire_job_data(job_key)
      end

      private

      def export_apps
        res = export_client.app.list
        res.body if res.success?
      end

      def import_app!(app)
        res = import_client.app.create(app)
        success = res.success?
        if res.success?
          app_id = app['id']
          (app['authorizations'] || []).each do |app_authorization|
            res = import_client.app.authorization.create(app_id, app_authorization)
            success = res.success?
          end
        end
        Data.increment_job_stat(job_key, 'imported_apps_count', 1) if success
        res
      end

      def migrate_apps
        return unless apps = export_apps
        Data.increment_job_stat(job_key, 'exported_apps_count', apps.size)
        apps.each do |app|
          import_app!(app)
        end
      end

      def count_followers
        res = export_client.follower.count
        res.success? ? res.body.to_f : 0
      end

      def export_followers(params)
        res = export_client.follower.list(params)
        res.body if res.success?
      end

      def import_follower(follower)
        res = import_client.follower.create(follower)
        Data.increment_job_stat(job_key, 'imported_followers_count', 1) if res.success?
        res
      end

      def migrate_followers
        followers_count = count_followers
        total_pages = (followers_count / PER_PAGE).ceil
        Data.set_job_stat(job_key, "followers_count", followers_count)
        params = {}
        total_pages.times do
          followers = export_followers(params)
          return unless followers
          params[:before_id] = followers.last['id']

          Data.increment_job_stat(job_key, 'exported_followers_count', followers.size)

          followers.each { |f| import_follower(f) }
        end
      end

      def count_followings
        res = export_client.following.count
        res.success? ? res.body.to_f : 0
      end

      def export_followings(params)
        res = export_client.following.list(params)
        res.body if res.success?
      end

      def import_following(following)
        res = import_client.following.create(following['entity'], following)
        Data.increment_job_stat(job_key, 'imported_followings_count', 1) if res.success?
        res
      end

      def migrate_followings
        followings_count = count_followings
        total_pages = (followings_count / PER_PAGE).ceil
        Data.set_job_stat(job_key, "followings_count", followings_count)
        params = {}
        total_pages.times do
          followings = export_followings(params)
          return unless followings
          params[:before_id] = followings.last['id']

          Data.increment_job_stat(job_key, 'exported_followings_count', followings.size)

          followings.each { |f| import_following(f) }
        end
      end

      def count_groups
        res = export_client.group.count
        res.success? ? res.body.to_f : 0
      end

      def export_groups(params)
        res = export_client.group.list(params)
        res.body if res.success?
      end

      def import_group(group)
        res = import_client.group.create(group)
        Data.increment_job_stat(job_key, 'imported_groups_count', 1) if res.success?
        res
      end

      def migrate_groups
        groups_count = count_groups
        total_pages = (groups_count / PER_PAGE).ceil
        Data.set_job_stat(job_key, "groups_count", groups_count)
        params = {}
        total_pages.times do
          groups = export_groups(params)
          return unless groups
          params[:before_id] = groups.last['id']

          Data.increment_job_stat(job_key, 'exported_groups_count', groups.size)

          groups.each { |f| import_group(f) }
        end
      end

      def count_posts
        res = export_client.post.count
        res.success? ? res.body.to_f : 0
      end

      def export_posts(params)
        res = export_client.post.list(params)
        res.body if res.success?
      end

      def export_post_versions(post_id, params={})
        res = export_client.post.version.list(post_id, params)
        res.body if res.success?
      end

      def import_post(post)
        post_versions = export_post_versions(post['id']) # TODO: handle more than 200 post versions
        post_versions.sort_by { |p| p['version'] * -1 }.map do |post_version|
          res = import_client.post.create(post_version)
          Data.increment_job_stat(job_key, 'imported_posts_count', 1) if res.success?
          res
        end
      end

      def migrate_posts
        posts_count = count_posts
        total_pages = (posts_count / PER_PAGE).ceil
        Data.set_job_stat(job_key, 'posts_count', count_posts)
        params = {}
        total_pages.times do
          posts = export_posts(params)
          return unless posts
          params[:before_id] = posts.last['id']

          Data.increment_job_stat(job_key, 'exported_posts_count', posts.size)

          posts.each { |post| import_post(post) }
        end
      end

      def export_profile
        res = export_client.profile.get
        res.body
      end

      def import_profile(type, data)
        res = import_client.profile.update(type, data)
      end

      def migrate_profile
        profile = export_profile
        profile.each_pair do |type, data|
          import_profile(type, data)
        end
      end
    end
  end
end
