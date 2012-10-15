require 'tent_migrate/data'
require 'girl_friday'
require 'connection_pool'
require 'tent_client'

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
      # TODO: set all redis keys related to job to expire in n days
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
        @export_app ||= Data::User.get_job_export_app(job_key)
      end

      def import_app
        @import_app ||= Data::User.get_job_import_app(job_key)
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

      def import_app(app)
        # TODO: update tentd to handle this (should import app with authorizations)
        res = import_client.app.create(app)
      end

      def migrate_apps
        return unless apps = export_apps
        # TODO: save count of exported apps
        # TODO: save count of imported apps
        apps.each do |app|
          import_app(app)
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
      end

      def migrate_followers
        total_pages = (count_followers / PER_PAGE).ceil
        # TODO: save count of followers
        # TODO: save count of exported followers
        # TODO: save count of imported followers
        params = {}
        total_pages.times do
          followers = export_followers(params)
          return unless followers
          params[:before_id] = followers.last['id']

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
        res = import_client.following.create(following)
      end

      def migrate_followings
        total_pages = (count_followings / PER_PAGE).ceil
        # TODO: save count of followings
        # TODO: save count of exported followings
        # TODO: save count of imported followings
        params = {}
        total_pages.times do
          followings = export_followings(params)
          return unless followings
          params[:before_id] = followings.last['id']

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
      end

      def migrate_groups
        total_pages = (count_groups / PER_PAGE).ceil
        # TODO: save count of groups
        # TODO: save count of exported groups
        # TODO: save count of imported groups
        params = {}
        total_pages.times do
          groups = export_groups(params)
          return unless groups
          params[:before_id] = groups.last['id']

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

      def import_post(post)
        res = import_client.post.create(post)
      end

      def migrate_posts
        total_pages = (count_posts / PER_PAGE).ceil
        # TODO: save count of posts
        # TODO: save count of exported posts
        # TODO: save count of imported posts
        # TODO: handle post versions
        params = {}
        total_pages.times do
          posts = export_posts(params)
          return unless posts
          params[:before_id] = posts.last['id']

          posts.each { |f| import_post(f) }
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
