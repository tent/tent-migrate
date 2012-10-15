require 'sinatra'
require 'omniauth-tent'
require 'tent_migrate/data'

module TentMigrate
  class App < Sinatra::Base
    use OmniAuth::Strategies::Tent, {
      :app => {
        :callback_path => '/export/auth/tent/callback',
        :name => 'Migrate',
        :description => 'Move your data',
        :icon => '',
        :url => ENV['HOST_DOMAIN'],
        :scopes => {
          'read_posts'        => '',
          'read_profile'      => 'Access your basic information'
          'write_profile'     => '',
          'read_followers'    => '',
          'read_followings'   => '',
          'read_groups'       => '',
          'read_permissions'  => '',
          'read_apps'         => '',
          'read_secrets'      => '',
        }
      },
      :profile_info_types => %w( all ),
      :post_types => %w( all ),
      :notification_url => ENV['NOTIFICATION_URL'].to_s,
      :get_app => lambda { |entity|
        Data::User.get_export_app(entity)
      },
    }

    use OmniAuth::Strategies::Tent, {
      :callback_path => '/import/auth/tent/callback',
      :app => {
        :name => 'Migrate',
        :description => 'Move your data',
        :icon => '',
        :url => ENV['HOST_DOMAIN'],
        :scopes => {
          'read_posts'        => '',
          'import_posts'      => '',
          'read_profile'      => 'Access your basic information'
          'write_profile'     => '',
          'read_followers'    => '',
          'write_followers'   => '',
          'read_followings'   => '',
          'write_followings'  => '',
          'read_groups'       => '',
          'write_groups'      => '',
          'read_permissions'  => '',
          'write_permissions' => '',
          'read_apps'         => '',
          'write_apps'        => '',
          'read_secrets'      => '',
          'write_secrets'     => '',
        }
      },
      :profile_info_types => %w( all ),
      :post_types => %w( all ),
      :notification_url => ENV['NOTIFICATION_URL'].to_s,
      :get_app => lambda { |entity|
        Data::User.get_import_app(entity)
      },
    }

    get '/' do
      # TODO: landing page goes here
      # - Step 1: Authenticate with export server
      # - Step 2: Autenticate with import server
      # - Migrate worker queued
      # - Step 3: Show status page (remind user to save the url)
      # - Step 4: Migration complete, show final stats, queue job to delete stats in n hours
    end

    get '/export/auth/tent/callback' do
      session['job_key'] = job_key = SecureRandom.hex(32)
      app_key = Data::User.set_export_app_from_auth_hash(env['omniauth.auth'])
      Data::User.set_job_export_app(job_key, app_key)
    end

    get '/import/auth/tent/callback' do
      job_key = session.delete('job_key')
      app_key = Data::User.set_import_app_from_auth_hash(env['omniauth.auth'])
      Data::User.set_job_import_app(job_key, app_key)

      Worker::MIGRATE_QUEUE << job_key

      redirect "/jobs/#{job_key}"
    end

    get '/jobs/:job_key' do
      # TODO: status page goes here with total/exported/imported counts
    end

    delete '/jobs/:job_key' do
      Data::User.delete_job(params[:job_key])
    end
  end
end
