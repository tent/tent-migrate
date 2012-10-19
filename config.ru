require 'bundler'
Bundler.require

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

$stdout.sync = true

require 'airbrake'
Airbrake.configure do |config|
  config.api_key    = ENV['AIRBRAKE_API_KEY']
  config.host       = ENV['AIRBRAKE_HOST']
  config.port       = 443
  config.secure     = config.port == 443
end

OmniAuth.config.on_failure = lambda { |env|
  Airbrake.notify($!)
  [302, { 'Location' => '/auth/failure' }, []]
}

class CatchErrors
  def initialize(app)
    @app = app
  end

  def call(env)
    @app.call(env)
  rescue => e
    puts "Error: #{e.inspect}\n\t#{e.backtrace.join("\n\t")}"
    Airbrake.notify_or_ignore(e)
    [500, { 'Content-Type' => 'text/plain' }, ['Internal Sever Error']]
  end
end

require 'tent-migrate'
require 'rack/session/redis'
map '/' do
  use CatchErrors
  use Airbrake::Rack
  use Rack::Session::Redis, :redis_server => ENV['REDIS_URL'],
                            :namespace => 'session:tent-migrate',
                            :secret => ENV['COOKIE_SECRET'],
                            :expire_after => 2592000 # 1 month
  run TentMigrate::App.new
end

