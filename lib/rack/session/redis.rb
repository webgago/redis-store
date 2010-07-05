module Rack
  module Session
    # Redis session storage for Rack applications.
    #
    # Options:
    #  :key     => Same as with the other cookie stores, key name
    #  :secret  => Encryption secret for the key
    #  :host    => Redis host name, default is localhost
    #  :port    => Redis port, default is 6379
    #  :db      => Database number, defaults to 0. Useful to separate your session storage from other data
    #  :key_prefix  => Prefix for keys used in Redis, e.g. myapp-. Useful to separate session storage keys visibly from others
    #  :expire_after => A number in seconds to set the timeout interval for the session. Will map directly to expiry in Redis
    class Redis < Abstract::ID
      attr_reader :mutex, :pool
      DEFAULT_OPTIONS = Abstract::ID::DEFAULT_OPTIONS.merge :redis_server => "localhost:6379"

      def initialize(app, options = {})
        super
        @mutex = Mutex.new
        @key_prefix = options[:key_prefix] || ""
        servers = [options[:servers]].flatten.compact.map do |server_options|
          {
            :namespace => 'rack:session',
            :host => 'localhost',
            :port => '6379',
            :db => 0
          }.update(RedisFactory.convert_to_redis_client_options(server_options))
        end
        @pool = RedisFactory.create(*servers) || @default_options[:redis_server]
      end

      def generate_sid
        loop do
          sid = super
          break sid unless @pool.marshalled_get(sid)
        end
      end

      def get_session(env, sid)
        session = @pool.marshalled_get(prefixed(sid)) if sid
        @mutex.lock if env['rack.multithread']
        unless sid and session
          env['rack.errors'].puts("Session '#{prefixed(sid).inspect}' not found, initializing...") if $VERBOSE and not sid.nil?
          session = {}
          sid = generate_sid
          ret = @pool.marshalled_set prefixed(sid), session
          raise "Session collision on '#{prefixed(sid).inspect}'" unless ret
        end
        return [sid, session]
      rescue Errno::ECONNREFUSED
        warn "#{self} is unable to find server."
        warn $!.inspect
        return [ nil, {} ]
      ensure
        @mutex.unlock if env['rack.multithread']
      end

      def set_session(env, session_id, new_session, options)
        @mutex.lock if env['rack.multithread']
        session = @pool.marshalled_get(session_id) rescue {}
        if options[:renew] or options[:drop]
          @pool.del session_id
          return false if options[:drop]
          session_id = generate_sid
          @pool.marshalled_set prefixed(session_id), 0
        end
        @pool.marshalled_set prefixed(session_id), new_session, options
        return session_id
      rescue Errno::ECONNREFUSED
        warn "#{self} is unable to find server."
        warn $!.inspect
        return false
      ensure
        @mutex.unlock if env['rack.multithread']
      end

      private
      def prefixed(sid)
        "#{@key_prefix}#{sid}"
      end
    end
  end
end
