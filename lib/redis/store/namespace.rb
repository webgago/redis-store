class Redis
  class Store < self
    module Namespace
      attr_reader :namespace

      def set(key, val, options = nil)
        with_namespace(key) { |namespaced_key| super(namespaced_key, val, options) }
      end

      def setnx(key, val, options = nil)
        with_namespace(key) { |namespaced_key| super(namespaced_key, val, options) }
      end

      def get(key, options = nil)
        with_namespace(key) { |namespaced_key| super(namespaced_key, options) }
      end

      def exists(key)
        with_namespace(key) { |namespaced_key| super(namespaced_key) }
      end

      def incrby(key, increment)
        with_namespace(key) { |namespaced_key| super(namespaced_key, increment) }
      end

      def decrby(key, increment)
        with_namespace(key) { |namespaced_key| super(namespaced_key, increment) }
      end

      def keys(pattern = "*")
        with_namespace(pattern) { |namespaced_pattern| super(namespaced_pattern) }
      end

      def del(*keys)
        super *keys.map {|key| interpolate(key) }
      end

      def mget(*keys)
        super *keys.map {|key| interpolate(key) }
      end

      def to_s
        "#{super} with namespace #{@namespace}"
      end

      def clear_namespace
        del "*"
      end

      private
        def with_namespace(key)
          yield interpolate(key)
        end

        def interpolate(key)
          key.match(%r{^#{@namespace}\:}) ? key : "#{@namespace}:#{key}"
        end
    end
  end
end
