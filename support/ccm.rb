# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'net/http'
require 'uri'
require 'fileutils'
require 'logger'
require 'cliver'
require 'os'
require 'dse'

# Cassandra Cluster Manager integration for driving a cassandra cluster from tests.
module CCM extend self
  class SameOrderLoadBalancingPolicy < Cassandra::LoadBalancing::Policy
    class Plan
      def initialize(hosts)
        @hosts = hosts
      end

      def has_next?
        !@hosts.empty?
      end

      def next
        @hosts.shift
      end
    end

    include MonitorMixin

    def initialize
      @hosts = ::Array.new

      mon_initialize
    end

    def host_up(host)
      synchronize { @hosts = @hosts.dup.push(host).sort_by!(&:ip) }

      self
    end

    def host_down(host)
      synchronize do
        @hosts = @hosts.dup
        @hosts.delete(host)
      end

      self
    end

    def distance(host)
      @hosts.include?(host) ? :local : :ignore
    end

    def plan(keyspace, statement, options)
      Plan.new(@hosts.dup)
    end
  end

  class PrintingNotifier
    def initialize(out)
      @out = out
    end

    def executing_command(cmd, pid)
      @out << "$> #{cmd} (#{pid})\n"
    end

    def command_output(pid, chunk)
      @out << chunk
    end

    def command_running(pid)
      @out << "\n....still running....\n"
    end

    def executed_command(cmd, pid, status)
      @out << "   [exit=#{status.exitstatus}]\n"
    end
  end

  class NullNotifier
    def executing_command(cmd, pid)
    end

    def command_output(pid, chunk)
    end

    def command_running(pid)
    end

    def executed_command(cmd, pid, status)
    end
  end

  if RUBY_ENGINE == 'jruby'
    class Runner
      def initialize(ccm_script, env, notifier)
        @cmd      = Cliver.detect!('ccm')
        @env      = env
        @notifier = notifier
      end

      def exec(*args)
        cmd = args.dup.unshift(@cmd).join(' ')
        pid = nil
        out = ''

        IO.popen([@env, @cmd, *args]) do |io|
          pid = io.pid
          @notifier.executing_command(cmd, pid)

          loop do
            begin
              Timeout.timeout(30) do
                out << chunk = io.readpartial(4096)

                @notifier.command_output(pid, chunk)
              end
            rescue Timeout::Error
              @notifier.command_running(pid)
            rescue EOFError
              break
            end
          end
        end

        @notifier.executed_command(cmd, pid, $?)
        raise "#{cmd} failed" unless $?.success?

        out
      end
    end
  else
    class Runner
      def initialize(ccm_script, env, notifier)
        @ccm_script  = ccm_script
        @env         = env
        @notifier    = notifier
        @python_path = Cliver.detect!('python', '~> 2.7', detector: /(?<=Python )[0-9][.0-9a-z]+/)
        start
      end

      def exec(*args)
        cmd = args.dup.unshift('ccm').join(' ')
        out = ''
        done = false

        @notifier.executing_command(cmd, @pid)

        begin
          @stdin.write(encode(args))
        rescue ::Errno::EPIPE
          output = @stdout.read rescue ''
          @notifier.command_output(@pid, output) unless output.empty?

          stop
          start

          raise
        end

        until done
          if IO.select([@stdout], nil, nil, 30)
            begin
              chunk = @stdout.read_nonblock(4096)

              if chunk.end_with?("\x01")
                chunk.chomp!("\x01")
                done = true
              end

              unless chunk.empty?
                out << chunk
                @notifier.command_output(@pid, chunk)
              end
            rescue IO::WaitReadable
            rescue EOFError
              stop
              start

              raise "#{cmd} failed"
            end
          else
            @notifier.command_running(@pid)
          end
        end

        out
      end

      private

      def start
        return if @stdin && @stdout && @pid

        in_r, @stdin = IO.pipe
        @stdout, out_w = IO.pipe

        if in_r.respond_to?(:set_encoding)
          in_r.set_encoding('binary')
          @stdin.set_encoding('binary')
          @stdout.set_encoding('binary')
          out_w.set_encoding('binary')
        end

        out_w.sync  = true
        @stdin.sync = true

        @pid = Process.spawn(
          @env,
          @python_path, '-u', @ccm_script,
          {
            :in => in_r,
            [:out, :err] => out_w
          }
        )

        @stdout.read(1)

        in_r.close
        out_w.close
      end

      def stop
        @stdin.close
        @stdout.close
        Process.waitpid(@pid)

        @stdin  = nil
        @stdout = nil
        @pid    = nil
      end

      def encode(args)
        body = JSON.dump(args)
        size = body.bytesize
        [size, body].pack("S!A#{size}")
      end
    end
  end

  if OS.linux?
    class Firewall
      def block(ip)
        $stderr.puts "Blocking #{ip}..."
        success = system('sudo', 'iptables', '-A', 'INPUT', '-d', ip, '-p', 'tcp', '-m', 'multiport', '--dport', '9042,7000,7001', '-j', 'DROP')
        raise "failed to block #{ip}" unless success
        success = system('sudo', 'iptables', '-A', 'OUTPUT', '-s', ip, '-p', 'tcp', '-m', 'multiport', '--dport', '9042,7000,7001', '-j', 'DROP')
        raise "failed to block #{ip}" unless success

        nil
      end

      def unblock(ip)
        $stderr.puts "Unblocking #{ip}..."
        success = system('sudo', 'iptables', '-D', 'INPUT', '-d', ip, '-p', 'tcp', '-m', 'multiport', '--dport', '9042,7000,7001', '-j', 'DROP')
        raise "failed to unblock #{ip}" unless success
        success = system('sudo', 'iptables', '-D', 'OUTPUT', '-s', ip, '-p', 'tcp', '-m', 'multiport', '--dport', '9042,7000,7001', '-j', 'DROP')
        raise "failed to unblock #{ip}" unless success

        nil
      end
    end
  elsif OS.mac?
    class Firewall
      def initialize
        @ready = false
      end

      def block(ip)
        prepare unless @ready

        $stderr.puts "Blocking #{ip}..."
        success = system('sudo', 'pfctl', '-t', '_ruby_driver_test_blocklist_', '-T', 'add', "#{ip}/32", err: '/dev/null')
        raise "failed to block #{ip}" unless success

        nil
      end

      def unblock(ip)
        prepare unless @ready

        $stderr.puts "Unblocking #{ip}..."
        success = system('sudo', 'pfctl', '-t', '_ruby_driver_test_blocklist_', '-T', 'delete', "#{ip}/32", err: '/dev/null')
        raise "failed to unblock #{ip}" unless success

        nil
      end

      private

      def prepare
        $stderr.puts "Checking if '_ruby_driver_test_blocklist_' table is present in pf.conf"
        if `sudo pfctl -s rules 2>/dev/null | grep 'block drop proto tcp from any to <_ruby_driver_test_blocklist_> port = 9042'`.chomp.empty?
          $stderr.puts "Ruby driver tests need to modify pf.conf to be able to simulate network partitions"
          success = system('sudo bash -c "echo \'block drop proto tcp from any to <_ruby_driver_test_blocklist_> port {9042, 7000, 7001}\' >> /etc/pf.conf"')
          abort "Unable to add rule to block _ruby_driver_test_blocklist_ table to /etc/pf.conf" unless success
          success = system('sudo bash -c "echo \'block drop proto tcp from <_ruby_driver_test_blocklist_> to any port {9042, 7000, 7001}\' >> /etc/pf.conf"')
          abort "Unable to add rule to block _ruby_driver_test_blocklist_ table to /etc/pf.conf" unless success
          $stderr.puts "Starting PF firewall"
          system('sudo pfctl -ef /etc/pf.conf 2>/dev/null')
        end

        @ready = true
      end
    end
  elsif OS.windows?
    abort "Cannot run test on Windows, due to lack of firewall support for simulating network partitions"
  end

  class Cluster
    class Node
      attr_reader :name, :status

      def initialize(name, status, cluster)
        @name    = name
        @status  = status
        @cluster = cluster
      end

      def stop
        return if @status == 'DOWN'
        @cluster.stop_node(@name)
        @status = 'DOWN'
        nil
      end

      def start
        return if @status == 'UP'
        @cluster.start_node(@name)
        @status = 'UP'
        nil
      end

      def decommission
        @cluster.decommission_node(@name)
        nil
      end

      def remove
        stop
        @cluster.remove_node(@name)
        nil
      end

      def up!
        @status = 'UP'
        nil
      end

      def down!
        @status = 'DOWN'
        nil
      end

      def up?
        @status == 'UP'
      end

      def down?
        @status == 'DOWN'
      end
    end

    attr_reader :name

    def initialize(name, ccm, firewall, nodes_count = nil, datacenters = nil, keyspaces = nil, dse = false)
      @name        = name
      @ccm         = ccm
      @firewall    = firewall
      @datacenters = datacenters
      @keyspaces   = keyspaces
      @dse = dse

      @nodes = []

      (1..nodes_count).each do |i|
        @nodes << Node.new("node#{i}", 'DOWN', self)
      end if nodes_count

      @blocked = ::Set.new
    end

    def running?
      @nodes.any?(&:up?)
    end

    def stop
      return if @nodes.all?(&:down?)

      if @cluster
        @cluster.close
        @cluster = @session = nil
      end

      @ccm.exec('stop')
      refresh_status

      nil
    end

    def start(jvm_arg=nil)
      if @cluster
        unless jvm_arg
          return if @nodes.all?(&:up?) && @cluster.hosts.select(&:up?).count == @nodes.size
        end

        @cluster.close
        @cluster = @session = nil
      end

      options = { :logger             => logger,
                  :consistency        => :all,
                  :synchronize_schema => false,
                  :idempotent         => true,
                  :timeout            => nil,
                  :heartbeat_interval => nil,
                  :idle_timeout       => nil }

      if @username && @password
        options[:username] = @username
        options[:password] = @password
      end

      if @ads_server && !@username
        ENV['KRB5CCNAME']='cassandra.cache'
        options[:auth_provider] = Dse::Auth::Providers::GssApi.new('dse', true, 'cassandra@DATASTAX.COM')
      end

      if @server_cert
        options[:server_cert] = @server_cert
      end

      if @client_cert
        options[:client_cert] = @client_cert
      end

      if @private_key
        options[:private_key] = @private_key
      end

      if @passphrase
        options[:passphrase] = @passphrase
      end

      options[:load_balancing_policy] = SameOrderLoadBalancingPolicy.new

      until @nodes.all?(&:up?) && @cluster && @cluster.hosts.select(&:up?).count == @nodes.size
        attempts = 1

        begin
          @ccm.exec('start', '--wait-for-binary-proto', jvm_arg ? "--jvm_arg=#{jvm_arg}" : '')
          refresh_status
        rescue => e
          @ccm.exec('stop') rescue nil

          raise e if attempts >= 20

          wait = attempts * 2
          $stderr.puts "#{e.class.name}: #{e.message}, retrying in #{wait}s..."
          attempts += 1
          sleep(wait)
          retry
        end

        attempts = 1

        begin
          @cluster = Dse.cluster(options)
        rescue => e
          refresh_status
          next unless @nodes.all?(&:up?)
          raise e if attempts >= 20

          wait = attempts * 2
          $stderr.puts "#{e.class.name}: #{e.message}, retrying in #{wait}s..."
          attempts += 1
          sleep(wait)
          retry
        end

        until @cluster.hosts.all?(&:up?)
          $stderr.puts "not all hosts are up yet, retrying in 1s..."
          sleep(1)
        end
      end

      $stderr.puts "creating session"
      @session = @cluster.connect

      nil
    end

    def restart
      stop
      start
    end

    def start_node(name, jvm_arg=nil)
      node = @nodes.find {|n| n.name == name}
      raise "unknown node #{name.inspect}" unless node

      i  = name.sub('node', '')
      ip = "127.0.0.#{i}"

      until node.up?
        attempts = 1

        begin
          @ccm.exec(node.name, 'start', '--wait-other-notice', '--wait-for-binary-proto',
                    jvm_arg ? "--jvm_arg=#{jvm_arg}" : '')
          refresh_status
        rescue => e
          @ccm.exec(node.name, 'stop') rescue nil

          if attempts >= 20
            raise e
          else
            wait = attempts * 2
            $stderr.puts "#{e.class.name}: #{e.message}, retrying in #{wait}s..."
            attempts += 1
            sleep(wait)
            retry
          end
        end

        if @cluster
          attempts = 1

          until @cluster.has_host?(ip) && @cluster.host(ip).up?
            refresh_status

            break if node.down?

            if attempts >= 20
              @ccm.exec(node.name, 'stop')
              refresh_status
              break
            end

            wait = attempts * 2
            $stderr.puts "did not receive node up event for #{node.name.inspect}, retrying in #{wait}s..."
            attempts += 1
            sleep(wait)
          end
        end
      end

      nil
    end

    def stop_node(name)
      node = @nodes.find {|n| n.name == name}
      return if node.nil? || node.down?
      @ccm.exec(node.name, 'stop')
      node.down!

      nil
    end

    def remove_node(name)
      node = @nodes.find {|n| n.name == name}
      return if node.nil?
      @ccm.exec(node.name, 'stop')
      @ccm.exec(node.name, 'remove')
      node.down!
      @nodes.delete(node)

      nil
    end

    def decommission_node(name)
      node = @nodes.find {|n| n.name == name}
      return if node.nil?
      @ccm.exec(node.name, 'decommission')

      nil
    end

    def add_node(name)
      return if @nodes.any? {|n| n.name == name}

      i = name.sub('node', '')

      add_args = ['-b', "-t 127.0.0.#{i}:9160", "-l 127.0.0.#{i}:7000", "--binary-itf=127.0.0.#{i}:9042", name]
      add_args << '--dse' if @dse

      @ccm.exec('add', *add_args)
      @nodes << Node.new(name, 'DOWN', self)

      nil
    end

    def block_node(name)
      node = @nodes.find {|n| n.name == name}
      return if node.nil?
      return if @blocked.include?(name)

      @ccm.exec(node.name, 'pause')
      @blocked.add(name)

      nil
    end

    def block_nodes
      @nodes.each do |node|
        block_node(node.name)
      end
    end

    def unblock_nodes
      @blocked.each do |name|
        node = @nodes.find {|n| n.name == name}
        return if node.nil?

        @ccm.exec(node.name, 'resume')
      end
      @blocked.clear

      nil
    end

    def datacenters_count
      @datacenters ||= begin
        @cluster.hosts.group_by(&:datacenter).size
      end
    end

    def nodes_count
      @nodes.size
    end

    def enable_authentication
      stop
      @username = 'cassandra'
      @password = 'cassandra'
      @ccm.exec('updateconf', 'authenticator: PasswordAuthenticator')
      start

      [@username, @password]
    end

    def disable_authentication
      stop
      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      @username = @password = nil
      start
    end

    def enable_dse_authentication
      stop
      @username = 'cassandra'
      @password = 'cassandra'
      @ccm.exec('updateconf', 'authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator')
      @ccm.exec('updatedseconf',
                'authentication_options.default_scheme: internal',
                'authentication_options.enabled: true'
      )
      start

      [@username, @password]
    end

    def disable_dse_authentication
      stop
      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      @ccm.exec('updatedseconf', 'authentication_options.enabled: false')
      @username = @password = nil
      start
    end

    def enable_kerberos
      stop

      # Start the embedded-ads jar
      ads_dir = '/home/jenkins'
      @ads_server = IO.popen("java -jar #{ads_dir}/embedded-ads.jar -k")
      puts 'The embedded-ads server has been started for Kerberos'
      sleep(5)

      # Set env vars
      ads_config_dir = `echo $WORKSPACE`.chomp
      dse_keytab = "#{ads_config_dir}/dse.keytab"
      krb5_config = "#{ads_config_dir}/krb5.conf"
      ENV['DSE_KEYTAB'] = dse_keytab
      ENV['KRB5_CONFIG'] = krb5_config

      # Generate the user files
      `kinit -t #{ads_config_dir}/dseuser.keytab -k dseuser@DATASTAX.COM -c dseuser.cache`
      `kinit -t #{ads_config_dir}/cassandra.keytab -k cassandra@DATASTAX.COM -c cassandra.cache`

      # Update DSE configs
      @ccm.exec('updatedseconf',
                "kerberos_options.keytab: #{dse_keytab}",
                'kerberos_options.service_principal: dse/_HOST@DATASTAX.COM',
                'kerberos_options.http_principal: dse/_HOST@DATASTAX.COM',
                'kerberos_options.qop: auth'
      )

      if CCM.dse_version < '5.0.0'
        @ccm.exec('updateconf', 'authenticator: com.datastax.bdp.cassandra.auth.KerberosAuthenticator')
      else
        @ccm.exec('updateconf', 'authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator')
        @ccm.exec('updatedseconf',
                  'authentication_options.enabled: true',
                  'authentication_options.default_scheme: kerberos',
                  'authentication_options.scheme_permissions: true',
                  'authentication_options.allow_digest_with_kerberos: true',
                  'authentication_options.transitional_mode: disabled'
        )
      end

      # Start DSE with Kerberos
      start("-Djava.security.krb5.conf=#{krb5_config}")
    end

    def disable_kerberos
      stop
      Process.kill('INT', @ads_server.pid)
      Process.waitpid(@ads_server.pid)
      puts 'The embedded-ads server with Kerberos has been stopped'
      @ads_server = nil

      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      @ccm.exec('updatedseconf', 'authentication_options.enabled: false') unless CCM.dse_version < '5.0.0'
      start
    end

    def enable_ldap
      stop

      @username = 'cassandra'
      @password = 'cassandra'

      # Start the embedded-ads jar
      ads_dir = '/home/jenkins'
      @ads_server = IO.popen("java -jar #{ads_dir}/embedded-ads.jar")
      puts 'The embedded-ads server has been started for LDAP'
      sleep(5)

      # Update DSE configs
      @ccm.exec('updatedseconf',
                'ldap_options.search_dn: uid=cassandra,ou=users,dc=datastax,dc=com',
                'ldap_options.search_password: cassandra',
                'ldap_options.server_host: 127.0.0.1',
                'ldap_options.server_port: 10389',
                'ldap_options.user_search_base: ou=users,dc=datastax,dc=com',
                'ldap_options.user_search_filter: (uid={0})'
      )

      if CCM.dse_version < '5.0.0'
        @ccm.exec('updateconf', 'authenticator: com.datastax.bdp.cassandra.auth.LdapAuthenticator')
      else
        @ccm.exec('updateconf', 'authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator')
        @ccm.exec('updatedseconf',
                  'authentication_options.enabled: true',
                  'authentication_options.default_scheme: ldap'
        )
      end

      # Start DSE
      start

      [@username, @password]
    end

    def disable_ldap
      stop
      Process.kill('INT', @ads_server.pid)
      Process.waitpid(@ads_server.pid)
      puts 'The embedded-ads server with LDAP has been stopped'
      @ads_server = nil

      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      @ccm.exec('updatedseconf', 'authentication_options.enabled: false') unless CCM.dse_version < '5.0.0'
      @username = @password = nil
      start
    end

    def enable_ssl
      stop
      ssl_root = File.expand_path(File.dirname(__FILE__) + '/../support/ssl')
      @ccm.exec('updateconf',
        'client_encryption_options.enabled: true',
        "client_encryption_options.keystore: #{ssl_root}/.keystore",
        'client_encryption_options.keystore_password: ruby-driver'
      )
      @server_cert = ssl_root + '/cassandra.pem'
      start
      @server_cert
    end

    def enable_ssl_client_auth
      stop
      ssl_root = File.expand_path(File.dirname(__FILE__) + '/../support/ssl')
      @ccm.exec('updateconf',
        'client_encryption_options.enabled: true',
        "client_encryption_options.keystore: #{ssl_root}/.keystore",
        'client_encryption_options.keystore_password: ruby-driver',
        'client_encryption_options.require_client_auth: true',
        "client_encryption_options.truststore: #{ssl_root}/.truststore",
        'client_encryption_options.truststore_password: ruby-driver'
      )
      @server_cert = ssl_root + '/cassandra.pem'
      @client_cert = ssl_root + '/driver.pem'
      @private_key = ssl_root + '/driver.key'
      @passphrase  = 'ruby-driver'
      start
      [@server_cert, @client_cert, @private_key, @passphrase]
    end

    def disable_ssl
      stop
      @ccm.exec('updateconf', 'client_encryption_options.enabled: false')
      @server_cert = nil
      @client_cert = nil
      @private_key = nil
      @passphrase  = nil
      start
    end

    def change_tombstone_thresholds
      stop
      @ccm.exec('updateconf',
                'tombstone_failure_threshold: 2000',
                'tombstone_warn_threshold: 1000'
      )
      start
    end

    def enable_graph
      stop
      @ccm.exec('setworkload', 'graph')
      start('-Dcassandra.ignore_dc=true')
    end

    def disable_graph
      stop
      @ccm.exec('setworkload', 'cassandra')
      start('-Dcassandra.ignore_dc=true')
    end

    def setup_schema(schema)
      schema.strip!
      schema.chomp!(";")
      statements = schema.split(";\n")

      Retry.with_attempts(5) do
        start
        @session.execute("USE system")

        if @cluster.hosts.sample.release_version >= '3.0'
          rows = @session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        else
          rows = @session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
        end

        rows.each do |row|
          next if row['keyspace_name'].start_with?('system') || row['keyspace_name'].start_with?('dse')
          @session.execute("DROP KEYSPACE #{row['keyspace_name']}")
        end

        statements.each do |statement|
          begin
            @session.execute(statement)
          rescue Cassandra::Errors::AlreadyExistsError
          end
        end

        @session.execute("USE system")
      end
    rescue Cassandra::Errors::NoHostsAvailable => e
      if e.errors.first.last.is_a?(Cassandra::Errors::ServerError)
        $stderr.puts "#{e.class.name}: #{e.message}, retrying..."
        retry
      end

      raise
    end

    def setup_graph_schema(schema, graph_name)
      Retry.with_attempts(5) do
        start
        @session.execute_graph(schema, graph_name: graph_name, graph_write_consistency: :all)
      end
    rescue Cassandra::Errors::NoHostsAvailable => e
      if e.errors.first.last.is_a?(Cassandra::Errors::ServerError)
        $stderr.puts "#{e.class.name}: #{e.message}, retrying..."
        retry
      end

      raise
    end

    def execute(cql)
      start

      cql.strip!
      cql.chomp!(";")
      cql.split(";\n").each do |statement|
        Retry.with_attempts(5) do
          begin
            @session.execute(statement)
          rescue Cassandra::Errors::AlreadyExistsError
          end
        end
      end

      @session.execute("USE system")

      nil
    end

    def refresh_status
      seen = ::Set.new
      @ccm.exec('status').each_line do |line|
        line.strip!
        next if line.start_with?('Cluster: ')
        name, status = line.split(": ")
        next if status.nil?
        node = @nodes.find {|n| n.name == name}

        if node
          if status == 'UP'
            node.up!
          else
            node.down!
          end
        else
          @nodes << node = Node.new(name, status, self)
        end

        seen << node
      end

      @nodes.select! {|n| seen.include?(n)}
      nil
    end

    def execute_cqlsh(statement)
      node = @nodes.find(&:up?)
      raise "no nodes running" unless node
      @ccm.exec(node.name, 'cqlsh', '-v', '-x', statement)
    end

    private

    def logger
      @logger ||= begin
        logger = Logger.new($stderr)
        logger.level = Logger::DEBUG
        logger.formatter = proc { |severity, time, progname, message|
          "Cluster:0x#{object_id.to_s(16)} | #{time.strftime("%T,%L")} - [#{severity}] #{message}\n"
        }
        logger
      end
    end
  end

  @raw_version = nil
  @cassandra_version = nil
  @dse = false

  def parse_version
    @raw_version ||= begin
      version = ENV['CASSANDRA_VERSION'] || '3.0.1'
      if ENV['DSE_VERSION']
        @dse = true
        version = ENV['DSE_VERSION']
      end
      version
    end
  end

  def cassandra_version
    parse_version
    @cassandra_version ||= begin
      version = @raw_version
      if @raw_version.start_with?('4.0') || @raw_version.start_with?('4.5') || @raw_version.start_with?('4.6')
        version = '2.0.17'
      elsif @raw_version.start_with?('4.7') || @raw_version.start_with?('4.8')
        version = '2.1.12'
      elsif @raw_version.start_with?('5.0')
        version = '3.0.5'
      end
      version
    end
  end

  def dse_version
    parse_version
    @raw_version
  end

  def setup_graph_cluster(no_dc = 1, no_nodes_per_dc = 3)
    setup_cluster(no_dc, no_nodes_per_dc, true)
  end

  def setup_spark_cluster(no_dc = 1, no_nodes_per_dc = 2)
    setup_cluster(no_dc, no_nodes_per_dc, true, true)
  end

  def setup_cluster(no_dc = 1, no_nodes_per_dc = 3, enable_graph = false, enable_spark = false)
    parse_version

    if enable_spark
      cluster_name = 'ruby-driver-dse_spark' + "-#{@raw_version}" + '-test-cluster'
    elsif enable_graph
      cluster_name = 'ruby-driver-dse_graph' + "-#{@raw_version}" + '-test-cluster'
    else
      cluster_name = 'ruby-driver-' + "#{@dse ? 'dse' : 'cassandra'}" + "-#{@raw_version}" + '-test-cluster'
    end

    if @current_cluster && @current_cluster.name == cluster_name
      unless @current_cluster.nodes_count == (no_dc * no_nodes_per_dc) && @current_cluster.datacenters_count == no_dc
        @current_cluster.stop
        remove_cluster(@current_cluster.name)
        create_cluster(cluster_name, @raw_version, no_dc, no_nodes_per_dc, enable_graph, enable_spark)
      end

      @current_cluster.start
      return @current_cluster
    end

    if cluster_exists?(cluster_name)
      switch_cluster(cluster_name)

      unless @current_cluster.nodes_count == (no_dc * no_nodes_per_dc) && @current_cluster.datacenters_count == no_dc
        @current_cluster.stop
        remove_cluster(@current_cluster.name)
        create_cluster(cluster_name, @raw_version, no_dc, no_nodes_per_dc, enable_graph, enable_spark)
      end
    else
      @current_cluster && @current_cluster.stop
      create_cluster(cluster_name, @raw_version, no_dc, no_nodes_per_dc, enable_graph, enable_spark)
    end

    @current_cluster.start
    @current_cluster
  end

  private

  def ccm
    @ccm ||= begin
      Runner.new(ccm_script, {
                 'MALLOC_ARENA_MAX'  => '1'},
                 PrintingNotifier.new($stderr))
    end
  end

  def firewall
    @firewall ||= Firewall.new
  end

  def ccm_home
    @ccm_home ||= begin
      ccm_home = File.expand_path(File.dirname(__FILE__) + '/../tmp')
      FileUtils.mkdir_p(ccm_home) unless File.directory?(ccm_home)
      ccm_home
    end
  end

  def ccm_script
    @ccm_script ||= File.expand_path(File.dirname(__FILE__) + '/ccm.py')
  end

  def switch_cluster(name)
    if @current_cluster
      if @current_cluster.name == name
        @current_cluster.start
        return nil
      end
      @current_cluster.stop
    end

    @current_cluster = clusters.find {|c| c.name == name}
    return unless @current_cluster

    ccm.exec('switch', @current_cluster.name)

    @current_cluster.start

    nil
  end

  def remove_cluster(name)
    cluster = clusters.find {|c| c.name == name}
    return unless cluster
    ccm.exec('remove', cluster.name)
    clusters.delete(cluster)

    nil
  end

  def create_cluster(name, version, datacenters, nodes_per_datacenter, enable_graph, enable_spark)
    nodes = Array.new(datacenters, nodes_per_datacenter).join(':')

    if @dse && ENV['DSE_DIR'] && !ENV['DSE_DIR'].empty?
      ccm.exec('create', name, '--dse', '--install-dir', ENV['DSE_DIR'])
    else
      create_args = ['-v', version, name]
      create_args << '--dse' if @dse
      ccm.exec('create', *create_args)
    end

    config = [
      '--rt', '1000',
      'read_request_timeout_in_ms: 1000',
      'write_request_timeout_in_ms: 1000',
      'request_timeout_in_ms: 1000',
      'phi_convict_threshold: 16',
      'hinted_handoff_enabled: false',
      'dynamic_snitch_update_interval_in_ms: 1000'
    ]

    if cassandra_version.start_with?('1.2.')
      config << 'reduce_cache_sizes_at: 0'
      config << 'reduce_cache_capacity_to: 0'
      config << 'flush_largest_memtables_at: 0'
      config << 'index_interval: 512'
    else
      config << 'cas_contention_timeout_in_ms: 10000'
      config << 'file_cache_size_in_mb: 0'
    end

    # config << 'native_transport_max_threads: 1'
    config << 'rpc_min_threads: 1'
    config << 'rpc_max_threads: 1'
    config << 'concurrent_reads: 2'
    config << 'concurrent_writes: 2'
    config << 'concurrent_compactors: 1'
    config << 'compaction_throughput_mb_per_sec: 0'

    if cassandra_version < '2.1'
      config << 'in_memory_compaction_limit_in_mb: 1'
    end

    if cassandra_version > '2.2'
      config << 'enable_user_defined_functions: true'
    end

    if cassandra_version > '3.0'
      config << 'enable_scripted_user_defined_functions: true'
    end

    config << 'key_cache_size_in_mb: 0'
    config << 'key_cache_save_period: 0'
    config << 'memtable_flush_writers: 1'
    config << 'max_hints_delivery_threads: 1'

    ccm.exec('updateconf', *config)
    ccm.exec('populate', '-n', nodes, '-i', '127.0.0.')

    if enable_spark
      ccm.exec('setworkload', 'graph,spark')
    elsif enable_graph
      ccm.exec('setworkload', 'graph')
    end

    clusters << @current_cluster = Cluster.new(name, ccm, firewall, nodes_per_datacenter * datacenters, datacenters,
                                               [], @dse)

    if enable_spark
      configure_spark(nodes_per_datacenter)
    end

    nil
  end

  def configure_spark(num_nodes)
    @current_cluster.start_node('node1')

    start = Time.now
    loop do
      begin
        s = TCPSocket.new('127.0.0.1', 7080)
        s.close
        break
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
        elapsed_time = Time.now - start
        if elapsed_time > 60
          raise RuntimeError, 'Spark master did not come up after 60 seconds'
        end
        puts "Spark master not ready yet after #{elapsed_time} seconds"
        sleep(5)
      end
    end

    begin
      cluster = Dse.cluster
      session = cluster.connect
      statement = "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'GraphAnalytics': '#{num_nodes}'}"
      session.execute(statement)
    ensure
      cluster.close
    end

    @current_cluster.start

    start = Time.now
    loop do
      uri = URI.parse('http://127.0.0.1:7080/')
      response_body = Net::HTTP.get_response(uri).body
      workers_up = response_body.match(/Alive Workers:.*(\d+)<\/li>/)[1].to_i
      break if workers_up == num_nodes
      if Time.now - start > 120
        raise RuntimeError, "Spark workers did not come up after 120 seconds. Currently have: #{workers_up}"
      end
      sleep(2)
    end
  end

  def update_conf
    ccm.exec('updateconf')
    nil
  end

  def clusters
    @clusters ||= begin
      ccm.exec('list').split("\n").map! do |name|
        name.strip!
        current = name.start_with?('*')
        name.sub!('*', '')
        cluster = Cluster.new(name, ccm, firewall)

        if current
          @current_cluster = cluster
          @current_cluster.refresh_status
        end

        cluster
      end
    end
  end

  def cluster_exists?(name)
    clusters.any? {|cluster| cluster.name == name}
  end

  def clear
    instance_variables.each do |ivar|
      remove_instance_variable(ivar)
    end

    nil
  end

  def stop_and_reset
    @current_cluster.stop
    @current_cluster = nil
  end
end

if __FILE__ == $0
  require 'bundler/setup'
  require 'cassandra'

  CCM.setup_cluster(1, 3)
end
