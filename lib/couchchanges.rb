require "em-http"
require "uri"
require "json"

class CouchChanges
  def initialize options={}
    @options = options.dup
    @uri = URI.parse(@options.delete(:url) + "/_changes")
    @last_seq = options[:since] || 0
  end

  def change &block
    block ? @change = block : @change
  end

  def update &block
    block ? @update = block : @update
  end

  def delete &block
    block ? @delete = block : @delete
  end

  def disconnect &block
    block ? @disconnect = block : @disconnect
  end

  def listen
    @http = http!
    buffer = ""
    @http.stream  {|chunk|
      buffer += chunk
      while line = buffer.slice!(/.+\r?\n/)
        handle line
      end
    }
    @http.errback { disconnected }
    @http
  end

  # REFACTOR!
  def http!
    options = {
      :timeout => 0,
      :query   => @options.merge({:feed => "continuous"})
    }

    EM::HttpRequest.new(@uri.to_s).get(options)
  end

  def handle line
    return if line.chomp.empty?

    hash = JSON.parse(line)
    if hash["last_seq"]
      disconnected
    else
      if hash["changes"].blank? || hash["changes"][0].blank?
        File.open("/home/ubuntu/couchchanged_debug.log", 'a') do |f|
          f.puts hash.inspect
        end
      else
        hash["rev"] = hash.delete("changes")[0]["rev"]
      end
      @last_seq = hash["seq"]

      callbacks hash
    end
  end

  def disconnected
    if @disconnect
      @disconnect.call @last_seq
    else
      EM.add_timer(@options[:reconnect]) {
        @options[:since] = @last_seq
        listen
      }
    end
  end

  def callbacks hash
    @change.call hash if @change
    if hash["deleted"]
      @delete.call hash if @delete
    else
      @update.call hash if @update
    end
  end
end
