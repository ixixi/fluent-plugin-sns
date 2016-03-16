module Fluent

  require 'aws-sdk-v1'

  class SNSOutput < Output

    Fluent::Plugin.register_output('sns', self)

    include SetTagKeyMixin
    config_set_default :include_tag_key, false

    include SetTimeKeyMixin
    config_set_default :include_time_key, true

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true

    config_param :sns_topic_name, :string
    config_param :sns_subject_template, :default => nil
    config_param :sns_subject_key, :string, :default => nil
    config_param :sns_subject, :string, :default => nil
    config_param :sns_body_template, :default => nil
    config_param :sns_body_key, :string, :default => nil
    config_param :sns_body, :string, :default => nil
    config_param :sns_endpoint, :string, :default => 'sns.ap-northeast-1.amazonaws.com'
    config_param :proxy, :string, :default => ENV['HTTP_PROXY']

    def configure(conf)
      super
    end

    def start
      super
      options = {}
      options[:sns_endpoint] = @sns_endpoint
      options[:proxy_uri] = @proxy
      if @aws_key_id && @aws_sec_key
        options[:access_key_id] = @aws_key_id
        options[:secret_access_key] = @aws_sec_key
      end
      AWS.config(options)

      @sns = AWS::SNS.new
      @topic = @sns.topics.find{|topic| @sns_topic_name == topic.name}

      @subject_template = nil
      unless @sns_subject_template.nil?
        template_file = open(@sns_subject_template)
        @subject_template = ERB.new(template_file.read)
        template_file.close
      end

      @body_template = nil
      unless @sns_body_template.nil?
        template_file = open(@sns_body_template)
        @body_template = ERB.new(template_file.read)
        template_file.close
      end
    end

    def shutdown
      super
    end

    def emit(tag, es, chain)
      chain.next
      es.each {|time,record|
        record['time'] = Time.at(time).localtime
        body = get_body(record).to_s.force_encoding('UTF-8')
        subject = get_subject(record).to_s.force_encoding('UTF-8').gsub(/(\r\n|\r|\n)/, '')
        @topic.publish( body, :subject => subject )
      }
    end

    def get_subject(record)
      unless @subject_template.nil?
        return @subject_template.result(binding)
      end
      subject = record[@sns_subject_key] || @sns_subject || 'Fluentd-Notification'
    end

    def get_body(record)
      unless @body_template.nil?
        return @body_template.result(binding)
      end
      record[@sns_body_key] || @sns_body || record.to_json
    end
  end
end
