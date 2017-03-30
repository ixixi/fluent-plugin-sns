module Fluent

  require 'aws-sdk'

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
    config_param :sns_endpoint, :string, :default => 'sns.ap-northeast-1.amazonaws.com',
                 :obsoleted => 'Use sns_region instead'
    config_param :sns_region, :string, :default => 'ap-northeast-1'
    config_param :proxy, :string, :default => ENV['HTTP_PROXY']

    def configure(conf)
      super
    end

    def start
      super
      options = {}
      options[:region] = @sns_region
      options[:http_proxy] = @proxy
      if @aws_key_id && @aws_sec_key
        options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key)
      end
      Aws.config.update(options)

      @sns = Aws::SNS::Resource.new
      @topic = @sns.topics.find{|topic| @sns_topic_name == topic.arn.split(":")[-1]}
      if @topic.nil?
        raise ConfigError, "No topic found for topic name #{@sns_topic_name}."
      end

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

        @topic.publish({
          message: body,
          subject: subject,
        })
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
