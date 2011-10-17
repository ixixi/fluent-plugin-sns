module Fluent
    
    require 'aws-sdk'
    
    class SNSOutput < Output
        
        Fluent::Plugin.register_output('sns', self)

        include SetTagKeyMixin
        config_set_default :include_tag_key, false

        include SetTimeKeyMixin
        config_set_default :include_time_key, true
        
        config_param :aws_key_id, :string
        config_param :aws_sec_key, :string

        config_param :sns_topic_name, :string
        config_param :sns_subject_key, :string, :default => nil
        config_param :sns_subject, :stringtring, :default => nil

        def configure(conf)
            super
        end

        def start
            super
            @sns = AWS::SNS.new(
                :access_key_id => @aws_key_id,
                :secret_access_key => @aws_sec_key )
            @topic = @sns.topics.create(@sns_topic_name, :subject => @subject)
        end

        def shutdown
            super
        end

        def emit(tag, es, chain)
            chain.next
            es.each {|record|
                subject = record[@sns_subject_key] || @sns_subject  || 'Fluent-Notification'
                msg = @topic.publish(record, :subject => subject )
                $stderr.puts "published topic: #{msg}"
            }
        end

    end
end
