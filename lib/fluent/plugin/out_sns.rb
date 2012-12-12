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
        config_param :sns_subject, :string, :default => nil
        config_param :sns_endpoint, :string, :default => 'sns.ap-northeast-1.amazonaws.com'
        
        def configure(conf)
            super
        end
        
        def start
            super
            AWS.config(
                       :access_key_id => @aws_key_id,
                       :secret_access_key => @aws_sec_key,
                       :sns_endpoint => @sns_endpoint )
            
            @sns = AWS::SNS.new
            @topic = get_topic
        end
        
        def shutdown
            super
        end
        
        def emit(tag, es, chain)
            chain.next
            es.each {|time,record|
                record["time"] = Time.at(time).localtime
                subject = record[@sns_subject_key] || @sns_subject  || 'Fluent-Notification'
                @topic.publish(record.to_json, :subject => subject )
            }
        end
        
        def get_topic()
            @sns.topics.each do |topic|
                if @sns_topic_name == topic.name
                    return topic
                end
            end
        end
    end
end
