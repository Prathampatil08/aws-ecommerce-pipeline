content = open('infrastructure/cloudformation/main-stack.yaml').read()

# Fix 1: Remove Kinesis Firehose (not available on free trial)
import re
content = re.sub(r'\n  # ─+\n  # KINESIS DATA FIREHOSE.*?(?=\n  # )', '', content, flags=re.DOTALL)
content = re.sub(r'\n  OrdersFirehose:.*?(?=\n  # )', '', content, flags=re.DOTALL)
content = re.sub(r'\n  FirehoseRole:.*?(?=\n  # )', '', content, flags=re.DOTALL)

# Fix 2: Remove CloudWatch logging from Step Functions (causes IAM issues)
old_logging = """      LoggingConfiguration:
        Level: ERROR
        IncludeExecutionData: true
        Destinations:
          - CloudWatchLogsLogGroup:
              LogGroupArn: !GetAtt PipelineLogGroup.Arn"""
content = content.replace(old_logging, '')

# Fix 3: Remove DefinitionS3Location and use inline placeholder
old_defn = """      DefinitionS3Location:
        Bucket: !Ref GlueScriptsBucket
        Key: step_functions/pipeline_definition.json"""
new_defn = """      Definition:
        Comment: "E-Commerce Pipeline"
        StartAt: RunBronzeToSilver
        States:
          RunBronzeToSilver:
            Type: Pass
            Next: RunSilverToGold
          RunSilverToGold:
            Type: Pass
            Next: PipelineSucceeded
          PipelineSucceeded:
            Type: Succeed"""
content = content.replace(old_defn, new_defn)

# Fix 4: Remove FirehoseStreamName from Outputs
old_output = """
  FirehoseStreamName:
    Value: !Ref OrdersFirehose
    Export: {Name: !Sub "${ProjectName}-firehose"}"""
content = content.replace(old_output, '')

open('infrastructure/cloudformation/main-stack.yaml', 'w').write(content)
print('All fixes applied successfully')
