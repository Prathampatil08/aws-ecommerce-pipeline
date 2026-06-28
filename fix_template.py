content = open('infrastructure/cloudformation/main-stack.yaml').read()

old = """      NotificationConfiguration:
        LambdaConfigurations:
          - Event: "s3:ObjectCreated:*"
            Filter:
              S3Key:
                Rules:
                  - Name: prefix
                    Value: bronze/
            Function: !GetAtt BatchIngestionLambda.Arn"""

content = content.replace(old, '')

old2 = "                Resource:\n                  - !Ref PipelineStateMachine"
new2 = '                Resource: "*"'
content = content.replace(old2, new2)

open('infrastructure/cloudformation/main-stack.yaml', 'w').write(content)
print('Template fixed successfully')
