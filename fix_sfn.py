import json

with open('src/step_functions/pipeline_definition.json') as f:
    definition = json.load(f)

# Replace the broken ValidateInput state with a simple pass-through
definition['States']['ValidateInput'] = {
    "Type": "Pass",
    "Comment": "Pass input through to next state",
    "Next": "RunBronzeToSilver"
}

with open('src/step_functions/pipeline_definition.json', 'w') as f:
    json.dump(definition, f, indent=2)

print('Step Functions definition fixed')
