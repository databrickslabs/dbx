import os, sys, json

print(f'host is {os.environ["DATABRICKS_HOST"]}')

payload = json.loads(sys.argv[1])
print(f'[{payload["command"]}]: flags are {payload["flags"]}')

answer = input('What is your name? ')

print(f'got answer: {answer}')

answer = input('Preferences? ')

print(f'got answer: {answer}')