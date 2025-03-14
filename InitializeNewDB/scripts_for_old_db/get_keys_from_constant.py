import json

file = open('../source/categories_constant.json', 'r', encoding='utf-8')
constants = json.load(file)
print(constants.keys())
