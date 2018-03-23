
# coding: utf-8

# In[1]:

import pandas as pd
twitter_handles_df = pd.read_csv("handle_values_twitter_30lakh1.csv", header = None, names=["handle"])
print("done!!!")


# In[2]:

def camel_case_split(value):
    value = value[1:0]
    matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', "____")
    return [m.group(0) for m in matches]

twitter_handles_df["handle_trimmed"] = twitter_handles_df["handle"].apply(lambda s: s[1:].lower())
twitter_handles_df.count


# In[37]:

twitter_handles_df.count


# In[ ]:

import re
re.sub('[A-Z]',"___", "YouTube")


# In[3]:

import re
brands_df = pd.read_csv("D:\\Data\\Dinesh\\Work\\revlon\\fuzzy_classification\\twitter_brands_na.csv", header = None, names= ["brand_name"])
brands_df["brand_name_trimmed"] = brands_df["brand_name"].apply(lambda s: re.sub('[^a-zA-Z0-9 ]', '', s).lower())

print("done!")
brands_df.head()


# In[39]:

# def find_match(left):
#     print(left)

from IPython.display import clear_output
import math
twitter_handles_df["key"] = 1
brands_df["key"] = 1
twitter_handles = twitter_handles_df.values.tolist()
brands = brands_df.values.tolist()
# combinations["flag"] = df_cartesian.apply(find_match)
# del combinations.key
# combinations.head()
brands[0]

results = []
print("started")
for ind, brand in enumerate(brands):
    brand_name = brand[1]
    brand_words = brand_name.split(" ")
#     print(brand_words)
    max_matched_words = 0 - math.inf
#     max_matched_words = []
    so_far_matching_handle = None
    match_perc = None
    handle_breakdown = None
#     max_match_perc = 0
    for handle in twitter_handles:
        handle_value = handle[1]
#         handle_words = []
#         value = handle[0][1:]
#         before_first_cl = value[0: re.search("[A-Z]", value).start() if re.search("[A-Z]", value) is not None else len(value)]
#         print("before first cl, " + before_first_cl)
#         if len(before_first_cl) > 0:
#             handle_words.append(before_first_cl)
#         handle_words.extend(re.findall('[A-Z][^A-Z]*', value))
#         if(len(handle_words) == 0):
#             handle_words.append(handle[0][1:])
#         matching_words = 0
#         temp = ""
#         new_handle_words = []
#         for w in handle_words:
#             if len(w) == 1:
#                 temp += w
#                 print(temp)
#             else:
#                 new_handle_words.append(w)
#                 print(len(temps))
#                 if len(temp) > 0:
#                     print(temp)
#                     new_handle_words.append(temp)
#                     temp = ""
#         if len(temp) > 0:
#             print("appending temp")
#             new_handle_words.append(temp)
#             temp = ""
#         if(len(new_handle_words) == 0):
#         print(str(handle[0][1:]) + " " + str(handle_words) + " " + str(new_handle_words))
#         for handle_name in new_handle_words:
        matching_words = 0
        for word in brand_words:
            if word in handle_value: # and len(word)/len(handle_name) > .20) or (handle_name in word and len(handle_name)/len(word) > .20):
                matching_words += 1
#         match_perc = (matching_words / len(brand_words) * len(new_handle_words))
#         if(match_perc ==1 or (len(brand_words)>2 and match_perc >= .75)):
            if(matching_words > max_matched_words):
                so_far_matching_handle = handle[0]
                max_matched_words = matching_words
#                 handle_breakdown.append(new_handle_words)
#             max_match_perc = match_perc if match_perc > max_match_perc else max_match_perc
            match_perc = max_matched_words/len(brand_words)
    results.append([brand[0], so_far_matching_handle, max_matched_words, match_perc])
    clear_output()
    print("done: " + str(ind) + ", result: " + str(results[-1]))

print("finished")
            


# In[41]:

df = pd.DataFrame(results, columns = ["brand", "matched_handle", "max_matched_words", "match_ratio"])
df.to_csv("twitter_token_match.csv")
print("done")


# In[34]:

# handle_words = re.findall('[A-Z][^A-Z]*', 'NYC')
# print(handle_words)
# temp = ""
# new_handle_words = []
# for w in handle_words:
#     if len(w) == 1:
#         temp += w
#         print(temp)
#     else:
#         new_handle_words.append(w)
#         print(len(temp))
#         if len(temp) > 0:
#             print("appending temp")
#             new_handle_words.append(temp)
#             temp = ""

# if len(temp) > 0:
#     print("appending temp")
#     new_handle_words.append(temp)
#     temp = ""
# print(new_handle_words)

# str = "elegantBay"
# re.search("[A-Z]", str).start()
# str = str[0:re.search("[A-Z]", str).start()]
# str

from IPython.display import clear_output

for i in range(10):
    clear_output()
    print(str(i) + "Hello World!")

