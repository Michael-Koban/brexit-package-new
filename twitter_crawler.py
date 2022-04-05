import requests
import os
import json
import pandas as pd
import csv , datetime, unicodedata, time, datetime, tweeterid #dateutil.parserm, 
from datetime import datetime

class TwitterCrawler():
    """Summary of class here.

    You must define a TwitterCrawler instance to start crawling Twitter Data.
    Longer class information....

    Attributes:
        likes_spam: A boolean indicating if we like SPAM or not.
        eggs: An integer count of the eggs we have laid.
    """ 
     
    def __init__(self, bearer_token: str):
        """Inits SampleClass with blah."""
        self.bearer_token = bearer_token
        os.environ['TOKEN'] = self.bearer_token
        self.headers = {"Authorization": "Bearer {}".format(self.bearer_token)}

    def auth():
     return os.getenv('TOKEN')

    # def create_headers(self):
    #     """ Creates a header for the Twitter API request,
    #      based on the Bearer Token

    #     Parameters
    #     ----------
    #     sound : str, optional
    #         The sound the animal makes (default is None)

    #     Raises
    #     ------
    #     NotImplementedError
    #         If no sound is set for the animal or passed in as a
    #         parameter.
    #     """

    #     headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
    #     return headers

    def __connect_to_endpoint(self, url: str, params: dict, next_token: str  = None,
     sleep_time:int = 3, num_of_trails = 10, verbose:bool = False, is_retweet = False):

            if is_retweet:
                params['pagination_token'] = next_token 
            else:
                params['next_token'] = next_token   #params object received from create_url function

            for i in range(num_of_trails):

                if i > num_of_trails//2: 
                    print(f"Failed to connect {i} times, going to sleep for 15 minutes..")
                    time.sleep(15*60) # sleep for 15 minutes

                response = requests.request("GET", url, headers = self.headers, params = params)
                if verbose: print(f"Trail #{i} Response Code: " + str(response.status_code))

                if response.status_code == 200: return response.json()
                
                if response.status_code == 429:
                    if verbose: print(f"Try sleeping for {sleep_time} seconds")
                    time.sleep(sleep_time)

            raise Exception(response.status_code, response.text)

    # def __connect_to_endpoint(self, url: str, params: dict, next_token = None, verbose = True):
    #     """ Connect to Twitter API via a endpoint

    #     Parameters
    #     ----------
    #     sound : str, optional
    #         The sound the animal makes (default is None)

    #     Raises
    #     ------
    #     NotImplementedError
    #         If no sound is set for the animal or passed in as a
    #         parameter.
    #     """
    #     params['next_token'] = next_token   #params object received from create_url function
    #     response = requests.request("GET", url, headers = self.headers, params = params)
    #     if verbose == True:
    #         print("Endpoint Response Code: " + str(response.status_code))
    
    #     if response.status_code == 429:
    #         print(response.text)
    #         print("try sleeping for 2 seconds")
    #         time.sleep(2)
    #         return response.status_code

    #     if response.status_code != 200:
    #         raise Exception(response.status_code, response.text)
    #     return response.json()
    
    def get_url_by_tweet_id(tweet_id: str):
        """ Connect to Twitter API via a endpoint

        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
        return "https://twitter.com/anyuser/status/" + str(tweet_id)

    def search_by_tweet_id(self, tweet_id: str):
        """ search url for idtweet - App rate limit: 
        300 requests per 15-minute window shared among all users of your app
        User rate limit (OAuth 1.0a): 900 requests per 15-minute window per each authenticated user


        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
    
        search_url = "https://api.twitter.com/2/tweets/:id"
        search_url = search_url.replace(":id", tweet_id)
        #change params based on the endpoint you are using
        query_params = {
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        json_response = self.__connect_to_endpoint(url = search_url, params = query_params)
        return json_response

    def search_recent_by_keyword(self, keyword: str, start_date, end_date, max_results = 10):
        """ search url for idtweet - App rate limit: 
        300 requests per 15-minute window shared among all users of your app
        User rate limit (OAuth 1.0a): 900 requests per 15-minute window per each authenticated user
         ONLY FROM LAST WEEK !@#!@#!@#!@#@#!@#!@#!@

        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
        search_url = "https://api.twitter.com/2/tweets/search/recent" 

        #change params based on the endpoint you are using
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'end_time': end_date,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}

        json_response = self.__connect_to_endpoint(url= search_url, params = query_params)
        return json_response 
    
###self.__connect_to_endpoint2(self, search_url, query_params, next_token = next_token, verbose = False)
    


    def __return_tweets_of_key_opinion_leader(self, query="", user_name=None,
                                        start_time = "2015-12-7T00:00:00Z",
                                        end_time = "2021-12-26T00:00:00Z",
                                        max_results = 10, evaluate_last_token = False,
                                        limit_amount_of_returned_tweets = 10000000,
                                       verbose_10 = False, dir_name = "key_opinion_leaders_tweets_tables_beta"
):
  
        search_url = "https://api.twitter.com/2/tweets/search/all" #endpoint use to collect data from

        import os.path
        try:
            os.mkdir(dir_name)
            print("creating directory", dir_name, "to insert all the tables of all the key opinion leaders")
        except:
            print("The dir", dir_name ,"already exist")

        if user_name is not None:
            query = str(query) + " from:" + str(user_name)
            try:
                display_start_time = datetime.strptime(start_time.split("T")[0], "%Y-%m-%d").strftime("%d-%m-%Y")
                display_end_time = datetime.strptime(end_time.split("T")[0], "%Y-%m-%d").strftime("%d-%m-%Y")
            except:
                display_start_time = start_time
                display_end_time = end_time
            print("Bringing all the tweets of the user:", user_name, "from:", display_start_time, "to", display_end_time)
            print()

        ##### the log dir
        import os.path
        #dir_name = "key_opinion_leaders_tweets_tables_beta"
        dir_name_beta = "key_opinion_leaders_tweets_tables_beta"
        dir_log_name = os.path.join(dir_name_beta, "log_key_opinion_leaders") 

        # try:
        #     os.mkdir(dir_name)
        #     print("creating directory", dir_name, "to insert all the tables of all the key opinion leaders")
        # except:
        #     print("The dir", dir_name ,"already exist")
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the key opinion leaders")
        except:
            print("The dir", dir_log_name ,"already exist")
        ########################
        path_for_log_dir_of_certain_user = os.path.join(dir_log_name, user_name)
        try:
            os.mkdir(path_for_log_dir_of_certain_user)
            print("creating directory", path_for_log_dir_of_certain_user,"in the dir",dir_log_name, "to insert all the logs of the key opinion leader", user_name)
        except:
            print("The dir", path_for_log_dir_of_certain_user ,"already exist")
            
        path_for_dir_retriving_tweets_streem = os.path.join(path_for_log_dir_of_certain_user, 'retriving_tweets_streem.txt')
        with open(path_for_dir_retriving_tweets_streem, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')
            
        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, user_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            next_token = last_lines[0:-1]
            a_file.close()    
        else:
            next_token = None
            
        ################ Add a time stamp ########################################
        path_for_dir_tokens = os.path.join(path_for_log_dir_of_certain_user, 'tokens.txt')
        with open(path_for_dir_tokens, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')
            
        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_tweets = 0
        counter_loops = 0
        
        while continue_searching == True and num_of_returned_tweets < limit_amount_of_returned_tweets:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of tweets is getting close to the limit number, we need to alter the max_result,
            #so we won't get tweets beyond what we asked
            
            if (limit_amount_of_returned_tweets - num_of_returned_tweets) < max_results:
                max_results = limit_amount_of_returned_tweets - num_of_returned_tweets
                if max_results < 10:
                    max_results = 10
            else :
                max_results = max_results

            #change params based on the endpoint you are using
            query_params = {'query': query,
                        'start_time': start_time,
                        'end_time': end_time,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {next_token}}
            
            json_response = self.__connect_to_endpoint(url = search_url,params= query_params, next_token = next_token)
            
            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_tweets += json_response["meta"]["result_count"]

            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                b = pd.json_normalize(json_response["includes"], ["users"]).add_prefix("users.")
                
                a.conversation_id = a.conversation_id.astype("string")
                a.id = a.id.astype("string")
                a["id_new"] = "id: " + a["id"].astype("string")
                a["conv_id_new"] = "conv_id: " + a["conversation_id"].astype("string")
            #c = pd.json_normalize(json_response["includes"]["places"]).add_prefix("places.")
                df_tweets_i = pd.merge(a, b, left_on="author_id", right_on="users.id")
                list_of_cols_to_add = ['author_id', 'conversation_id', "conv_id_new", "id", "id_new",
                                    'created_at','entities.mentions',
                            'public_metrics.like_count', 'public_metrics.quote_count',
                        'public_metrics.reply_count', 'public_metrics.retweet_count','referenced_tweets', 'text',
                            'users.created_at', 'users.description','users.id', 'users.name',
                        'users.public_metrics.followers_count', 'users.public_metrics.following_count',
                        'users.public_metrics.listed_count', 'users.public_metrics.tweet_count',
                        'users.username', 'users.verified']

                list_cols_to_drop = [x for x in df_tweets_i.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = df_tweets_i.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"

                #sort columns by alphabetic order
                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)
                
                name = user_name + ".csv"
                path_for_table = os.path.join(dir_name_beta, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)
            except:
                print("no data / include in the json")
        
            with open(path_for_dir_retriving_tweets_streem, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " tweets, and there are more tweets of that user to get, I am bringing more tweets!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_tweets)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose_10 == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                elif verbose_10 == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(path_for_dir_tokens, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more tweets from this user")
                continue_searching = False
                print("Total amount of collected tweets = ", num_of_returned_tweets)
            
            if num_of_returned_tweets >=limit_amount_of_returned_tweets:
                print("oooops, There may be more tweets to return, but you asked to limit the amount of returned tweets")
                print("infact you got", num_of_returned_tweets, "returned tweets and limited the function to get", limit_amount_of_returned_tweets, "tweets")
        
        #In what case we suspect that there may be more tweets that we didn't get? -->
        #When the number of tweets we asked to get is equal to the number of tweets we got back
        
        ### save all thr json responses in json file:
        path_for_dir_all_json_responses = os.path.join(path_for_log_dir_of_certain_user, 'all_json_responses.json')
        with open(path_for_dir_all_json_responses, 'w') as outfile:
            json.dump(json_response_list, outfile)
        return json_response_list, num_of_returned_tweets, next_tokens

 #The following cose check wheter the user_name is a list, if not it turnes it into a list
      
    def return_tweets_of_key_opinion_leaders(self, query="", dir_name="tweets", user_names =None, \
        start_time = "2015-12-7T00:00:00Z", end_time = "2021-12-26T00:00:00Z",
        max_results = 10, evaluate_last_token = False, \
            limit_amount_of_returned_tweets = 10000000, verbose_10 = False):

            if type(user_names) != list:
                user_names = [user_names]
            
            #users_json_response_lists = []
            names_evaluated = []
            names_didnt_evaluated = []
            next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
            for name in user_names:
                print("Bringing tweets of", name)
                query = ""
                user_name = name
                try:

                    json_response_list, num_of_returned_tweets,next_tokens = self.__return_tweets_of_key_opinion_leader(query=query, user_name=user_name,
                                                            start_time = start_time,
                                                            end_time = end_time,
                                                            max_results = max_results,
                                                            limit_amount_of_returned_tweets = limit_amount_of_returned_tweets,
                                                                                                                verbose_10 = True)
                    print(num_of_returned_tweets)
                    if num_of_returned_tweets > 0:
                        names_evaluated.append(name)
                        next_tokens_users.append(next_tokens)
                

                    else:
                        names_didnt_evaluated.append(name)
                        print("The user:", name, "had", num_of_returned_tweets, "tweets!!")

                    print("---------------------------------------------------------------")
                except:
                    print("There was a problem with the key opinion leader:", name)
                    names_didnt_evaluated.append(name)
                    print("*************************************************************************************")

    import time, datetime,json, numpy as np
    from inputimeout import inputimeout, TimeoutOccurred


    def create_url_tweet_ids(self, search_url, tweet_ids_list, verbose = False):
    
        #search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from
        search_url = search_url.replace("X", ','.join(tweet_ids_list))
        if verbose: print(search_url)
        #change params based on the endpoint you are using
        query_params = {
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        return (search_url, query_params)



    #   sleep_time = 15*60 ## 15 minutes in sec
    #   tweet_ids = all_harvard_data["ID"].to_numpy().astype(str)

    from os import path


    def get_tweets_by_tweet_ids(self , tweet_ids, #TypeError: 'type' object is not subscriptable 
     json_tweets_output_folder : str,
     tweets_per_api_request : int = 100,
     api_error_sleep_secs : int = 15*60,
     verbose = False ):

        os.makedirs(json_tweets_output_folder, exist_ok = True) 

        count_proccesed_tweets_file = os.path.join(json_tweets_output_folder, 'count_proccesed_tweets.txt')
        count_requests_sent_file = os.path.join(json_tweets_output_folder,'count_requests_sent.txt')
        json_output_file_basename = os.path.join(json_tweets_output_folder,'twitter_data_')

        if not os.path.exists(count_requests_sent_file):
            with open(count_requests_sent_file,'w') as f:
                f.write('%d' % 0)
        
        if not os.path.exists(count_proccesed_tweets_file):
            with open(count_proccesed_tweets_file,'w') as f:
                f.write('%d' % 0)
        
        count_proccesed_tweets = int( open(count_proccesed_tweets_file,'r').read())   

        count_requests_sent = int( open(count_requests_sent_file,'r').read())

        while(count_proccesed_tweets < len(tweet_ids)):
            
            if verbose: 
                print(f'~~~~~ Already sent {count_requests_sent} requests')
                print(f'~~~~~ Already proccesed {count_proccesed_tweets} tweets')
                print(f'~~~~~ Currently proccessing {tweets_per_api_request} tweets')

            
            ## FIXME - last batch size cant be more than what's left 

            tweet_ids_batch = tweet_ids[count_proccesed_tweets:(count_proccesed_tweets+ tweets_per_api_request )// (len(tweet_ids)-1)]
            search_url, query_params = self.create_url_tweet_ids("https://api.twitter.com/2/tweets/?ids=X" , tweet_ids_batch)
            # headers = create_headers(os.environ['TOKEN'])

            try:
                json_response = self.__connect_to_endpoint( url = search_url, params= query_params)
            except Exception as e: 
                print(f'Twitter API error: {e} \n Sleeping 15 min from {datetime.datetime.now()}')
                time.sleep(api_error_sleep_secs)

                    
            count_proccesed_tweets += tweets_per_api_request
            count_requests_sent += 1

            
            with open(f'{json_output_file_basename}{count_requests_sent}.json', "w") as twitter_data_file:
                json.dump(json_response, twitter_data_file, indent=4, sort_keys=True) 
                
            with open(count_proccesed_tweets_file, "w") as f:
                f.write('%d' % count_proccesed_tweets)
            
            with open(count_requests_sent_file, "w") as f:
                f.write('%d' % count_requests_sent) 
            
            # if verbose: print(f'~~~~~ Batch done, moving forward (sleep 2 min for debug)')
            # time.sleep(5)  



    def __return_retweets_of_tweet_id_SMALL(self, tweet_id=None,
                                        max_results = 10, evaluate_last_token = False,
                                        limit_amount_of_returned_retweets = 10000000,
                                    verbose = False, dir_tree_name = "conversation_trees"):

        search_url = "https://api.twitter.com/2/tweets/:id/retweeted_by" #endpoint use to collect data from
        search_url = search_url.replace(":id", tweet_id)

        import os.path
        #making a dir for the tree - this file will cintain a unique file for each conversation id
        #dir_tree_name = "conversation_trees"
        try:
            os.mkdir(dir_tree_name)
            print("creating tree directory", dir_tree_name, "to store all the trees")
        except:
            print("The dir", dir_tree_name ,"already exist")
        
        
        #making dir (inside the tree dir) to store, for each tweet-id all its retweets
        name_for_tweet_id = "conv_tree_for_" + str(tweet_id)
        dir_name_for_tweet_id = os.path.join(dir_tree_name, name_for_tweet_id) 
        try:
            os.mkdir(dir_name_for_tweet_id)
            print("creating directory", dir_name_for_tweet_id, "to insert all the retweets of the given tweet-id")
        except:
            print("The dir", dir_name_for_tweet_id ,"already exist")

        ##### the log dir
        dir_log_name = os.path.join(dir_name_for_tweet_id, "log_retweets_for_tweet_id_" + tweet_id) 
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the retweets for the tweet id - ", str(tweet_id))
        except:
            print("The dir", dir_log_name ,"already exist")
            
        ########################
    #     path_for_log_dir_of_certain_user = os.path.join(dir_log_name, user_name)
    #     try:
    #         os.mkdir(path_for_log_dir_of_certain_user)
    #         print("creating directory", path_for_log_dir_of_certain_user,"in the dir",dir_log_name, "to insert all the logs of the key opinion leader", user_name)
    #     except:
    #         print("The dir", path_for_log_dir_of_certain_user ,"already exist")

        path_for_dir_retriving_retweets_stream = os.path.join(dir_log_name, 'retriving_retweets_streem.txt')

        with open(path_for_dir_retriving_retweets_stream, 'a') as f:
            from datetime import datetime
            time_now = datetime.now()
            current_time = time_now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')

        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            #next_token = last_lines[0:-1]
            #a_file.close()
            if "Current" in last_lines:
                from file_read_backwards import FileReadBackwards

                with FileReadBackwards(tokens_location, encoding="utf-8") as frb:
                    for l in frb:
                        if "Current" in l:
                            continue
                        elif any(c.isalpha() for c in l):
                            next_token = l
                            break
        else:
            next_token = None
        ################ Add a time stamp ########################################
        with open(tokens_location, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')

        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_retweets = 0
        counter_loops = 0

        while continue_searching == True and num_of_returned_retweets < limit_amount_of_returned_retweets:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of retweets is getting close to the limit number, we need to alter the max_result,
            #so we won't get retweets beyond what we asked
            #if (limit_amount_of_returned_retweets - num_of_returned_retweets) < max_results:
            max_results = min(limit_amount_of_returned_retweets - num_of_returned_retweets,max_results)
            #else :
            #    max_results = max_results

        #change params based on the endpoint you are using
            query_params = {
                            'expansions': 'pinned_tweet_id',
                            'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                            'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                            #'place.fields': 'contained_within, country, country_code, full_name, geo, id, name, place_type',
                            'max_results': max_results,
                            'pagination_token': next_token}

            
            json_response = self.__connect_to_endpoint(url = search_url, params= query_params, next_token = next_token, is_retweet = True)

            # #check if the tweeet_id is valid = if we can find its retweets:
            # if "data" not in json_response:
            #     tweet_ids_not_found.append(json_response["errors"][0]["resource_id"])
            #     print("prob")

            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_retweets += json_response["meta"]["result_count"]

            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                a["id_new"] = "id: " + a["id"].astype("string")
                list_of_cols_to_add = ["id","id_new", "verified", "username", "name", "created_at", "public_metrics.followers_count",
                                    "public_metrics.following_count", "public_metrics.tweet_count",
                                    "public_metrics.listed_count", "pinned_tweet_id"]
                list_cols_to_drop = [x for x in a.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = a.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"

                #sort columns by alphabetic order
                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)

                name = tweet_id + "_retweets" + ".csv"
                path_for_table = os.path.join(dir_name_for_tweet_id, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)
            except:
                print("no data / include in the json")

            path_for_dir_all_json_responses = os.path.join(dir_log_name, 'all_json_responses.json')
            with open(path_for_dir_all_json_responses, 'w') as outfile:
                json.dump(json_response_list, outfile)

            with open(path_for_dir_retriving_retweets_stream, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " tweets, and there are more tweets of that user to get, I am bringing more tweets!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_retweets)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                elif verbose == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(tokens_location, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more tweets from this user")
                continue_searching = False
                print("Total amount of collected tweets = ", num_of_returned_retweets)

            if num_of_returned_retweets >=limit_amount_of_returned_retweets:
                print("oooops, There may be more tweets to return, but you asked to limit the amount of returned tweets")
                print("infact you got", num_of_returned_retweets, "returned tweets and limited the function to get", limit_amount_of_returned_retweets, "tweets")

        #In what case we suspect that there may be more tweets that we didn't get? -->
        #When the number of tweets we asked to get is equal to the number of tweets we got back

        ### save all thr json responses in json file:

        return json_response_list, num_of_returned_retweets, next_tokens


    def return_retweets_by_tweet_ids(self, tweet_ids,max_results = 10, evaluate_last_token = False,
                                        limit_amount_of_returned_retweets = 10000000,
                                    verbose = False, dir_tree_name = "conversation_trees"):
                if max_results > 100:
                    max_results = 100
                    print('max_results can not be greater than 100, changed to 100')
                    
                if type(tweet_ids) != list:
                    tweet_ids = [tweet_ids]
                
                #users_json_response_lists = []
                tweet_ids_evaluated = []
                tweet_ids_didnt_evaluated = []
                next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
                for tweet_id in tweet_ids:
                    print("Bringing retweets of", tweet_id)
                    try:
                        json_response_list, num_of_returned_retweets,next_tokens =\
                             self.__return_retweets_of_tweet_id_SMALL(tweet_id=tweet_id,
                                    max_results = max_results, evaluate_last_token = evaluate_last_token,
                                    limit_amount_of_returned_retweets = limit_amount_of_returned_retweets,
                                   verbose = verbose, dir_tree_name = dir_tree_name)
                        
                        
                        print(num_of_returned_retweets)
                        if num_of_returned_retweets > 0:
                            tweet_ids_evaluated.append(tweet_id)
                            next_tokens_users.append(next_tokens)
                    

                        else:
                            tweet_ids_didnt_evaluated.append(tweet_id)
                            print("The tweet_id:", tweet_id, "had", num_of_returned_retweets, "tweets!!")

                        print("---------------------------------------------------------------")
                    except:
                        print("There was a problem with the tweet id:", tweet_id)
                        tweet_ids_didnt_evaluated.append(tweet_id)
                        print("*************************************************************************************")


#### quotes


    def __return_quotes_of_tweet_id_SMALL(self, tweet_id=None,
                                        max_results = 10, evaluate_last_token = False,
                                            limit_amount_of_returned_quotes = 10000000,
                                    verbose = False, dir_tree_name = "conversation_trees"):

        search_url = "https://api.twitter.com/2/tweets/:id/quote_tweets"
        search_url = search_url.replace(":id", tweet_id)

        import os.path
        #making a dir for the tree - this file will cintain a unique file for each conversation id
        #dir_tree_name = "conversation_trees"
        try:
            os.mkdir(dir_tree_name)
            print("creating tree directory", dir_tree_name, "to store all the trees")
        except:
            print("The dir", dir_tree_name ,"already exist")
        
        
        #making dir (inside the tree dir) to store, for each tweet-id all its quotes
        name_for_tweet_id = "conv_tree_for_" + str(tweet_id)
        dir_name_for_tweet_id = os.path.join(dir_tree_name, name_for_tweet_id) 
        try:
            os.mkdir(dir_name_for_tweet_id)
            print("creating directory", dir_name_for_tweet_id, "to insert all the quotes of the given tweet-id")
        except:
            print("The dir", dir_name_for_tweet_id ,"already exist")

        ##### the log dir
        dir_log_name = os.path.join(dir_name_for_tweet_id, "log_quotes_for_tweet_id_" + tweet_id) 
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the quotes for the tweet id - ", str(tweet_id))
        except:
            print("The dir", dir_log_name ,"already exist")
            
        ########################
    #     path_for_log_dir_of_certain_user = os.path.join(dir_log_name, user_name)
    #     try:
    #         os.mkdir(path_for_log_dir_of_certain_user)
    #         print("creating directory", path_for_log_dir_of_certain_user,"in the dir",dir_log_name, "to insert all the logs of the key opinion leader", user_name)
    #     except:
    #         print("The dir", path_for_log_dir_of_certain_user ,"already exist")

        path_for_dir_retriving_quotes_stream = os.path.join(dir_log_name, 'retriving_quotes_streem.txt')
        with open(path_for_dir_retriving_quotes_stream, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')

        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            #next_token = last_lines[0:-1]
            #a_file.close()
            if "Current" in last_lines:
                from file_read_backwards import FileReadBackwards

                with FileReadBackwards(tokens_location, encoding="utf-8") as frb:
                    for l in frb:
                        if "Current" in l:
                            continue
                        elif any(c.isalpha() for c in l):
                            next_token = l
                            break
        else:
            next_token = None

        ################ Add a time stamp ########################################
        with open(tokens_location, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')

        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_quotes = 0
        counter_loops = 0

        while continue_searching == True and num_of_returned_quotes < limit_amount_of_returned_quotes:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of quotes is getting close to the limit number, we need to alter the max_result,
            #so we won't get quotes beyond what we asked
            if (limit_amount_of_returned_quotes - num_of_returned_quotes) < max_results:
                max_results = limit_amount_of_returned_quotes - num_of_returned_quotes
            else :
                max_results = max_results

        #change params based on the endpoint you are using
            query_params = {'expansions': 'author_id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id',
                            'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                            'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                            'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                            'max_results': max_results,
                            'pagination_token': {next_token}}
            
            json_response = self.__connect_to_endpoint(url = search_url, params= query_params, next_token = next_token, is_retweet = True)

            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_quotes += json_response["meta"]["result_count"]

            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                a["id_new"] = "id: " + a["id"].astype("string")
                b = pd.json_normalize(json_response["includes"], ["users"]).add_prefix("users.")

                df_tweets_i = pd.merge(a, b, left_on="author_id", right_on="users.id")

                list_of_cols_to_add = ['id', "id_new", 'conversation_id', 'lang', 'author_id', 'referenced_tweets',
                                    'text', 'created_at', 'source', 'reply_settings',
                                    'public_metrics.retweet_count', 'public_metrics.reply_count',
                                    'public_metrics.like_count', 'public_metrics.quote_count',
                                    'in_reply_to_user_id', 'geo.place_id',
                                    'users.username', 'users.name', 'users.verified',
                                    'users.created_at', 'users.id', 'users.public_metrics.followers_count',
                                    'users.public_metrics.following_count',
                                    'users.public_metrics.tweet_count',
                                    'users.public_metrics.listed_count'] #'referenced_tweet_type', 'referenced_tweet_id'
                
                list_cols_to_drop = [x for x in a.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = df_tweets_i.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"


                
                # #add 2 new columns for referenced tweets
                # referenced_tweet_type = []
                # referenced_tweet_id = []
                # for row in range(df_tweets_i.shape[0]):
                #     referenced_tweet_type.append(df_tweets_i.referenced_tweets[0][0]["type"])
                #     referenced_tweet_id.append(df_tweets_i.referenced_tweets[0][0]["id"])
                            
                # df_tweets_i["referenced_tweets_type"] = np.asarray(referenced_tweet_type)
                # df_tweets_i["referenced_tweets_id"] = np.asarray(referenced_tweet_id)
                #sort columns by alphabetic order
                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)

                name = tweet_id + "_quotes" + ".csv"
                path_for_table = os.path.join(dir_name_for_tweet_id, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)
            except:
                print("no data / include in the json")

                ### save all thr json responses in json file:
                path_for_dir_all_json_responses = os.path.join(dir_log_name, 'all_json_responses.json')
                with open(path_for_dir_all_json_responses, 'w') as outfile:
                    json.dump(json_response_list, outfile)



            with open(path_for_dir_retriving_quotes_stream, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " tweets, and there are more tweets of that user to get, I am bringing more tweets!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_quotes)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                elif verbose == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(tokens_location, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more tweets from this user")
                continue_searching = False
                print("Total amount of collected tweets = ", num_of_returned_quotes)

            if num_of_returned_quotes >=limit_amount_of_returned_quotes:
                print("oooops, There may be more tweets to return, but you asked to limit the amount of returned tweets")
                print("infact you got", num_of_returned_quotes, "returned tweets and limited the function to get", limit_amount_of_returned_quotes, "tweets")

        #In what case we suspect that there may be more tweets that we didn't get? -->
        #When the number of tweets we asked to get is equal to the number of tweets we got back


        return json_response_list, num_of_returned_quotes, next_tokens,path_for_table


    def return_quotes_by_tweet_ids(self, tweet_ids,max_results = 10, evaluate_last_token = False,
                                            limit_amount_of_returned_quotes = 10000000,
                                        verbose = False, dir_tree_name = "conversation_trees"):
                    if max_results > 100:
                        max_results = 100
                        print('max_results can not be greater than 100, changed to 100')
                    if max_results < 10:
                        max_results = 10
                        print('max_results can not be smaller than 10, changed to 10')
                    if type(tweet_ids) != list:
                        tweet_ids = [tweet_ids]
                    
                    #users_json_response_lists = []
                    tweet_ids_evaluated = []
                    tweet_ids_didnt_evaluated = []
                    next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
                    path_for_table_dict = {}
                    for tweet_id in tweet_ids:
                        print("Bringing quotes of", tweet_id)
                        try:
                            json_response_list, num_of_returned_quotes,next_tokens, path_for_table =\
                                self.__return_quotes_of_tweet_id_SMALL(tweet_id=tweet_id,
                                        max_results = max_results, evaluate_last_token = evaluate_last_token,
                                        limit_amount_of_returned_quotes = limit_amount_of_returned_quotes,
                                    verbose = verbose, dir_tree_name = dir_tree_name)
                            
                            path_for_table_dict[tweet_id] = path_for_table

                            
                            print(num_of_returned_quotes)
                            if num_of_returned_quotes > 0:
                                tweet_ids_evaluated.append(tweet_id)
                                next_tokens_users.append(next_tokens)
                        

                            else:
                                tweet_ids_didnt_evaluated.append(tweet_id)
                                print("The tweet_id:", tweet_id, "had", num_of_returned_quotes, "quotes!!")

                            print("---------------------------------------------------------------")
                        except:
                            print("There was a problem with the tweet id:", tweet_id)
                            tweet_ids_didnt_evaluated.append(tweet_id)
                            print("*************************************************************************************")

                    return path_for_table_dict

# likes
    def __return_likes_of_tweet_id_SMALL(self, tweet_id=None,
                                        max_results = 10, evaluate_last_token = False,
                                            limit_amount_of_returned_likes = 10000000,
                                    verbose = False, dir_tree_name = "conversation_trees"):

        search_url = "https://api.twitter.com/2/tweets/:id/liking_users"
        search_url = search_url.replace(":id", tweet_id)

        import os.path
        #making a dir for the tree - this file will cintain a unique file for each conversation id
        #dir_tree_name = "conversation_trees"
        try:
            os.mkdir(dir_tree_name)
            print("creating tree directory", dir_tree_name, "to store all the trees")
        except:
            print("The dir", dir_tree_name ,"already exist")
        
        
        #making dir (inside the tree dir) to store, for each tweet-id all its quotes
        name_for_tweet_id = "conv_tree_for_" + str(tweet_id)
        dir_name_for_tweet_id = os.path.join(dir_tree_name, name_for_tweet_id) 
        try:
            os.mkdir(dir_name_for_tweet_id)
            print("creating directory", dir_name_for_tweet_id, "to insert all the likes of the given tweet-id")
        except:
            print("The dir", dir_name_for_tweet_id ,"already exist")

        ##### the log dir
        dir_log_name = os.path.join(dir_name_for_tweet_id, "log_likes_for_tweet_id_" + tweet_id) 
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the likes for the tweet id - ", str(tweet_id))
        except:
            print("The dir", dir_log_name ,"already exist")
            
        ########################
    #     path_for_log_dir_of_certain_user = os.path.join(dir_log_name, user_name)
    #     try:
    #         os.mkdir(path_for_log_dir_of_certain_user)
    #         print("creating directory", path_for_log_dir_of_certain_user,"in the dir",dir_log_name, "to insert all the logs of the key opinion leader", user_name)
    #     except:
    #         print("The dir", path_for_log_dir_of_certain_user ,"already exist")

        path_for_dir_retriving_likes_stream = os.path.join(dir_log_name, 'retriving_likes_streem.txt')
        with open(path_for_dir_retriving_likes_stream, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')

        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            #next_token = last_lines[0:-1]
            #a_file.close()
            if "Current" in last_lines:
                from file_read_backwards import FileReadBackwards

                with FileReadBackwards(tokens_location, encoding="utf-8") as frb:
                    for l in frb:
                        if "Current" in l:
                            continue
                        elif any(c.isalpha() for c in l):
                            next_token = l
                            break
        else:
            next_token = None

        ################ Add a time stamp ########################################
        with open(tokens_location, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')

        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_likes = 0
        counter_loops = 0

        while continue_searching == True and num_of_returned_likes < limit_amount_of_returned_likes:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of quotes is getting close to the limit number, we need to alter the max_result,
            #so we won't get quotes beyond what we asked
            if (limit_amount_of_returned_likes - num_of_returned_likes) < max_results:
                max_results = limit_amount_of_returned_likes - num_of_returned_likes
            else :
                max_results = max_results

        #change params based on the endpoint you are using
            query_params = {'expansions': 'pinned_tweet_id',
                            'tweet.fields': "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,public_metrics,organic_metrics,promoted_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld",
                            'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                            'max_results': max_results,
                            'pagination_token': {next_token}}
            
            json_response = self.__connect_to_endpoint(url = search_url, params= query_params, next_token = next_token, is_retweet = True)

            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_likes += json_response["meta"]["result_count"]

            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                a["id_new"] = "id: " + a["id"].astype("string")
                #b = pd.json_normalize(json_response["includes"], ["users"]).add_prefix("users.")

                #df_tweets_i = pd.merge(a, b, left_on="author_id", right_on="users.id")

                list_of_cols_to_add = ['id', "id_new", 'verified', 'created_at', 'description', 'name', 'username',
       'public_metrics.followers_count', 'public_metrics.following_count',
       'public_metrics.tweet_count', 'public_metrics.listed_count'] #'referenced_tweet_type', 'referenced_tweet_id'
                
                list_cols_to_drop = [x for x in a.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = a.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"
                        
                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)

                name = tweet_id + "_likes" + ".csv"
                path_for_table = os.path.join(dir_name_for_tweet_id, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)
            except:
                print("no data / include in the json")

                ### save all thr json responses in json file:
                path_for_dir_all_json_responses = os.path.join(dir_log_name, 'all_json_responses.json')
                with open(path_for_dir_all_json_responses, 'w') as outfile:
                    json.dump(json_response_list, outfile)


            with open(path_for_dir_retriving_likes_stream, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " tweets, and there are more tweets of that user to get, I am bringing more tweets!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_likes)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                elif verbose == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["pagination_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(tokens_location, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more tweets from this user")
                continue_searching = False
                print("Total amount of collected tweets = ", num_of_returned_likes)

            if num_of_returned_likes >=limit_amount_of_returned_likes:
                print("oooops, There may be more likes to return, but you asked to limit the amount of returned likes")
                print("infact you got", num_of_returned_likes, "returned likes and limited the function to get", limit_amount_of_returned_likes, "tweets")

        return json_response_list, num_of_returned_likes, next_tokens, path_for_table


    def return_likes_by_tweet_ids(self, tweet_ids,max_results = 10, evaluate_last_token = False,
                                            limit_amount_of_returned_likes = 10000000,
                                        verbose = False, dir_tree_name = "conversation_trees"):
                    if max_results > 100:
                        max_results = 100
                        print('max_results can not be greater than 100, changed to 100')
                    if max_results < 1:
                        max_results = 1
                        print('max_results can not be smaller than 10, changed to 10')
                    if type(tweet_ids) != list:
                        tweet_ids = [tweet_ids]
                    
                    #users_json_response_lists = []
                    tweet_ids_evaluated = []
                    tweet_ids_didnt_evaluated = []
                    next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
                    path_for_table_dict = {}
                    for tweet_id in tweet_ids:
                        print("Bringing likes of", tweet_id)
                        try:
                            json_response_list, num_of_returned_likes,next_tokens, path_for_table =\
                                self.__return_likes_of_tweet_id_SMALL(tweet_id=tweet_id,
                                        max_results = max_results, evaluate_last_token = evaluate_last_token,
                                        limit_amount_of_returned_likes = limit_amount_of_returned_likes,
                                    verbose = verbose, dir_tree_name = dir_tree_name)
                            
                            path_for_table_dict[tweet_id] = path_for_table

                            
                            print(num_of_returned_likes)
                            if num_of_returned_likes > 0:
                                tweet_ids_evaluated.append(tweet_id)
                                next_tokens_users.append(next_tokens)
                        

                            else:
                                tweet_ids_didnt_evaluated.append(tweet_id)
                                print("The tweet_id:", tweet_id, "had", num_of_returned_likes, "likes!!")

                            print("---------------------------------------------------------------")
                        except:
                            print("There was a problem with the tweet id:", tweet_id)
                            tweet_ids_didnt_evaluated.append(tweet_id)
                            print("*************************************************************************************")

                    return path_for_table_dict


###### replies:
# Twitter's API doesn't allow you to get replies to a particular tweet. Strange
# but true. But you can use Twitter's Search API to search for tweets that are
# directed at a particular user, and then search through the results to see if 
# any are replies to a given tweet. You probably are also interested in the
# replies to any replies as well, so the process is recursive. The big caveat 
# here is that the search API only returns results for the last 7 days. So 
# you'll want to run this sooner rather than later.
#### link: https://gist.github.com/edsu/54e6f7d63df3866a87a15aed17b51eaf

## To get the commetnts f a certain tweet you need to provide the conversation id
    def __return_replies_of_conv_id_SMALL(self, conversation_id="", query = "",
                                        start_time = "2015-12-7T00:00:00Z",
                                        end_time = "today",
                                        max_results = 10, evaluate_last_token = False,
                                        limit_amount_of_returned_comments = 10000000,
                                        verbose = False, dir_tree_name = "conversation_trees"):
        
        from datetime import date
        if end_time == "today":
            #if you wish to get all the comments that were written till this day:
            end_time = date.today().strftime("%Y-%m-%d")+"T00:00:00Z"

        tweet_id = conversation_id
        search_url = "https://api.twitter.com/2/tweets/search/all" #endpoint use to collect data from
        query = query + "conversation_id:"+ str(conversation_id)

        import os.path
        #making a dir for the tree - this file will cintain a unique file for each conversation id
        #dir_tree_name = "conversation_trees"
        try:
            os.mkdir(dir_tree_name)
            print("creating tree directory", dir_tree_name, "to store all the trees")
        except:
            print("The dir", dir_tree_name ,"already exist")


        #making dir (inside the tree dir) to store, for each tweet-id all its quotes
        name_for_tweet_id = "conv_tree_for_" + str(tweet_id)
        dir_name_for_tweet_id = os.path.join(dir_tree_name, name_for_tweet_id) 
        try:
            os.mkdir(dir_name_for_tweet_id)
            print("creating directory", dir_name_for_tweet_id, "to insert all the comments of the given conversation-id")
        except:
            print("The dir", dir_name_for_tweet_id ,"already exist")

        ##### the log dir
        dir_log_name = os.path.join(dir_name_for_tweet_id, "log_comments_for_conversation_id_" + tweet_id) 
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the comments for the tweet id - ", str(tweet_id))
        except:
            print("The dir", dir_log_name ,"already exist")

        ########################

        path_for_dir_retriving_comments_stream = os.path.join(dir_log_name, 'retriving_comments_streem.txt')
        with open(path_for_dir_retriving_comments_stream, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')

        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            #next_token = last_lines[0:-1]
            #a_file.close()
            if "Current" in last_lines:
                from file_read_backwards import FileReadBackwards

                with FileReadBackwards(tokens_location, encoding="utf-8") as frb:
                    for l in frb:
                        if "Current" in l:
                            continue
                        elif any(c.isalpha() for c in l):
                            next_token = l
                            break
        else:
            next_token = None

        ################ Add a time stamp ########################################
        with open(tokens_location, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')

        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_comments = 0
        counter_loops = 0

        while continue_searching == True and num_of_returned_comments < limit_amount_of_returned_comments:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of quotes is getting close to the limit number, we need to alter the max_result,
            #so we won't get quotes beyond what we asked
            if (limit_amount_of_returned_comments - num_of_returned_comments) < max_results:
                max_results = limit_amount_of_returned_comments - num_of_returned_comments
            else :
                max_results = max_results
        #change params based on the endpoint you are using
            query_params = {'query': query,
                                    'start_time': start_time,
                                    'end_time': end_time,
                                    'max_results': max_results,
                                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id',
                                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                                    'next_token': {next_token}}

            json_response = self.__connect_to_endpoint(url = search_url, params= query_params, next_token = next_token, is_retweet = False)

            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_comments += json_response["meta"]["result_count"]


            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                a["id_new"] = "id: " + a["id"].astype("string")

                b = pd.json_normalize(json_response["includes"], ["users"]).add_prefix("users.")

                df_tweets_i = pd.merge(a, b, left_on="author_id", right_on="users.id")

                list_of_cols_to_add = ['id', "id_new",'text', 'conversation_id', 'in_reply_to_user_id','reply_settings', 'referenced_tweets', 'lang', 'created_at',
        'author_id', 'source', 'entities.mentions', 'public_metrics.retweet_count', 'public_metrics.reply_count',
        'public_metrics.like_count', 'public_metrics.quote_count', 'users.username', 'users.created_at', 'users.id', 'users.description',
            'users.verified', 'users.name', 'users.public_metrics.followers_count','users.public_metrics.following_count',
            'users.public_metrics.tweet_count','users.public_metrics.listed_count'] #'referenced_tweet_type', 'referenced_tweet_id'

                list_cols_to_drop = [x for x in a.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = df_tweets_i.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"

                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)

                name = tweet_id + "_comments" + ".csv"
                path_for_table = os.path.join(dir_name_for_tweet_id, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)

            except:
                print("no data / include in the json")

                ### save all thr json responses in json file:
                path_for_dir_all_json_responses = os.path.join(dir_log_name, 'all_json_responses.json')
                with open(path_for_dir_all_json_responses, 'w') as outfile:
                    json.dump(json_response_list, outfile)



            with open(path_for_dir_retriving_comments_stream, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " comments, and there are more comments of that conversation-id to get, I am bringing more comments!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_comments)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "comments, and there are more comments of that conversation-id to get, I am bringing more comments!\n")
                elif verbose == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "comments, and there are more comments of that conversation-id to get, I am bringing more comments!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(tokens_location, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more comments from this conversation id")
                continue_searching = False
                print("Total amount of collected comments = ", num_of_returned_comments)

            if num_of_returned_comments >=limit_amount_of_returned_comments:
                print("oooops, There may be more comments to return, but you asked to limit the amount of returned comments")
                print("infact you got", num_of_returned_comments, "returned comments and limited the function to get", limit_amount_of_returned_comments, "comments")
        
        return json_response_list, num_of_returned_comments, next_tokens, path_for_table

#max result in comment function: 10 - 500

    def return_comments_by_tweet_ids(self, conversation_ids = None, query = "",
                                start_time = "2015-12-7T00:00:00Z",
                                end_time = "today",
                                max_results = 10, evaluate_last_token = False,
                                limit_amount_of_returned_comments = 10000000,
                                verbose = False, dir_tree_name = "conversation_trees"):
                                
        
        tweet_ids = conversation_ids
        if max_results > 500:
            max_results = 500
            print('max_results can not be greater than 100, changed to 100')
        if max_results < 10:
            max_results = 10
            print('max_results can not be smaller than 10, changed to 10')
        if type(tweet_ids) != list:
            tweet_ids = [tweet_ids]

        #users_json_response_lists = []
        tweet_ids_evaluated = []
        tweet_ids_didnt_evaluated = []
        next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
        path_for_table_dict = {}
        for tweet_id in tweet_ids:
            print("Bringing comments of", tweet_id)
            try:
                json_response_list, num_of_returned_comments,next_tokens, path_for_table =\
                    self.__return_replies_of_conv_id_SMALL(conversation_id=tweet_id,
                                                        query = query,
                                                        start_time = start_time, end_time = end_time,
                                                        max_results = max_results, evaluate_last_token = evaluate_last_token,
                                                        limit_amount_of_returned_comments = limit_amount_of_returned_comments,
                                                        verbose = verbose, dir_tree_name = dir_tree_name)

                path_for_table_dict[tweet_id] = path_for_table

                print(num_of_returned_comments)
                if num_of_returned_comments > 0:
                    tweet_ids_evaluated.append(tweet_id)
                    next_tokens_users.append(next_tokens)


                else:
                    tweet_ids_didnt_evaluated.append(tweet_id)
                    print("The conversation id:", tweet_id, "had", num_of_returned_comments, "comments!!")

                print("---------------------------------------------------------------")
            except:
                print("There was a problem with the conversation id:", tweet_id)
                tweet_ids_didnt_evaluated.append(tweet_id)
                print("*************************************************************************************")

        return path_for_table_dict











    def get_conversation_tree(self, tweet_id : str ,max_results = 10,\
        tree_height = 1, evaluate_last_token = False,\
        limit_amount_of_returned_quotes = 10000000, verbose = False, dir_tree_name = "conversation_trees"):
        
        print(f'tree_height: {tree_height}\n tweet_id: {tweet_id}')

        if(tree_height < 1):
            return "Tree depth reached maximum"

        path_for_table_dict = self.return_quotes_by_tweet_ids(tweet_ids=tweet_id, max_results=max_results,\
            limit_amount_of_returned_quotes = limit_amount_of_returned_quotes, verbose = verbose, dir_tree_name =dir_tree_name)
        
        if not path_for_table_dict: return f"No retweets/quotes for tweet {tweet_id}"

        retweets_df = pd.read_csv( path_for_table_dict[tweet_id])

        retweets_ids = list(retweets_df.id_new)

        retweets_ids = [x.split(' ')[1] for x in retweets_ids]

        for retweet_id in retweets_ids:
            #os.make_dir('')
            self.get_conversation_tree(tweet_id=retweet_id, max_results=max_results, tree_height = tree_height - 1,\
            limit_amount_of_returned_quotes = limit_amount_of_returned_quotes, verbose = verbose, dir_tree_name = dir_tree_name + '/conv_tree_for_' + tweet_id)

            
            



